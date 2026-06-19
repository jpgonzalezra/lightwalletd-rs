//! Background task that polls the node and fills the block cache.
//!
//! Each step fetches the next block above the cache tip, verifies it chains onto the cached tip
//! (`prevHash` matches), and either appends it or, on a mismatch, rolls back one block as a reorg.

use std::sync::Arc;
use std::time::Duration;

use crate::cache::{Cache, CacheError};
use crate::fetch::{self, FetchError};
use crate::node::{NodeError, NodeRpc};

/// Poll the node forever, appending new blocks to the cache and rolling back reorgs.
pub async fn run(node: Arc<dyn NodeRpc>, cache: Arc<Cache>, start_height: u64) {
    tracing::info!(start_height, "ingestor started");
    loop {
        match step(node.as_ref(), &cache, start_height).await {
            // Advanced one block (or handled a reorg): try the next one immediately.
            Ok(true) => {}
            // Cache is at the node's tip: wait before polling again.
            Ok(false) => tokio::time::sleep(Duration::from_secs(2)).await,
            Err(error) => {
                tracing::warn!(%error, "ingestor step failed; retrying");
                tokio::time::sleep(Duration::from_secs(8)).await;
            }
        }
    }
}

/// Try to ingest one block. Returns `Ok(true)` if a block was added or a reorg was rolled back,
/// `Ok(false)` if the cache is already at the node's tip.
async fn step(node: &dyn NodeRpc, cache: &Cache, start_height: u64) -> Result<bool, StepError> {
    let tip = node.get_block_count().await?;
    let next = match cache.latest_height()? {
        Some(height) => height + 1,
        None => start_height,
    };
    if next > tip {
        return Ok(false);
    }

    let block = fetch::compact_block(node, next).await?;
    let chains = match cache.latest_hash()? {
        None => true,
        Some(latest) => latest == block.prev_hash,
    };
    if chains {
        cache.add(next, &block)?;
        if next % 100 == 0 {
            tracing::info!(height = next, tip, "ingesting");
        } else {
            tracing::debug!(height = next, "ingested");
        }
    } else {
        tracing::warn!(height = next - 1, "reorg detected; rolling back one block");
        cache.reorg(next.saturating_sub(2))?;
    }
    Ok(true)
}

/// Errors from a single ingestor step.
#[derive(Debug, thiserror::Error)]
enum StepError {
    #[error(transparent)]
    Node(#[from] NodeError),
    #[error(transparent)]
    Fetch(#[from] FetchError),
    #[error(transparent)]
    Cache(#[from] CacheError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    use crate::proto::CompactBlock;
    use crate::testutil::{FakeNode, temp_cache};

    fn tip_block(height: u64, hash: Vec<u8>) -> CompactBlock {
        CompactBlock {
            height,
            hash,
            ..Default::default()
        }
    }

    fn fixture_raw() -> Vec<u8> {
        let json = std::fs::read_to_string("testdata/compact_blocks.json").unwrap();
        let fixtures: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap();
        hex::decode(fixtures[0]["full"].as_str().unwrap()).unwrap()
    }

    fn fake_serving(raw: Vec<u8>, tip: u64) -> FakeNode {
        FakeNode {
            block_count: Some(tip),
            block_verbose: Some(
                serde_json::from_value(json!({
                    "hash": "00",
                    "trees": { "sapling": { "size": 0 }, "orchard": { "size": 0 } },
                }))
                .unwrap(),
            ),
            block_raw: Some(raw),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn step_appends_block_that_chains_onto_the_cached_tip() {
        let raw = fixture_raw();
        let parsed = crate::compact::to_compact_block(&raw).unwrap();
        let (_dir, cache) = temp_cache();
        cache
            .add(100, &tip_block(100, parsed.prev_hash.clone()))
            .unwrap();

        let advanced = step(&fake_serving(raw, 101), &cache, 100).await.unwrap();

        assert!(advanced);
        assert_eq!(cache.latest_height().unwrap(), Some(101));
    }

    #[tokio::test]
    async fn step_is_idle_when_cache_is_at_the_tip() {
        let (_dir, cache) = temp_cache();
        cache.add(100, &tip_block(100, vec![0u8; 32])).unwrap();

        let fake = FakeNode {
            block_count: Some(100),
            ..Default::default()
        };
        let advanced = step(&fake, &cache, 100).await.unwrap();

        assert!(!advanced);
    }

    #[tokio::test]
    async fn step_rolls_back_one_block_when_prev_hash_does_not_chain() {
        let (_dir, cache) = temp_cache();
        cache.add(100, &tip_block(100, vec![0xff; 32])).unwrap();

        let advanced = step(&fake_serving(fixture_raw(), 101), &cache, 100)
            .await
            .unwrap();

        assert!(advanced);
        assert_eq!(cache.latest_height().unwrap(), None);
    }
}
