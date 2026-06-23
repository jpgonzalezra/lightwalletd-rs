//! Background task that polls the node and fills the block cache.
//!
//! Each step fetches the next block above the cache tip, verifies it chains onto the cached tip
//! (`prevHash` matches), and either appends it or, on a mismatch, rolls back one block as a reorg.

use std::sync::Arc;
use std::time::Duration;

use crate::cache::{Cache, CacheError};
use crate::encoding;
use crate::fetch::{self, FetchError};
use crate::node::{NodeError, NodeRpc};

/// After this many consecutive corruption recoveries without a successful normal step, fall back to
/// the backoff sleep so recovery can never spin at full CPU even if the `fetch` height guard is
/// somehow defeated.
const MAX_CONSECUTIVE_RECOVERIES: u32 = 5;

/// Poll the node forever, appending new blocks to the cache and rolling back reorgs.
pub async fn run(node: Arc<dyn NodeRpc>, cache: Arc<Cache>, start_height: u64) {
    tracing::info!(start_height, "ingestor started");
    let mut consecutive_recoveries = 0u32;
    loop {
        match step(node.as_ref(), &cache, start_height).await {
            // Advanced one block (or handled a reorg): try the next one immediately.
            Ok(true) => consecutive_recoveries = 0,
            // Cache is at the node's tip: wait before polling again.
            Ok(false) => {
                consecutive_recoveries = 0;
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            // Cache corruption: truncate from the corrupt point and retry immediately, so re-ingestion
            // refills it rather than the loop stalling on the backoff sleep.
            Err(error) if should_recover(&error, consecutive_recoveries) => {
                consecutive_recoveries += 1;
                tracing::warn!(%error, consecutive_recoveries, "cache corruption during ingest; recovering");
                if let Err(recover_error) = recover(&cache) {
                    tracing::error!(%recover_error, "cache recovery failed; backing off");
                    tokio::time::sleep(Duration::from_secs(8)).await;
                }
            }
            // Node/transport errors — and corruption past the recovery bound — back off.
            Err(error) => {
                if consecutive_recoveries >= MAX_CONSECUTIVE_RECOVERIES {
                    tracing::error!(%error, "corruption recovery limit reached; backing off");
                } else {
                    tracing::warn!(%error, "ingestor step failed; retrying");
                }
                consecutive_recoveries = 0;
                tokio::time::sleep(Duration::from_secs(8)).await;
            }
        }
    }
}

/// Whether a failed step should truncate-and-recover (then retry immediately) rather than back off: a
/// cache corruption/decode error, but only while consecutive recoveries stay under
/// [`MAX_CONSECUTIVE_RECOVERIES`] — the backstop against a sleepless truncate→re-add→detect loop.
fn should_recover(error: &StepError, consecutive_recoveries: u32) -> bool {
    error.is_corruption() && consecutive_recoveries < MAX_CONSECUTIVE_RECOVERIES
}

/// Locate the lowest corrupt height and truncate from it; re-ingestion refills what was dropped.
fn recover(cache: &Cache) -> Result<(), CacheError> {
    if let Some(corrupt) = cache.lowest_corrupt_height()? {
        tracing::warn!(corrupt, "truncating cache from corrupt height");
        cache.reorg(corrupt.saturating_sub(1))?;
    }
    Ok(())
}

/// Try to ingest one block. Returns `Ok(true)` if a block was added or a reorg was rolled back,
/// `Ok(false)` if the cache is already at the node's tip.
///
/// The tip height **and** hash come from a single `getblockchaininfo`, so a reorg that replaces the
/// tip block without advancing the height is caught by comparing the hash, not just the height.
async fn step(node: &dyn NodeRpc, cache: &Cache, start_height: u64) -> Result<bool, StepError> {
    let info = node.get_blockchain_info().await?;
    let tip_height = info.blocks;

    let next = match cache.latest_height()? {
        None => start_height,
        Some(latest_height) if latest_height == tip_height => {
            // Same height: a tip hash mismatch means a reorg replaced the tip block in place.
            let tip_hash = encoding::display_hex_to_wire(&info.bestblockhash)?;
            if cache
                .latest_hash()?
                .is_some_and(|latest| latest == tip_hash)
            {
                return Ok(false);
            }
            tracing::warn!(
                height = latest_height,
                "tip reorg detected; rolling back one block"
            );
            cache.reorg(latest_height.saturating_sub(1))?;
            return Ok(true);
        }
        Some(latest_height) if latest_height > tip_height => {
            // Node is behind our cache (deep reorg or node rollback): drop our tip block.
            tracing::warn!(
                latest_height,
                tip_height,
                "node behind cache; rolling back one block"
            );
            cache.reorg(latest_height.saturating_sub(1))?;
            return Ok(true);
        }
        Some(latest_height) => latest_height + 1,
    };
    if next > tip_height {
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
            tracing::info!(height = next, tip = tip_height, "ingesting");
        } else {
            tracing::debug!(height = next, "ingested");
        }
    } else {
        tracing::warn!(
            height = next.saturating_sub(1),
            "reorg detected; rolling back one block"
        );
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
    #[error("decoding tip hash: {0}")]
    Encoding(#[from] hex::FromHexError),
}

impl StepError {
    /// Whether this error is a cache corruption recoverable by truncation, as opposed to a
    /// node/transport failure (which only warrants a backoff retry).
    fn is_corruption(&self) -> bool {
        matches!(
            self,
            StepError::Cache(CacheError::Corruption { .. } | CacheError::Decode(_))
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    use crate::node::GetBlockchainInfo;
    use crate::proto::CompactBlock;
    use crate::testutil::{FakeNode, temp_cache};

    fn tip_block(height: u64, hash: Vec<u8>) -> CompactBlock {
        CompactBlock {
            height,
            hash,
            ..Default::default()
        }
    }

    /// The raw block and its parsed form for fixture `index` (heights 289460..=289465). The parsed
    /// height is used so the cache guards (Phase 3) and the fetch height check (Phase 4) hold.
    fn fixture(index: usize) -> (Vec<u8>, CompactBlock) {
        let json = std::fs::read_to_string("testdata/compact_blocks.json").unwrap();
        let fixtures: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap();
        let raw = hex::decode(fixtures[index]["full"].as_str().unwrap()).unwrap();
        let parsed = crate::compact::to_compact_block(&raw).unwrap();
        (raw, parsed)
    }

    fn blockchain_info(blocks: u64, bestblockhash: &str) -> GetBlockchainInfo {
        serde_json::from_value(json!({
            "chain": "main",
            "blocks": blocks,
            "bestblockhash": bestblockhash,
            "consensus": { "chaintip": "00000000" },
        }))
        .unwrap()
    }

    fn fake_serving(raw: Vec<u8>, tip: u64) -> FakeNode {
        FakeNode {
            blockchain_info: Some(blockchain_info(tip, "00")),
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
        let (raw, parsed) = fixture(0);
        let height = parsed.height;
        let (_dir, cache) = temp_cache();
        cache
            .add(height - 1, &tip_block(height - 1, parsed.prev_hash.clone()))
            .unwrap();

        let advanced = step(&fake_serving(raw, height), &cache, height - 1)
            .await
            .unwrap();

        assert!(advanced);
        assert_eq!(cache.latest_height().unwrap(), Some(height));
    }

    #[tokio::test]
    async fn step_is_idle_when_the_tip_hash_matches_at_the_same_height() {
        let (_dir, cache) = temp_cache();
        cache.add(100, &tip_block(100, vec![0u8; 32])).unwrap();

        // The cached tip hash is 32 zero bytes; its display-order hex is 64 zeros.
        let fake = FakeNode {
            blockchain_info: Some(blockchain_info(100, &"00".repeat(32))),
            ..Default::default()
        };
        let advanced = step(&fake, &cache, 100).await.unwrap();

        assert!(!advanced);
    }

    #[tokio::test]
    async fn step_rolls_back_one_block_when_the_tip_hash_differs_at_the_same_height() {
        let (_dir, cache) = temp_cache();
        cache.add(100, &tip_block(100, vec![0xaa; 32])).unwrap();
        cache.add(101, &tip_block(101, vec![0xbb; 32])).unwrap();

        // Same height 101, but the node reports a different tip hash → an in-place tip reorg.
        let fake = FakeNode {
            blockchain_info: Some(blockchain_info(101, &"cc".repeat(32))),
            ..Default::default()
        };
        let advanced = step(&fake, &cache, 100).await.unwrap();

        assert!(advanced);
        assert_eq!(cache.latest_height().unwrap(), Some(100));
    }

    #[tokio::test]
    async fn step_rolls_back_one_block_when_prev_hash_does_not_chain() {
        let (raw, parsed) = fixture(0);
        let height = parsed.height;
        let (_dir, cache) = temp_cache();
        cache
            .add(height - 1, &tip_block(height - 1, vec![0xff; 32]))
            .unwrap();

        let advanced = step(&fake_serving(raw, height), &cache, height - 1)
            .await
            .unwrap();

        assert!(advanced);
        assert_eq!(cache.latest_height().unwrap(), None);
    }

    #[tokio::test]
    async fn step_surfaces_corruption_when_the_cached_tip_is_undecodable() {
        let (_dir, cache) = temp_cache();
        cache.insert_raw(100, &[0x08, 0xff]).unwrap();

        // Same height as the tip → `step` reads the (undecodable) tip hash and surfaces the corruption.
        let fake = FakeNode {
            blockchain_info: Some(blockchain_info(100, &"00".repeat(32))),
            ..Default::default()
        };
        let error = step(&fake, &cache, 100).await.unwrap_err();

        assert!(error.is_corruption());
    }

    #[tokio::test]
    async fn step_wrong_height_block_is_not_classified_as_corruption() {
        let (raw, parsed) = fixture(0);
        let (_dir, cache) = temp_cache();

        // Empty cache, node well ahead: `step` fetches `start_height`, but the node serves a block at
        // a different height → a `FetchError`, kept on the node backoff (never a cache corruption).
        let fake = fake_serving(raw, parsed.height + 10);
        let error = step(&fake, &cache, parsed.height - 5).await.unwrap_err();

        assert!(!error.is_corruption());
        assert!(matches!(
            error,
            StepError::Fetch(FetchError::UnexpectedHeight { .. })
        ));
    }

    #[test]
    fn recover_truncates_a_corrupt_suffix_to_the_last_good_block() {
        let (_dir, cache) = temp_cache();
        for height in 100..=102 {
            cache
                .add(height, &tip_block(height, vec![height as u8; 32]))
                .unwrap();
        }
        cache.insert_raw(103, &[0x08, 0xff]).unwrap();

        recover(&cache).unwrap();

        assert_eq!(cache.latest_height().unwrap(), Some(102));
        assert!(cache.validate_light().is_ok());
    }

    #[test]
    fn should_recover_only_for_corruption_within_the_recovery_bound() {
        let corruption = StepError::Cache(CacheError::Corruption {
            height: 1,
            detail: String::new(),
        });
        assert!(should_recover(&corruption, 0));
        assert!(should_recover(&corruption, MAX_CONSECUTIVE_RECOVERIES - 1));
        // At the cap, fall back to backoff instead of recovering again (no sleepless loop).
        assert!(!should_recover(&corruption, MAX_CONSECUTIVE_RECOVERIES));

        let transport = StepError::Fetch(FetchError::UnexpectedHeight {
            requested: 5,
            got: 9,
        });
        assert!(!should_recover(&transport, 0));
    }
}
