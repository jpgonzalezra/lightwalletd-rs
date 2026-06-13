//! Background task that polls the node and fills the block cache.
//!
//! Each step fetches the next block above the cache tip, verifies it chains onto the cached tip
//! (`prevHash` matches), and either appends it or, on a mismatch, rolls back one block as a reorg.

use std::sync::Arc;
use std::time::Duration;

use crate::cache::{Cache, CacheError};
use crate::fetch::{self, FetchError};
use crate::node::{NodeClient, NodeError};

/// Poll the node forever, appending new blocks to the cache and rolling back reorgs.
pub async fn run(node: NodeClient, cache: Arc<Cache>, start_height: u64) {
    tracing::info!(start_height, "ingestor started");
    loop {
        match step(&node, &cache, start_height).await {
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
async fn step(node: &NodeClient, cache: &Cache, start_height: u64) -> Result<bool, StepError> {
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
