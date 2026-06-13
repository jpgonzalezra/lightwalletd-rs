//! Fetch a single block from the node and assemble its [`CompactBlock`].
//!
//! Shared by `GetBlock` (on a cache miss) and the ingestor.

use crate::compact::{self, ParseError};
use crate::node::{NodeError, NodeRpc};
use crate::proto::CompactBlock;

/// Errors from fetching and parsing a block.
#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    /// The node RPC failed.
    #[error(transparent)]
    Node(#[from] NodeError),
    /// The block could not be parsed.
    #[error(transparent)]
    Parse(#[from] ParseError),
}

/// Fetch the block at `height` and build its `CompactBlock`, including the note-commitment tree sizes.
///
/// The hash and tree sizes come from a verbose `getblock`; the bytes come from a raw `getblock` keyed by
/// that hash, so both refer to the same block even across a reorg.
pub async fn compact_block(node: &dyn NodeRpc, height: u64) -> Result<CompactBlock, FetchError> {
    let verbose = node.get_block_verbose(height).await?;
    let raw = node.get_block_raw(&verbose.hash).await?;
    let mut block = compact::to_compact_block(&raw)?;
    if let Some(meta) = block.chain_metadata.as_mut() {
        meta.sapling_commitment_tree_size = verbose.trees.sapling.size;
        meta.orchard_commitment_tree_size = verbose.trees.orchard.size;
    }
    Ok(block)
}
