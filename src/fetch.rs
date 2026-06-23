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
    /// The node returned a block at a different height than requested.
    #[error("expected block at height {requested}, node returned height {got}")]
    UnexpectedHeight {
        /// Height we asked the node for.
        requested: u64,
        /// Height of the block the node actually returned.
        got: u64,
    },
}

/// Fetch the block at `height` and build its `CompactBlock`, including the note-commitment tree sizes.
///
/// The hash and tree sizes come from a verbose `getblock`; the bytes come from a raw `getblock` keyed by
/// that hash, so both refer to the same block even across a reorg.
pub async fn compact_block(node: &dyn NodeRpc, height: u64) -> Result<CompactBlock, FetchError> {
    let verbose = node.get_block_verbose(height).await?;
    let raw = node.get_block_raw(&verbose.hash).await?;
    let mut block = compact::to_compact_block(&raw)?;
    // A wrong-height block stays on the node/transport backoff instead of being mislabeled as cache
    // corruption and fed into the recovery path.
    if block.height != height {
        return Err(FetchError::UnexpectedHeight {
            requested: height,
            got: block.height,
        });
    }
    if let Some(meta) = block.chain_metadata.as_mut() {
        meta.sapling_commitment_tree_size = verbose.trees.sapling.size;
        meta.orchard_commitment_tree_size = verbose.trees.orchard.size;
    }
    Ok(block)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    use crate::testutil::FakeNode;

    fn fixture_raw() -> Vec<u8> {
        let json_text = std::fs::read_to_string("testdata/compact_blocks.json").unwrap();
        let fixtures: Vec<serde_json::Value> = serde_json::from_str(&json_text).unwrap();
        hex::decode(fixtures[0]["full"].as_str().unwrap()).unwrap()
    }

    #[tokio::test]
    async fn compact_block_fills_tree_sizes_from_verbose() {
        let raw = fixture_raw();
        let height = compact::to_compact_block(&raw).unwrap().height;

        let fake = FakeNode {
            block_verbose: Some(
                serde_json::from_value(json!({
                    "hash": "00",
                    "trees": { "sapling": { "size": 11 }, "orchard": { "size": 22 } },
                }))
                .unwrap(),
            ),
            block_raw: Some(raw),
            ..Default::default()
        };

        let block = compact_block(&fake, height).await.unwrap();
        let meta = block.chain_metadata.unwrap();

        assert_eq!(meta.sapling_commitment_tree_size, 11);
        assert_eq!(meta.orchard_commitment_tree_size, 22);
    }

    #[tokio::test]
    async fn compact_block_rejects_a_block_at_the_wrong_height() {
        let raw = fixture_raw();
        let actual_height = compact::to_compact_block(&raw).unwrap().height;

        let fake = FakeNode {
            block_verbose: Some(
                serde_json::from_value(json!({
                    "hash": "00",
                    "trees": { "sapling": { "size": 0 }, "orchard": { "size": 0 } },
                }))
                .unwrap(),
            ),
            block_raw: Some(raw),
            ..Default::default()
        };

        let error = compact_block(&fake, actual_height + 1).await.unwrap_err();

        assert!(matches!(
            error,
            FetchError::UnexpectedHeight { requested, got }
                if requested == actual_height + 1 && got == actual_height
        ));
    }
}
