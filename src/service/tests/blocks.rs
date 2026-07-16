//! Unit tests for the block-serving methods (`GetBlock`, `GetBlockRange`).

use std::sync::Arc;

use tokio_stream::StreamExt;
use tonic::{Code, Request};

use crate::proto::compact_tx_streamer_server::CompactTxStreamer;
use crate::proto::{
    BlockId, BlockRange, CompactBlock, CompactOrchardAction, CompactSaplingSpend, CompactTx,
    CompactTxIn, PoolType,
};
use crate::testutil::{FakeNode, temp_cache};

use super::super::Streamer;
use super::streamer_with;

/// A streamer whose cache is pre-populated with the given blocks, so `GetBlockRange(Nullifiers)`
/// serves them without needing a real raw block from the node.
fn streamer_with_cached_blocks(blocks: &[CompactBlock]) -> (tempfile::TempDir, Streamer) {
    let (dir, cache) = temp_cache();
    for block in blocks {
        cache.add(block.height, block).unwrap();
    }
    let streamer = Streamer::new(
        Arc::new(FakeNode::default()),
        Arc::new(cache),
        "main".to_string(),
        None,
    );
    (dir, streamer)
}

/// A block at `height` with two transactions: one transparent-only, one carrying a Sapling spend
/// nullifier. Used to exercise `GetBlockRangeNullifiers`'s pool filtering end to end.
fn block_with_transparent_and_sapling_txs(height: u64) -> CompactBlock {
    let transparent_tx = CompactTx {
        index: 0,
        vin: vec![CompactTxIn::default()],
        ..Default::default()
    };
    let sapling_tx = CompactTx {
        index: 1,
        spends: vec![CompactSaplingSpend { nf: vec![7; 32] }],
        ..Default::default()
    };
    CompactBlock {
        height,
        vtx: vec![transparent_tx, sapling_tx],
        ..Default::default()
    }
}

#[tokio::test]
async fn get_block_past_the_tip_maps_to_out_of_range() {
    let fake = Arc::new(FakeNode {
        block_verbose_err: Some((-8, "block height not in best chain".to_string())),
        ..Default::default()
    });
    let (_dir, streamer) = streamer_with(fake);

    let status = streamer
        .get_block(Request::new(BlockId {
            height: 99_999_999,
            hash: vec![],
        }))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::OutOfRange);
}

#[tokio::test]
async fn get_block_with_unclassified_node_error_maps_to_unavailable() {
    let fake = Arc::new(FakeNode {
        block_verbose_err: Some((-99, "something unexpected".to_string())),
        ..Default::default()
    });
    let (_dir, streamer) = streamer_with(fake);

    let status = streamer
        .get_block(Request::new(BlockId {
            height: 1,
            hash: vec![],
        }))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::Unavailable);
}

#[tokio::test]
async fn get_block_unspecified_identifier_is_invalid_argument() {
    let (_dir, streamer) = streamer_with(Arc::new(FakeNode::default()));

    let status = streamer
        .get_block(Request::new(BlockId {
            height: 0,
            hash: vec![],
        }))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn get_block_range_without_start_is_invalid_argument() {
    let (_dir, streamer) = streamer_with(Arc::new(FakeNode::default()));

    let status = streamer
        .get_block_range(Request::new(BlockRange {
            start: None,
            end: Some(BlockId {
                height: 2,
                hash: vec![],
            }),
            ..Default::default()
        }))
        .await
        .err()
        .unwrap();

    assert_eq!(status.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn get_block_range_without_end_is_invalid_argument() {
    let (_dir, streamer) = streamer_with(Arc::new(FakeNode::default()));

    let status = streamer
        .get_block_range(Request::new(BlockRange {
            start: Some(BlockId {
                height: 1,
                hash: vec![],
            }),
            end: None,
            ..Default::default()
        }))
        .await
        .err()
        .unwrap();

    assert_eq!(status.code(), Code::InvalidArgument);
}

fn nullifiers_range(start: u64, end: u64, pool_types: Vec<i32>) -> BlockRange {
    BlockRange {
        start: Some(BlockId {
            height: start,
            hash: vec![],
        }),
        end: Some(BlockId {
            height: end,
            hash: vec![],
        }),
        pool_types,
    }
}

#[tokio::test]
async fn get_block_range_nullifiers_honors_requested_pool_types() {
    // Sapling-only: the transparent-only tx is dropped by the pool filter; the Sapling tx survives
    // with its spend nullifier intact.
    let (_dir, streamer) =
        streamer_with_cached_blocks(&[block_with_transparent_and_sapling_txs(1)]);

    let stream = streamer
        .get_block_range_nullifiers(Request::new(nullifiers_range(
            1,
            1,
            vec![PoolType::Sapling as i32],
        )))
        .await
        .unwrap()
        .into_inner();
    let blocks: Vec<CompactBlock> = stream.map(|b| b.unwrap()).collect().await;

    assert_eq!(blocks.len(), 1);
    assert_eq!(blocks[0].vtx.len(), 1);
    assert_eq!(blocks[0].vtx[0].spends[0].nf, vec![7; 32]);
}

#[tokio::test]
async fn get_block_range_nullifiers_excludes_pools_not_requested() {
    // Orchard-only: neither the transparent-only tx nor the Sapling tx has an Orchard component, so
    // both are dropped and the block comes back with an empty `vtx`.
    let (_dir, streamer) =
        streamer_with_cached_blocks(&[block_with_transparent_and_sapling_txs(1)]);

    let stream = streamer
        .get_block_range_nullifiers(Request::new(nullifiers_range(
            1,
            1,
            vec![PoolType::Orchard as i32],
        )))
        .await
        .unwrap()
        .into_inner();
    let blocks: Vec<CompactBlock> = stream.map(|b| b.unwrap()).collect().await;

    assert_eq!(blocks.len(), 1);
    assert!(blocks[0].vtx.is_empty());
}

#[tokio::test]
async fn get_block_range_nullifiers_always_drops_transparent_even_when_requested() {
    // Requesting transparent explicitly does not bring transparent data back: `GetBlockRangeNullifiers`
    // never returns it (use `GetBlockRange` for that), matching Go's forced removal.
    let (_dir, streamer) =
        streamer_with_cached_blocks(&[block_with_transparent_and_sapling_txs(1)]);

    let stream = streamer
        .get_block_range_nullifiers(Request::new(nullifiers_range(
            1,
            1,
            vec![PoolType::Transparent as i32, PoolType::Sapling as i32],
        )))
        .await
        .unwrap()
        .into_inner();
    let blocks: Vec<CompactBlock> = stream.map(|b| b.unwrap()).collect().await;

    assert_eq!(blocks.len(), 1);
    assert_eq!(blocks[0].vtx.len(), 1);
    assert!(blocks[0].vtx[0].vin.is_empty() && blocks[0].vtx[0].vout.is_empty());
}

#[tokio::test]
async fn get_block_range_nullifiers_default_pool_types_keeps_shielded_nullifiers() {
    // Empty `pool_types` is the legacy default: shielded only, same as `GetBlockRange`.
    let mut block = block_with_transparent_and_sapling_txs(1);
    block.vtx.push(CompactTx {
        index: 2,
        actions: vec![CompactOrchardAction {
            nullifier: vec![9; 32],
            cmx: vec![1; 32],
            ephemeral_key: vec![2; 32],
            ciphertext: vec![3; 52],
        }],
        ..Default::default()
    });
    let (_dir, streamer) = streamer_with_cached_blocks(&[block]);

    let stream = streamer
        .get_block_range_nullifiers(Request::new(nullifiers_range(1, 1, vec![])))
        .await
        .unwrap()
        .into_inner();
    let blocks: Vec<CompactBlock> = stream.map(|b| b.unwrap()).collect().await;

    assert_eq!(blocks.len(), 1);
    // The transparent-only tx is dropped; the Sapling and Orchard txs survive, the Orchard action
    // reduced to its nullifier.
    assert_eq!(blocks[0].vtx.len(), 2);
    assert_eq!(blocks[0].vtx[0].spends[0].nf, vec![7; 32]);
    assert_eq!(blocks[0].vtx[1].actions[0].nullifier, vec![9; 32]);
    assert!(blocks[0].vtx[1].actions[0].cmx.is_empty());
}
