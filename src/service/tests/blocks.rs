//! Unit tests for the block-serving methods (`GetBlock`, `GetBlockRange`).

use std::sync::Arc;

use tokio_stream::StreamExt;
use tonic::{Code, Request};

use crate::proto::compact_tx_streamer_server::CompactTxStreamer;
use crate::proto::{
    BlockId, BlockRange, CompactBlock, CompactOrchardAction, CompactSaplingSpend, CompactTx,
    CompactTxIn, PoolType,
};
use crate::repair::RepairSignal;
use crate::testutil::{FakeNode, fake_node_serving, temp_cache, testdata_blocks};

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

/// A streamer whose cache holds `blocks` and whose node serves `raws`, so a range can be made to
/// straddle the cache/node boundary the way a real one does at the cached tip.
fn streamer_with_cache_and_node(
    blocks: &[CompactBlock],
    raws: &[Vec<u8>],
) -> (tempfile::TempDir, Streamer, RepairSignal) {
    let (dir, cache) = temp_cache();
    for block in blocks {
        cache.add(block.height, block).unwrap();
    }
    let repair = RepairSignal::new();
    let streamer = Streamer::new(
        Arc::new(fake_node_serving(raws)),
        Arc::new(cache),
        "main".to_string(),
        None,
    )
    .with_repair_signal(repair.clone());
    (dir, streamer, repair)
}

/// A block at `height` hashing to a value derived from its height, chaining onto `prev_hash`.
fn chained_block(height: u64, prev_hash: Vec<u8>) -> CompactBlock {
    CompactBlock {
        height,
        hash: vec![height as u8; 32],
        prev_hash,
        ..Default::default()
    }
}

/// A contiguous chain of synthetic blocks covering `heights`.
fn chain(heights: std::ops::RangeInclusive<u64>) -> Vec<CompactBlock> {
    heights
        .map(|height| chained_block(height, vec![(height - 1) as u8; 32]))
        .collect()
}

/// Collect a block range, stopping at the first error, as `(blocks served, error)`.
async fn collect_range(
    streamer: &Streamer,
    start: u64,
    end: u64,
) -> (Vec<CompactBlock>, Option<tonic::Status>) {
    let mut stream = streamer
        .get_block_range(Request::new(nullifiers_range(start, end, vec![])))
        .await
        .unwrap()
        .into_inner();
    let mut blocks = Vec::new();
    while let Some(item) = stream.next().await {
        match item {
            Ok(block) => blocks.push(block),
            Err(status) => return (blocks, Some(status)),
        }
    }
    (blocks, None)
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

#[tokio::test]
async fn get_block_range_aborts_on_a_forward_discontinuity() {
    // Height 3 no longer chains onto height 2: what the cache holds mid reorg repair.
    let mut blocks = chain(1..=3);
    blocks[2].prev_hash = vec![0xff; 32];
    let (_dir, streamer, _repair) = streamer_with_cache_and_node(&blocks, &[]);

    let (served, status) = collect_range(&streamer, 1, 3).await;

    assert_eq!(
        (served.len(), status.map(|status| status.code())),
        (2, Some(Code::Aborted))
    );
}

#[tokio::test]
async fn get_block_range_aborts_on_a_reverse_discontinuity() {
    let mut blocks = chain(1..=3);
    blocks[2].prev_hash = vec![0xff; 32];
    let (_dir, streamer, _repair) = streamer_with_cache_and_node(&blocks, &[]);

    let (served, status) = collect_range(&streamer, 3, 1).await;

    assert_eq!(
        (served.len(), status.map(|status| status.code())),
        (1, Some(Code::Aborted))
    );
}

#[tokio::test]
async fn get_block_range_serves_a_contiguous_reverse_range() {
    let (_dir, streamer, _repair) = streamer_with_cache_and_node(&chain(1..=3), &[]);

    let (served, status) = collect_range(&streamer, 3, 1).await;

    assert_eq!(
        (
            served.iter().map(|block| block.height).collect::<Vec<_>>(),
            status.is_none()
        ),
        (vec![3, 2, 1], true)
    );
}

#[tokio::test]
async fn a_discontinuity_reports_the_lower_height_of_the_seam_for_repair() {
    // Truncating from the lower height drops both sides of the seam, whichever one is stale.
    let mut blocks = chain(1..=3);
    blocks[2].prev_hash = vec![0xff; 32];
    let (_dir, streamer, repair) = streamer_with_cache_and_node(&blocks, &[]);

    collect_range(&streamer, 1, 3).await;

    assert_eq!(repair.take(), Some(2));
}

#[tokio::test]
async fn a_contiguous_range_reports_nothing_for_repair() {
    let (_dir, streamer, repair) = streamer_with_cache_and_node(&chain(1..=3), &[]);

    collect_range(&streamer, 1, 3).await;

    assert_eq!(repair.take(), None);
}

/// The four consecutive raw blocks of `testdata/blocks`, with their parsed compact forms.
fn testdata_chain() -> (Vec<Vec<u8>>, Vec<CompactBlock>) {
    let raws = testdata_blocks();
    let parsed = raws
        .iter()
        .map(|raw| crate::compact::to_compact_block(raw).unwrap())
        .collect();
    (raws, parsed)
}

#[tokio::test]
async fn get_block_range_serves_a_contiguous_range_across_the_cache_and_the_node() {
    // The real boundary: the cache ends partway through the range and the node serves the rest.
    let (raws, parsed) = testdata_chain();
    let (_dir, streamer, _repair) = streamer_with_cache_and_node(&parsed[..2], &raws[2..]);

    let (served, status) = collect_range(&streamer, parsed[0].height, parsed[3].height).await;

    assert_eq!(
        (
            served.iter().map(|block| block.height).collect::<Vec<_>>(),
            status.is_none()
        ),
        (
            parsed.iter().map(|block| block.height).collect::<Vec<_>>(),
            true
        )
    );
}

#[tokio::test]
async fn get_block_range_aborts_when_the_cached_block_is_from_an_abandoned_fork() {
    // The cache holds a block of the fork the node has already abandoned, so the node's next block
    // does not chain onto it. Serving both would describe a chain that never existed.
    let (raws, parsed) = testdata_chain();
    let mut stale = parsed[1].clone();
    stale.hash = vec![0xff; 32];
    let (_dir, streamer, repair) = streamer_with_cache_and_node(&[stale.clone()], &raws[2..3]);

    let (served, status) = collect_range(&streamer, stale.height, stale.height + 1).await;

    assert_eq!(
        (
            served.len(),
            status.map(|status| status.code()),
            repair.take()
        ),
        (1, Some(Code::Aborted), Some(stale.height))
    );
}

#[tokio::test]
async fn get_block_range_aborts_on_a_discontinuity_between_two_node_served_blocks() {
    // Above the cached tip every height is fetched from the node one at a time, so a reorg between
    // two of those fetches breaks the chain with the cache nowhere near the seam.
    let (mut raws, parsed) = testdata_chain();
    raws[3][4..36].copy_from_slice(&[0xff; 32]);
    let (_dir, streamer, _repair) = streamer_with_cache_and_node(&parsed[..2], &raws[2..]);

    let (served, status) = collect_range(&streamer, parsed[0].height, parsed[3].height).await;

    assert_eq!(
        (served.len(), status.map(|status| status.code())),
        (3, Some(Code::Aborted))
    );
}

#[tokio::test]
async fn a_discontinuity_between_two_node_served_blocks_reports_nothing_for_repair() {
    // The cache holds no block at either side of the seam, so truncating it would repair nothing and
    // (the seam being above the cached tip) would empty it down to the floor.
    let (mut raws, parsed) = testdata_chain();
    raws[3][4..36].copy_from_slice(&[0xff; 32]);
    let (_dir, streamer, repair) = streamer_with_cache_and_node(&parsed[..2], &raws[2..]);

    collect_range(&streamer, parsed[0].height, parsed[3].height).await;

    assert_eq!(repair.take(), None);
}

#[tokio::test]
async fn get_block_range_serves_a_contiguous_range_spanning_several_cache_chunks() {
    let (_dir, streamer, _repair) = streamer_with_cache_and_node(&chain(1..=70), &[]);

    let (served, status) = collect_range(&streamer, 1, 70).await;

    assert_eq!((served.len(), status.is_none()), (70, true));
}

#[tokio::test]
async fn get_block_range_aborts_on_a_discontinuity_across_a_cache_chunk_boundary() {
    // Heights 1..=64 and 65..=70 are read under separate transactions; the link handed between them
    // is what catches a seam the chunking would otherwise hide.
    let mut blocks = chain(1..=70);
    blocks[64].prev_hash = vec![0xff; 32];
    let (_dir, streamer, repair) = streamer_with_cache_and_node(&blocks, &[]);

    let (served, status) = collect_range(&streamer, 1, 70).await;

    assert_eq!(
        (
            served.len(),
            status.map(|status| status.code()),
            repair.take()
        ),
        (64, Some(Code::Aborted), Some(64))
    );
}

#[tokio::test]
async fn get_block_range_nullifiers_aborts_on_a_discontinuity_too() {
    let mut blocks = chain(1..=3);
    blocks[2].prev_hash = vec![0xff; 32];
    let (_dir, streamer, _repair) = streamer_with_cache_and_node(&blocks, &[]);

    let mut stream = streamer
        .get_block_range_nullifiers(Request::new(nullifiers_range(1, 3, vec![])))
        .await
        .unwrap()
        .into_inner();
    let mut codes = Vec::new();
    while let Some(item) = stream.next().await {
        if let Err(status) = item {
            codes.push(status.code());
        }
    }

    assert_eq!(codes, vec![Code::Aborted]);
}
