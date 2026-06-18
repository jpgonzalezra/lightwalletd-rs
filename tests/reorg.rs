//! A reorg replaces a block and drops its successors: stage three blocks, then reorg from the middle
//! height with a different block and confirm the tip rewinds and the orphaned block is gone.

mod common;

use common::{TestServer, testdata_blocks};
use lightwalletd_rs::proto::{BlockId, ChainSpec};

async fn block_hash(server: &mut TestServer, height: u64) -> Vec<u8> {
    server
        .compact
        .get_block(BlockId {
            height,
            hash: vec![],
        })
        .await
        .unwrap()
        .into_inner()
        .hash
}

#[tokio::test]
async fn reorg_rewrites_from_staged_height_and_drops_successors() {
    let mut server = TestServer::start().await;

    server.reset(380640, "2bb40e60", "main").await;
    server
        .stage_blocks(testdata_blocks().into_iter().take(3).collect())
        .await;
    server.apply_staged(380642).await;

    let tip = server
        .compact
        .get_latest_block(ChainSpec {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(tip.height, 380642);
    let hash_before = block_hash(&mut server, 380641).await;

    // Reorg: a different block at 380641 replaces it and orphans 380642.
    server.stage_blocks_create(380641, 99, 1).await;
    server.apply_staged(380641).await;

    let tip = server
        .compact
        .get_latest_block(ChainSpec {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(tip.height, 380641);

    let hash_after = block_hash(&mut server, 380641).await;
    assert_ne!(hash_after, hash_before, "the reorged block hash changed");

    // The orphaned block is no longer served.
    let orphaned = server
        .compact
        .get_block(BlockId {
            height: 380642,
            hash: vec![],
        })
        .await;
    assert!(orphaned.is_err(), "block 380642 should be gone after reorg");
}
