//! End-to-end happy path: stage the testdata blocks over the control plane, apply them, and read the
//! chain back through `CompactTxStreamer`.

mod common;

use common::{TestServer, testdata_blocks};
use lightwalletd_rs::proto::{BlockId, BlockRange, ChainSpec, Empty};

#[tokio::test]
async fn syncs_staged_blocks_over_grpc() {
    let mut server = TestServer::start().await;

    server.reset(380640, "2bb40e60", "main").await;
    server.stage_blocks(testdata_blocks()).await;
    server.apply_staged(380643).await;

    let info = server
        .compact
        .get_lightd_info(Empty {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(info.chain_name, "main");
    assert_eq!(info.block_height, 380643);
    assert_eq!(info.sapling_activation_height, 380640);
    assert_eq!(info.consensus_branch_id, "2bb40e60");

    let latest = server
        .compact
        .get_latest_block(ChainSpec {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(latest.height, 380643);

    let range = BlockRange {
        start: Some(BlockId {
            height: 380640,
            hash: vec![],
        }),
        end: Some(BlockId {
            height: 380643,
            hash: vec![],
        }),
        ..Default::default()
    };
    let mut stream = server
        .compact
        .get_block_range(range)
        .await
        .unwrap()
        .into_inner();

    let mut blocks = Vec::new();
    while let Some(block) = stream.message().await.unwrap() {
        blocks.push(block);
    }

    let heights: Vec<u64> = blocks.iter().map(|block| block.height).collect();
    assert_eq!(heights, vec![380640, 380641, 380642, 380643]);
    // Each applied block's prev hash chains onto its predecessor's hash.
    for pair in blocks.windows(2) {
        assert_eq!(pair[1].prev_hash, pair[0].hash);
    }
}
