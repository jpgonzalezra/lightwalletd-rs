//! The `basic-reorg` vector, vendored and run without network: seed the real mainnet block 663150,
//! pad 100 synthetic blocks, mine the real `recv` transaction into 663190, apply to 663250, and read
//! the chain back. A failure here is a real finding (a parser/darkside bug on mainnet data) to triage.

mod common;

use common::{RECV_TXID_DISPLAY, TestServer, basic_reorg_block, recv_tx, wire_txid};
use lightwalletd_rs::proto::{BlockId, ChainSpec, TxFilter};

#[tokio::test]
async fn basic_reorg_vector_syncs_real_mainnet_data() {
    let mut server = TestServer::start().await;

    server.reset(663150, "bad", "x").await;
    server.stage_blocks(vec![basic_reorg_block()]).await;
    server.stage_blocks_create(663151, 0, 100).await;
    server.stage_transactions(663190, vec![recv_tx()]).await;
    server.apply_staged(663250).await;

    let tip = server
        .compact
        .get_latest_block(ChainSpec {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(tip.height, 663250);

    // The real mainnet block parses into a compact block at its height.
    let block = server
        .compact
        .get_block(BlockId {
            height: 663150,
            hash: vec![],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(block.height, 663150);

    // The real transaction is found at the height it was mined into.
    let tx = server
        .compact
        .get_transaction(TxFilter {
            hash: wire_txid(RECV_TXID_DISPLAY),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(tx.height, 663190);
    assert_eq!(tx.data, recv_tx());
}
