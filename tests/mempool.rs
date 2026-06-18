//! The darkside staging area is served as the mempool: a staged-but-unapplied transaction shows up in
//! `GetMempoolTx`, and once `ApplyStaged` mines it into a block the mempool drains.

mod common;

use common::{RECV_TXID_DISPLAY, TestServer, recv_tx, testdata_blocks, wire_txid};
use lightwalletd_rs::proto::GetMempoolTxRequest;

async fn mempool_txids(server: &mut TestServer) -> Vec<Vec<u8>> {
    let mut stream = server
        .compact
        .get_mempool_tx(GetMempoolTxRequest::default())
        .await
        .unwrap()
        .into_inner();
    let mut txids = Vec::new();
    while let Some(tx) = stream.message().await.unwrap() {
        txids.push(tx.txid);
    }
    txids
}

#[tokio::test]
async fn staged_transaction_appears_in_mempool_until_mined() {
    let mut server = TestServer::start().await;

    // Apply the testdata blocks first so the staging area is empty before staging the transaction.
    server.reset(380640, "2bb40e60", "main").await;
    server.stage_blocks(testdata_blocks()).await;
    server.apply_staged(380643).await;
    assert!(mempool_txids(&mut server).await.is_empty());

    // A staged-but-unapplied transaction is served as a mempool entry.
    server.stage_transactions(380641, vec![recv_tx()]).await;
    assert_eq!(
        mempool_txids(&mut server).await,
        vec![wire_txid(RECV_TXID_DISPLAY)],
    );

    // Applying it mines the transaction into its block, draining the mempool.
    server.apply_staged(380643).await;
    assert!(mempool_txids(&mut server).await.is_empty());
}
