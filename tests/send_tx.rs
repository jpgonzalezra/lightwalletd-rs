//! `SendTransaction` round-trip: a transaction sent through the production gRPC lands in the darkside
//! incoming pool and comes back verbatim through `GetIncomingTransactions`, with a matching txid.

mod common;

use common::{RECV_TXID_DISPLAY, TestServer, recv_tx};
use lightwalletd_rs::proto::{Empty, RawTransaction};

#[tokio::test]
async fn send_transaction_round_trips_through_incoming_pool() {
    let mut server = TestServer::start().await;
    server.reset(663150, "bad", "x").await;

    let raw = recv_tx();
    let send = server
        .compact
        .send_transaction(RawTransaction {
            data: raw.clone(),
            height: 0,
        })
        .await
        .unwrap()
        .into_inner();

    // On success the txid is reported in the message field, in display order.
    assert_eq!(send.error_code, 0);
    assert_eq!(send.error_message, RECV_TXID_DISPLAY);

    let mut incoming = server
        .darkside
        .get_incoming_transactions(Empty {})
        .await
        .unwrap()
        .into_inner();

    let mut received = Vec::new();
    while let Some(tx) = incoming.message().await.unwrap() {
        received.push(tx.data);
    }
    assert_eq!(received, vec![raw]);
}
