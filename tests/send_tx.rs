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

    // Height 0 is `service.proto`'s "in the mempool": these transactions have not been mined.
    assert_eq!(
        read_incoming(&mut server).await,
        vec![RawTransaction {
            data: raw,
            height: 0
        }]
    );
}

/// Reading the pool does not drain it: `ClearIncomingTransactions` is what empties it, which is what
/// `proto/darkside.proto` documents. A harness that polls therefore sees the same transaction on
/// every cycle, and one written to expect a drain re-stages and re-mines it.
#[tokio::test]
async fn reading_the_incoming_pool_leaves_it_in_place() {
    let mut server = TestServer::start().await;
    server.reset(663150, "bad", "x").await;

    let raw = recv_tx();
    server
        .compact
        .send_transaction(RawTransaction {
            data: raw.clone(),
            height: 0,
        })
        .await
        .unwrap();
    read_incoming(&mut server).await;

    assert_eq!(
        read_incoming(&mut server).await,
        vec![RawTransaction {
            data: raw,
            height: 0
        }]
    );
}

async fn read_incoming(server: &mut TestServer) -> Vec<RawTransaction> {
    let mut stream = server
        .darkside
        .get_incoming_transactions(Empty {})
        .await
        .unwrap()
        .into_inner();

    let mut received = Vec::new();
    while let Some(transaction) = stream.message().await.unwrap() {
        received.push(transaction);
    }
    received
}
