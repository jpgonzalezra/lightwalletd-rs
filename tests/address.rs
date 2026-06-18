//! Transparent-address data staged through the control plane: `GetTaddressTransactions` filters by
//! address and height range, and `GetAddressUtxos` returns staged UTXOs with the txid in display order.

mod common;

use common::{TestServer, recv_tx, wire_txid};
use lightwalletd_rs::proto::{
    BlockId, BlockRange, DarksideAddressTransaction, GetAddressUtxosArg, GetAddressUtxosReply,
    RawTransaction, TransparentAddressBlockFilter,
};

const ADDRESS: &str = "t1ScrubbedBeforePublicationPlan001aaaaa";

async fn taddress_txs(server: &mut TestServer, start: u64, end: u64) -> Vec<RawTransaction> {
    let mut stream = server
        .compact
        .get_taddress_transactions(TransparentAddressBlockFilter {
            address: ADDRESS.to_string(),
            range: Some(BlockRange {
                start: Some(BlockId {
                    height: start,
                    hash: vec![],
                }),
                end: Some(BlockId {
                    height: end,
                    hash: vec![],
                }),
                ..Default::default()
            }),
        })
        .await
        .unwrap()
        .into_inner();
    let mut txs = Vec::new();
    while let Some(tx) = stream.message().await.unwrap() {
        txs.push(tx);
    }
    txs
}

#[tokio::test]
async fn taddress_transactions_filter_by_address_and_height_range() {
    let mut server = TestServer::start().await;
    server.reset(663150, "bad", "x").await;

    let raw = recv_tx();
    server
        .darkside
        .add_address_transaction(DarksideAddressTransaction {
            address: ADDRESS.to_string(),
            data: raw.clone(),
            height: 644337,
        })
        .await
        .unwrap();

    // In range (inclusive at both ends): the staged transaction is returned at its mined height.
    let in_range = taddress_txs(&mut server, 644337, 650510).await;
    assert_eq!(in_range.len(), 1);
    assert_eq!(in_range[0].data, raw);
    assert_eq!(in_range[0].height, 644337);
    assert_eq!(taddress_txs(&mut server, 2, 644337).await.len(), 1);

    // Out of range on either side: nothing is returned.
    assert!(taddress_txs(&mut server, 644338, 650510).await.is_empty());
    assert!(taddress_txs(&mut server, 2, 644336).await.is_empty());
}

#[tokio::test]
async fn address_utxos_round_trip_with_display_order_txid() {
    let mut server = TestServer::start().await;
    server.reset(663150, "bad", "x").await;

    let wire = wire_txid("0821a89be7f2fc1311792c3fa1dd2171a8cdfb2effd98590cbd5ebcdcfcf491f");
    server
        .darkside
        .add_address_utxo(GetAddressUtxosReply {
            address: ADDRESS.to_string(),
            txid: wire.clone(),
            index: 1,
            script: vec![0xab, 0xcd],
            value_zat: 625_100_000,
            height: 663190,
        })
        .await
        .unwrap();

    let reply = server
        .compact
        .get_address_utxos(GetAddressUtxosArg {
            addresses: vec![ADDRESS.to_string()],
            start_height: 0,
            max_entries: 0,
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(reply.address_utxos.len(), 1);
    let utxo = &reply.address_utxos[0];
    assert_eq!(utxo.address, ADDRESS);
    // The wire txid round-trips: darkside stores it display-order, the service reverses it back.
    assert_eq!(utxo.txid, wire);
    assert_eq!(utxo.index, 1);
    assert_eq!(utxo.value_zat, 625_100_000);
    assert_eq!(utxo.height, 663190);
}
