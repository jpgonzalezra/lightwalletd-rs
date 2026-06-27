//! Unit tests for the transparent-address methods (balance, UTXOs, transaction listings).

use std::sync::Arc;

use serde_json::json;
use tonic::{Code, Request};

use crate::node;
use crate::proto::compact_tx_streamer_server::CompactTxStreamer;
use crate::proto::{
    AddressList, Balance, BlockId, BlockRange, GetAddressUtxosArg, GetAddressUtxosReply,
    RawTransaction, TransparentAddressBlockFilter,
};
use crate::service::address::{MAX_TADDRESS_TXIDS, collect_utxos};
use crate::testutil::FakeNode;

use super::{TADDR, streamer_with};

fn address_utxo(txid: &str, height: u64) -> node::AddressUtxo {
    serde_json::from_value(json!({
        "address": "t1",
        "txid": txid,
        "outputIndex": 2,
        "script": "abcd",
        "satoshis": 7,
        "height": height,
    }))
    .unwrap()
}

#[tokio::test]
async fn collect_utxos_reverses_txid_and_applies_start_height_and_max_entries() {
    let utxos = vec![
        address_utxo("00112233", 100),
        address_utxo("44556677", 200),
        address_utxo("8899aabb", 300),
    ];
    let fake = Arc::new(FakeNode {
        address_utxos: Some(utxos),
        ..Default::default()
    });
    let (_dir, streamer) = streamer_with(fake);

    let replies = collect_utxos(
        &streamer,
        &GetAddressUtxosArg {
            addresses: vec![TADDR.to_string()],
            start_height: 150,
            max_entries: 1,
        },
    )
    .await
    .unwrap();

    assert_eq!(
        replies,
        vec![GetAddressUtxosReply {
            address: "t1".to_string(),
            txid: vec![0x77, 0x66, 0x55, 0x44],
            index: 2,
            script: vec![0xab, 0xcd],
            value_zat: 7,
            height: 200,
        }]
    );
}

#[tokio::test]
async fn get_taddress_balance_returns_value_zat() {
    let fake = Arc::new(FakeNode {
        address_balance: Some(serde_json::from_value(json!({ "balance": 4242 })).unwrap()),
        ..Default::default()
    });
    let (_dir, streamer) = streamer_with(fake);

    let response = streamer
        .get_taddress_balance(Request::new(AddressList {
            addresses: vec![TADDR.to_string()],
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response, Balance { value_zat: 4242 });
}

#[tokio::test]
async fn get_taddress_balance_invalid_address_maps_to_invalid_argument() {
    let fake = Arc::new(FakeNode {
        address_balance_err: Some((-5, "parse error: invalid Bech32 encoding".to_string())),
        ..Default::default()
    });
    let (_dir, streamer) = streamer_with(fake);

    let status = streamer
        .get_taddress_balance(Request::new(AddressList {
            addresses: vec![TADDR.to_string()],
        }))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn get_address_utxos_invalid_address_maps_to_invalid_argument() {
    let fake = Arc::new(FakeNode {
        address_utxos_err: Some((-5, "parse error: invalid Bech32 encoding".to_string())),
        ..Default::default()
    });
    let (_dir, streamer) = streamer_with(fake);

    let status = streamer
        .get_address_utxos(Request::new(GetAddressUtxosArg {
            addresses: vec![TADDR.to_string()],
            start_height: 0,
            max_entries: 0,
        }))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn get_taddress_balance_no_information_available_maps_to_not_found() {
    let fake = Arc::new(FakeNode {
        address_balance_err: Some((-5, "No information available for address".to_string())),
        ..Default::default()
    });
    let (_dir, streamer) = streamer_with(fake);

    let status = streamer
        .get_taddress_balance(Request::new(AddressList {
            addresses: vec![TADDR.to_string()],
        }))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::NotFound);
}

#[tokio::test]
async fn get_taddress_transactions_streams_one_raw_tx_per_txid() {
    use tokio_stream::StreamExt;
    let fake = Arc::new(FakeNode {
        address_txids: Some(vec!["aa".to_string()]),
        raw_transaction: Some(
            serde_json::from_value(json!({ "hex": "deadbeef", "height": 100 })).unwrap(),
        ),
        ..Default::default()
    });
    let (_dir, streamer) = streamer_with(fake);

    let filter = TransparentAddressBlockFilter {
        address: TADDR.to_string(),
        range: Some(BlockRange {
            start: Some(BlockId {
                height: 1,
                hash: vec![],
            }),
            end: Some(BlockId {
                height: 2,
                hash: vec![],
            }),
            ..Default::default()
        }),
    };
    let response = streamer
        .get_taddress_transactions(Request::new(filter))
        .await
        .unwrap()
        .into_inner();
    let transactions: Vec<_> = response.collect().await;

    assert_eq!(transactions.len(), 1);
    assert_eq!(
        *transactions[0].as_ref().unwrap(),
        RawTransaction {
            data: vec![0xde, 0xad, 0xbe, 0xef],
            height: 100,
        }
    );
}

#[tokio::test]
async fn get_taddress_balance_malformed_address_rejected_before_node() {
    // The FakeNode panics on any RPC, so a passing test proves the format check rejects locally.
    let (_dir, streamer) = streamer_with(Arc::new(FakeNode::default()));

    let status = streamer
        .get_taddress_balance(Request::new(AddressList {
            addresses: vec!["not_a_real_address".to_string()],
        }))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn get_taddress_transactions_without_range_is_invalid_argument() {
    let (_dir, streamer) = streamer_with(Arc::new(FakeNode::default()));

    let status = streamer
        .get_taddress_transactions(Request::new(TransparentAddressBlockFilter {
            address: TADDR.to_string(),
            range: None,
        }))
        .await
        .err()
        .unwrap();

    assert_eq!(status.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn get_taddress_transactions_without_start_is_invalid_argument() {
    let (_dir, streamer) = streamer_with(Arc::new(FakeNode::default()));

    let status = streamer
        .get_taddress_transactions(Request::new(TransparentAddressBlockFilter {
            address: TADDR.to_string(),
            range: Some(BlockRange {
                start: None,
                end: Some(BlockId {
                    height: 2,
                    hash: vec![],
                }),
                ..Default::default()
            }),
        }))
        .await
        .err()
        .unwrap();

    assert_eq!(status.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn get_taddress_transactions_rejects_too_many_txids() {
    let fake = Arc::new(FakeNode {
        address_txids: Some(vec!["00".to_string(); MAX_TADDRESS_TXIDS + 1]),
        ..Default::default()
    });
    let (_dir, streamer) = streamer_with(fake.clone());

    let status = streamer
        .get_taddress_transactions(Request::new(TransparentAddressBlockFilter {
            address: TADDR.to_string(),
            range: Some(BlockRange {
                start: Some(BlockId {
                    height: 1,
                    hash: vec![],
                }),
                end: Some(BlockId {
                    height: 1_000_000,
                    hash: vec![],
                }),
                ..Default::default()
            }),
        }))
        .await
        .err()
        .unwrap();

    assert_eq!(status.code(), Code::ResourceExhausted);
    // The cap is enforced before any per-txid fetch reaches the node.
    assert!(fake.requested_txid.lock().unwrap().is_none());
}
