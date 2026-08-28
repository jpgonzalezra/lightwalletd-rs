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
use crate::service::address::{
    MAX_ADDRESS_UTXOS, MAX_INDEXED_OUTPUTS_PER_BLOCK, MAX_STREAMED_ADDRESSES,
    MAX_TADDRESS_BLOCK_SPAN, MAX_TADDRESS_TXIDS, collect_utxos,
};
use crate::testutil::FakeNode;

use super::{streamer_with, taddr};

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
            addresses: vec![taddr()],
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
async fn collect_utxos_accepts_addresses_up_to_the_cap() {
    let fake = Arc::new(FakeNode {
        address_utxos: Some(Vec::new()),
        ..Default::default()
    });
    let (_dir, streamer) = streamer_with(fake);

    let replies = collect_utxos(
        &streamer,
        &GetAddressUtxosArg {
            addresses: vec![taddr(); MAX_STREAMED_ADDRESSES],
            start_height: 0,
            max_entries: 0,
        },
    )
    .await
    .unwrap();

    assert_eq!(replies, Vec::new());
}

#[tokio::test]
async fn collect_utxos_rejects_too_many_addresses() {
    // The FakeNode panics on any RPC, so a passing test proves the cap rejects before the node call.
    let (_dir, streamer) = streamer_with(Arc::new(FakeNode::default()));

    let status = collect_utxos(
        &streamer,
        &GetAddressUtxosArg {
            addresses: vec![taddr(); MAX_STREAMED_ADDRESSES + 1],
            start_height: 0,
            max_entries: 0,
        },
    )
    .await
    .unwrap_err();

    assert_eq!(status.code(), Code::ResourceExhausted);
}

/// A node holding `count` unspent outputs, all at the same height, for whatever addresses are asked.
fn node_holding_utxos(count: usize) -> Arc<FakeNode> {
    Arc::new(FakeNode {
        address_utxos: Some(vec![address_utxo("00112233", 100); count]),
        ..Default::default()
    })
}

fn utxos_arg(max_entries: u32) -> GetAddressUtxosArg {
    GetAddressUtxosArg {
        addresses: vec![taddr()],
        start_height: 0,
        max_entries,
    }
}

#[tokio::test]
async fn collect_utxos_accepts_a_result_at_the_reply_cap() {
    let (_dir, streamer) = streamer_with(node_holding_utxos(MAX_ADDRESS_UTXOS));

    let replies = collect_utxos(&streamer, &utxos_arg(0)).await.unwrap();

    assert_eq!(replies.len(), MAX_ADDRESS_UTXOS);
}

#[tokio::test]
async fn collect_utxos_rejects_a_result_over_the_reply_cap() {
    let (_dir, streamer) = streamer_with(node_holding_utxos(MAX_ADDRESS_UTXOS + 1));

    let status = collect_utxos(&streamer, &utxos_arg(0)).await.unwrap_err();

    assert_eq!(status.code(), Code::ResourceExhausted);
}

#[tokio::test]
async fn collect_utxos_serves_a_bounded_max_entries_over_an_oversized_result() {
    let (_dir, streamer) = streamer_with(node_holding_utxos(MAX_ADDRESS_UTXOS + 1));

    let replies = collect_utxos(&streamer, &utxos_arg(5)).await.unwrap();

    assert_eq!(replies.len(), 5);
}

/// `startHeight` selects a block, so a client can only page past a height it has read in full. A
/// group as large as one block can produce still fits in a single reply, which is what keeps paging
/// moving; the cap itself is held above that bound by a `const` assertion in `address.rs`.
#[tokio::test]
async fn collect_utxos_serves_a_whole_height_group_the_size_of_a_full_block() {
    let utxos = vec![address_utxo("00112233", 100); MAX_INDEXED_OUTPUTS_PER_BLOCK];
    let fake = Arc::new(FakeNode {
        address_utxos: Some(utxos),
        ..Default::default()
    });
    let (_dir, streamer) = streamer_with(fake);

    let replies = collect_utxos(&streamer, &utxos_arg(0)).await.unwrap();

    assert_eq!(replies.len(), MAX_INDEXED_OUTPUTS_PER_BLOCK);
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
            addresses: vec![taddr()],
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
            addresses: vec![taddr()],
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
            addresses: vec![taddr()],
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
            addresses: vec![taddr()],
        }))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::NotFound);
}

#[tokio::test]
async fn get_taddress_balance_rejects_too_many_addresses() {
    // The FakeNode panics on any RPC, so a passing test proves the cap rejects before the node call.
    let (_dir, streamer) = streamer_with(Arc::new(FakeNode::default()));

    let status = streamer
        .get_taddress_balance(Request::new(AddressList {
            addresses: vec![taddr(); MAX_STREAMED_ADDRESSES + 1],
        }))
        .await
        .err()
        .unwrap();

    assert_eq!(status.code(), Code::ResourceExhausted);
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
        address: taddr(),
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
            address: taddr(),
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
            address: taddr(),
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

/// A node whose chain tip is at `tip` and whose address index matches nothing.
fn node_at_tip(tip: u64) -> Arc<FakeNode> {
    Arc::new(FakeNode {
        blockchain_info: Some(
            serde_json::from_value(json!({
                "chain": "main",
                "blocks": tip,
                "bestblockhash": "00",
                "consensus": { "chaintip": "00000000" },
            }))
            .unwrap(),
        ),
        address_txids: Some(Vec::new()),
        ..Default::default()
    })
}

fn range_filter(start: u64, end: Option<u64>) -> TransparentAddressBlockFilter {
    TransparentAddressBlockFilter {
        address: taddr(),
        range: Some(BlockRange {
            start: Some(BlockId {
                height: start,
                hash: vec![],
            }),
            end: end.map(|height| BlockId {
                height,
                hash: vec![],
            }),
            ..Default::default()
        }),
    }
}

#[tokio::test]
async fn get_taddress_transactions_without_an_end_scans_up_to_the_tip() {
    let fake = node_at_tip(2_000_000);
    let (_dir, streamer) = streamer_with(fake.clone());

    streamer
        .get_taddress_transactions(Request::new(range_filter(1_500_000, None)))
        .await
        .unwrap();

    assert_eq!(
        *fake.requested_txid_range.lock().unwrap(),
        Some((1_500_000, 2_000_000))
    );
}

#[tokio::test]
async fn get_taddress_transactions_with_a_zero_end_scans_up_to_the_tip() {
    let fake = node_at_tip(2_000_000);
    let (_dir, streamer) = streamer_with(fake.clone());

    streamer
        .get_taddress_transactions(Request::new(range_filter(1_500_000, Some(0))))
        .await
        .unwrap();

    assert_eq!(
        *fake.requested_txid_range.lock().unwrap(),
        Some((1_500_000, 2_000_000))
    );
}

#[tokio::test]
async fn get_taddress_transactions_keeps_an_explicit_end() {
    let fake = node_at_tip(2_000_000);
    let (_dir, streamer) = streamer_with(fake.clone());

    streamer
        .get_taddress_transactions(Request::new(range_filter(1_500_000, Some(1_600_000))))
        .await
        .unwrap();

    assert_eq!(
        *fake.requested_txid_range.lock().unwrap(),
        Some((1_500_000, 1_600_000))
    );
}

#[tokio::test]
async fn get_taddress_transactions_rejects_an_over_wide_range() {
    // The FakeNode panics on any RPC, so a passing test proves the span check rejects before it.
    let (_dir, streamer) = streamer_with(Arc::new(FakeNode::default()));

    let status = streamer
        .get_taddress_transactions(Request::new(range_filter(0, Some(u64::MAX))))
        .await
        .err()
        .unwrap();

    assert_eq!(status.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn get_taddress_transactions_accepts_a_range_at_the_span_limit() {
    let fake = node_at_tip(2_000_000);
    let (_dir, streamer) = streamer_with(fake.clone());

    streamer
        .get_taddress_transactions(Request::new(range_filter(
            1,
            Some(1 + MAX_TADDRESS_BLOCK_SPAN),
        )))
        .await
        .unwrap();

    assert_eq!(
        *fake.requested_txid_range.lock().unwrap(),
        Some((1, 1 + MAX_TADDRESS_BLOCK_SPAN))
    );
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
            address: taddr(),
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
