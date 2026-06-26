use std::sync::Arc;

use serde_json::json;
use tonic::{Code, Request};

use crate::node::{self, NodeRpc};
use crate::proto::compact_tx_streamer_server::CompactTxStreamer;
use crate::proto::{
    AddressList, Balance, BlockId, BlockRange, ChainSpec, Duration, Empty, GetAddressUtxosArg,
    GetAddressUtxosReply, PingResponse, RawTransaction, SendResponse,
    TransparentAddressBlockFilter, TreeState, TxFilter,
};
use crate::testutil::{FakeNode, temp_cache};

use super::Streamer;
use super::address::collect_utxos;
use super::treestate::node_tree_state_to_proto;

/// A well-formed transparent address (the same one the integration tests use), accepted by
/// `check_taddress` so a test reaches the node path.
const TADDR: &str = "t1ScrubbedBeforePublicationPlan001aaaaa";

/// A valid mainnet unified address, used to exercise the donation-address passthrough.
const DONATION_UA: &str = "u1scrubbedbeforepublicationplan001000000000000000000";

fn streamer_with(node: Arc<dyn NodeRpc>) -> (tempfile::TempDir, Streamer) {
    let (dir, cache) = temp_cache();
    (
        dir,
        Streamer::new(node, Arc::new(cache), "main".to_string(), None),
    )
}

/// A node answering the two RPCs `get_lightd_info` issues (`getinfo` + `getblockchaininfo`).
fn lightd_info_node() -> Arc<FakeNode> {
    Arc::new(FakeNode {
        info: Some(
            serde_json::from_value(
                json!({ "build": "v1.2.3", "subversion": "/MagicBean:5.10.0/" }),
            )
            .unwrap(),
        ),
        blockchain_info: Some(
            serde_json::from_value(json!({
                "chain": "main",
                "blocks": 100,
                "bestblockhash": "00",
                "consensus": { "chaintip": "00000000" },
            }))
            .unwrap(),
        ),
        ..Default::default()
    })
}

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
async fn get_latest_block_reverses_display_hash_to_wire() {
    let display_hash = "0011223344556677889900aabbccddeeff00112233445566778899aabbccddee";
    let fake = Arc::new(FakeNode {
        blockchain_info: Some(
            serde_json::from_value(json!({
                "chain": "main",
                "blocks": 1000,
                "bestblockhash": display_hash,
                "consensus": { "chaintip": "00000000" },
            }))
            .unwrap(),
        ),
        ..Default::default()
    });
    let (_dir, streamer) = streamer_with(fake);

    let response = streamer
        .get_latest_block(Request::new(ChainSpec::default()))
        .await
        .unwrap()
        .into_inner();

    let mut wire = hex::decode(display_hash).unwrap();
    wire.reverse();
    assert_eq!(
        response,
        BlockId {
            height: 1000,
            hash: wire
        }
    );
}

#[tokio::test]
async fn get_transaction_reverses_filter_txid_and_maps_offchain_height() {
    let fake = Arc::new(FakeNode {
        raw_transaction: Some(
            serde_json::from_value(json!({ "hex": "deadbeef", "height": -1 })).unwrap(),
        ),
        ..Default::default()
    });
    let (_dir, streamer) = streamer_with(fake.clone());

    let wire_txid: Vec<u8> = (1u8..=32).collect();
    let response = streamer
        .get_transaction(Request::new(TxFilter {
            hash: wire_txid.clone(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    // The node is called with the display-order (reversed) hex of the wire txid.
    let mut display = wire_txid;
    display.reverse();
    assert_eq!(
        *fake.requested_txid.lock().unwrap(),
        Some(hex::encode(display))
    );
    assert_eq!(
        response,
        RawTransaction {
            data: hex::decode("deadbeef").unwrap(),
            height: u64::MAX,
        }
    );
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

#[tokio::test]
async fn get_tree_state_unspecified_identifier_is_invalid_argument() {
    let (_dir, streamer) = streamer_with(Arc::new(FakeNode::default()));

    let status = streamer
        .get_tree_state(Request::new(BlockId {
            height: 0,
            hash: vec![],
        }))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn get_transaction_unknown_txid_maps_to_not_found() {
    let fake = Arc::new(FakeNode {
        raw_transaction_err: Some((-5, "No such mempool or main chain transaction".to_string())),
        ..Default::default()
    });
    let (_dir, streamer) = streamer_with(fake);

    let status = streamer
        .get_transaction(Request::new(TxFilter {
            hash: vec![0xaa; 32],
            ..Default::default()
        }))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::NotFound);
}

#[tokio::test]
async fn get_transaction_with_unclassified_node_error_maps_to_unavailable() {
    let fake = Arc::new(FakeNode {
        raw_transaction_err: Some((-99, "something unexpected".to_string())),
        ..Default::default()
    });
    let (_dir, streamer) = streamer_with(fake);

    let status = streamer
        .get_transaction(Request::new(TxFilter {
            hash: vec![0xaa; 32],
            ..Default::default()
        }))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::Unavailable);
}

#[tokio::test]
async fn get_transaction_with_wrong_length_hash_is_invalid_argument() {
    let (_dir, streamer) = streamer_with(Arc::new(FakeNode::default()));

    let status = streamer
        .get_transaction(Request::new(TxFilter {
            hash: vec![0xaa, 0xbb, 0xcc, 0xdd],
            ..Default::default()
        }))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn get_transaction_without_hash_is_invalid_argument() {
    let (_dir, streamer) = streamer_with(Arc::new(FakeNode::default()));

    let status = streamer
        .get_transaction(Request::new(TxFilter::default()))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn send_transaction_with_empty_data_is_invalid_argument() {
    let (_dir, streamer) = streamer_with(Arc::new(FakeNode::default()));

    let status = streamer
        .send_transaction(Request::new(RawTransaction {
            data: vec![],
            height: 0,
        }))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn send_transaction_returns_txid_on_success() {
    let fake = Arc::new(FakeNode {
        send_ok: Some("txid-abc".to_string()),
        ..Default::default()
    });
    let (_dir, streamer) = streamer_with(fake);

    let response = streamer
        .send_transaction(Request::new(RawTransaction {
            data: vec![1, 2, 3],
            height: 0,
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        response,
        SendResponse {
            error_code: 0,
            error_message: "txid-abc".to_string(),
        }
    );
}

#[tokio::test]
async fn send_transaction_reports_node_rejection_in_band() {
    let fake = Arc::new(FakeNode {
        send_err: Some((-26, "tx rejected".to_string())),
        ..Default::default()
    });
    let (_dir, streamer) = streamer_with(fake);

    let response = streamer
        .send_transaction(Request::new(RawTransaction {
            data: vec![1, 2, 3],
            height: 0,
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        response,
        SendResponse {
            error_code: -26,
            error_message: "tx rejected".to_string(),
        }
    );
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

#[test]
fn node_tree_state_to_proto_maps_final_state_per_pool() {
    let tree_state: node::GetTreeState = serde_json::from_value(json!({
        "hash": "abcd",
        "height": 1234,
        "time": 42,
        "sapling": { "commitments": { "finalState": "aa" } },
        "orchard": { "commitments": { "finalState": "bb" } },
    }))
    .unwrap();

    assert_eq!(
        node_tree_state_to_proto("main", tree_state).unwrap(),
        TreeState {
            network: "main".to_string(),
            height: 1234,
            hash: "abcd".to_string(),
            time: 42,
            sapling_tree: "aa".to_string(),
            orchard_tree: "bb".to_string(),
        }
    );
}

#[test]
fn node_tree_state_to_proto_rejects_an_empty_frontier() {
    // A pre-Sapling height: the node returns no commitment tree for either pool.
    let tree_state: node::GetTreeState = serde_json::from_value(json!({
        "hash": "abcd",
        "height": 100,
        "time": 42,
    }))
    .unwrap();

    let status = node_tree_state_to_proto("main", tree_state).unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
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
async fn ping_disabled_by_default_returns_failed_precondition() {
    let (_dir, streamer) = streamer_with(Arc::new(FakeNode::default()));

    let status = streamer
        .ping(Request::new(Duration { interval_us: 0 }))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::FailedPrecondition);
}

#[tokio::test]
async fn ping_enabled_reports_entry_and_exit_for_a_single_request() {
    let (_dir, streamer) = streamer_with(Arc::new(FakeNode::default()));
    let streamer = streamer.with_ping_enabled(true);

    let response = streamer
        .ping(Request::new(Duration { interval_us: 0 }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response, PingResponse { entry: 1, exit: 0 });
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
async fn get_lightd_info_advertises_configured_donation_address() {
    let (_dir, streamer) = streamer_with(lightd_info_node());
    let streamer = streamer.with_donation_address(Some(DONATION_UA.to_string()));

    let response = streamer
        .get_lightd_info(Request::new(Empty {}))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.donation_address, DONATION_UA);
}

#[tokio::test]
async fn get_lightd_info_donation_address_empty_when_unset() {
    let (_dir, streamer) = streamer_with(lightd_info_node());

    let response = streamer
        .get_lightd_info(Request::new(Empty {}))
        .await
        .unwrap()
        .into_inner();

    assert!(response.donation_address.is_empty());
}

#[tokio::test]
async fn get_taddress_transactions_rejects_too_many_txids() {
    let fake = Arc::new(FakeNode {
        address_txids: Some(vec![
            "00".to_string();
            super::address::MAX_TADDRESS_TXIDS + 1
        ]),
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

#[tokio::test]
async fn get_lightd_info_reports_sapling_by_branch_id_and_next_pending_upgrade() {
    let fake = Arc::new(FakeNode {
        info: Some(
            serde_json::from_value(json!({ "build": "v1.2.3", "subversion": "/Zebra:5.1.1/" }))
                .unwrap(),
        ),
        blockchain_info: Some(
            serde_json::from_value(json!({
                "chain": "main",
                "blocks": 100,
                "bestblockhash": "00",
                "consensus": { "chaintip": "5437f330" },
                "upgrades": {
                    "76b809bb": { "name": "Sapling", "activationheight": 419200, "status": "active" },
                    "c8e71055": { "name": "NU6", "activationheight": 2726400, "status": "active" },
                    "aaaaaaaa": { "name": "NU7", "activationheight": 9000000, "status": "pending" },
                    "bbbbbbbb": { "name": "NU8", "activationheight": 9999999, "status": "pending" },
                },
            }))
            .unwrap(),
        ),
        ..Default::default()
    });
    let (_dir, streamer) = streamer_with(fake);

    let info = streamer
        .get_lightd_info(Request::new(Empty {}))
        .await
        .unwrap()
        .into_inner();

    // Sapling is found by branch ID; the next upgrade is the lowest-height pending one (NU7, not NU8).
    assert_eq!(info.sapling_activation_height, 419200);
    assert_eq!(info.upgrade_name, "NU7");
    assert_eq!(info.upgrade_height, 9000000);
}
