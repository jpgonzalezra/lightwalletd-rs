//! Unit tests for the transaction methods (`GetTransaction`, `SendTransaction`).

use std::sync::Arc;

use serde_json::json;
use tonic::{Code, Request};

use crate::proto::compact_tx_streamer_server::CompactTxStreamer;
use crate::proto::{RawTransaction, SendResponse, TxFilter};
use crate::testutil::FakeNode;

use super::streamer_with;

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
