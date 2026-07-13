//! Unit tests for the tree-state methods and the `node_tree_state_to_proto` mapping.

use std::sync::Arc;

use serde_json::json;
use tonic::{Code, Request};

use crate::node;
use crate::proto::compact_tx_streamer_server::CompactTxStreamer;
use crate::proto::{BlockId, TreeState};
use crate::service::treestate::node_tree_state_to_proto;
use crate::testutil::FakeNode;

use super::streamer_with;

/// A display-order hash and its wire-order (little-endian) encoding, for by-hash requests.
fn display_and_wire_hash() -> (&'static str, Vec<u8>) {
    let display = "00000000005a1db0281385a6eeb05d7beff2a42f17cedc94280215f087b5e07d";
    (
        display,
        crate::encoding::display_hex_to_wire(display).unwrap(),
    )
}

fn sample_tree_state() -> node::GetTreeState {
    serde_json::from_value(json!({
        "hash": "abcd",
        "height": 1234,
        "time": 42,
        "sapling": { "commitments": { "finalState": "aa" } },
    }))
    .unwrap()
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
async fn get_tree_state_rejects_a_wrong_length_hash() {
    let (_dir, streamer) = streamer_with(Arc::new(FakeNode::default()));

    let status = streamer
        .get_tree_state(Request::new(BlockId {
            height: 0,
            hash: vec![0xaa; 31],
        }))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn get_tree_state_by_hash_serves_the_display_order_hash_to_the_node() {
    let (display, wire) = display_and_wire_hash();
    let node = Arc::new(FakeNode {
        treestate: Some(sample_tree_state()),
        ..Default::default()
    });
    let (_dir, streamer) = streamer_with(node.clone());

    streamer
        .get_tree_state(Request::new(BlockId {
            height: 0,
            hash: wire,
        }))
        .await
        .unwrap();

    // BlockID.hash arrives wire-order (little-endian); z_gettreestate wants display-order hex, like
    // every other node RPC that takes a hash.
    assert_eq!(
        node.requested_treestate_id.lock().unwrap().as_deref(),
        Some(display)
    );
}

#[tokio::test]
async fn get_tree_state_height_takes_precedence_over_hash() {
    // Matches Go's `GetTreeState`: `if id.Height > 0 { use height } else { use hash }`.
    let (_, wire) = display_and_wire_hash();
    let node = Arc::new(FakeNode {
        treestate: Some(sample_tree_state()),
        ..Default::default()
    });
    let (_dir, streamer) = streamer_with(node.clone());

    streamer
        .get_tree_state(Request::new(BlockId {
            height: 1234,
            hash: wire,
        }))
        .await
        .unwrap();

    assert_eq!(
        node.requested_treestate_id.lock().unwrap().as_deref(),
        Some("1234")
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
        "ironwood": { "commitments": { "finalState": "cc" } },
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
            ironwood_tree: "cc".to_string(),
        }
    );
}

#[test]
fn node_tree_state_to_proto_defaults_absent_ironwood_to_empty() {
    // A pre-NU6.3 node response: sapling/orchard present, no `ironwood` key.
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
            ironwood_tree: String::new(),
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
