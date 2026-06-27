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
