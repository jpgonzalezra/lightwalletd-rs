//! Unit tests for the subtree-roots method.

use std::sync::Arc;

use tokio_stream::StreamExt;
use tonic::{Code, Request};

use crate::node::GetSubtrees;
use crate::proto::compact_tx_streamer_server::CompactTxStreamer;
use crate::proto::{GetSubtreeRootsArg, ShieldedProtocol};
use crate::testutil::FakeNode;

use super::streamer_with;

// The testnet reality right after NU6.3 activation: the node accepts the `ironwood` pool but has
// no completed subtrees yet. The stream must end cleanly with zero items, not error.
#[tokio::test]
async fn ironwood_subtree_roots_with_no_subtrees_is_an_empty_stream() {
    let (_dir, streamer) = streamer_with(Arc::new(FakeNode {
        subtrees: Some(GetSubtrees { subtrees: vec![] }),
        ..Default::default()
    }));

    let stream = streamer
        .get_subtree_roots(Request::new(GetSubtreeRootsArg {
            start_index: 0,
            shielded_protocol: ShieldedProtocol::Ironwood as i32,
            max_entries: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    let roots: Vec<_> = stream.collect().await;

    assert!(roots.is_empty());
}

// A pre-NU6.3 node rejects `z_getsubtreesbyindex ironwood ...` outright, because it doesn't
// recognize the pool name at all (zebra-rpc's `POOL_LIST` is `["sapling", "orchard"]` before the
// Ironwood RPC support lands). That's not a server failure — the subtree can't exist yet — so the
// stream must still end cleanly with zero items, exactly like the "recognized but empty" case above.
#[tokio::test]
async fn pre_ironwood_node_error_yields_an_empty_stream() {
    let (_dir, streamer) = streamer_with(Arc::new(FakeNode {
        subtrees_err: Some((
            -1,
            "invalid pool name, must be one of: [\"sapling\", \"orchard\"]".to_string(),
        )),
        ..Default::default()
    }));

    let stream = streamer
        .get_subtree_roots(Request::new(GetSubtreeRootsArg {
            start_index: 0,
            shielded_protocol: ShieldedProtocol::Ironwood as i32,
            max_entries: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    let roots: Vec<_> = stream.collect().await;

    assert!(roots.is_empty());
}

// An unrelated node error (anything not matching the unrecognized-pool message) must still surface
// as a failed RPC, not be swallowed into an empty stream.
#[tokio::test]
async fn unrelated_node_error_still_propagates() {
    let (_dir, streamer) = streamer_with(Arc::new(FakeNode {
        subtrees_err: Some((-1, "some other failure".to_string())),
        ..Default::default()
    }));

    let status = streamer
        .get_subtree_roots(Request::new(GetSubtreeRootsArg {
            start_index: 0,
            shielded_protocol: ShieldedProtocol::Sapling as i32,
            max_entries: 0,
        }))
        .await
        .err()
        .unwrap();

    assert_eq!(status.code(), Code::Unavailable);
}
