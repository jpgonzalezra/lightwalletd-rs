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
// Ironwood RPC support lands). That's not a server failure (the subtree can't exist yet), so the
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

// The protocol types a subtree index as `uint32`, but the node addresses subtrees with a `u16`.
// `u16::MAX` is the last index it can answer for, and it must still reach the node: rejecting it
// would refuse a legitimate request.
#[tokio::test]
async fn highest_addressable_start_index_reaches_the_node() {
    let node = Arc::new(FakeNode {
        subtrees: Some(GetSubtrees { subtrees: vec![] }),
        ..Default::default()
    });
    let (_dir, streamer) = streamer_with(node.clone());

    streamer
        .get_subtree_roots(Request::new(GetSubtreeRootsArg {
            start_index: u16::MAX as u32,
            shielded_protocol: ShieldedProtocol::Sapling as i32,
            max_entries: 0,
        }))
        .await
        .unwrap();

    assert_eq!(
        *node.requested_subtree_params.lock().unwrap(),
        Some(("sapling".to_string(), u16::MAX as u32, 0))
    );
}

// One past what the node can address. The node would answer `Invalid params`, which carries a
// generic code and would surface as a retryable `Unavailable`, so a wallet would keep retrying
// input that can never succeed. It has to be rejected here as the client error it is, without
// consulting the node at all.
#[tokio::test]
async fn start_index_above_the_node_range_is_rejected_without_a_node_call() {
    let node = Arc::new(FakeNode {
        subtrees: Some(GetSubtrees { subtrees: vec![] }),
        ..Default::default()
    });
    let (_dir, streamer) = streamer_with(node.clone());

    let status = streamer
        .get_subtree_roots(Request::new(GetSubtreeRootsArg {
            start_index: u16::MAX as u32 + 1,
            shielded_protocol: ShieldedProtocol::Sapling as i32,
            max_entries: 0,
        }))
        .await
        .err()
        .unwrap();

    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(*node.requested_subtree_params.lock().unwrap(), None);
}

// A limit is a ceiling, not a position: one past the range asks for all of the subtrees, which is
// exactly what the unlimited `0` expresses. Clamping to `u16::MAX` would instead cap the count one
// short of the `u16::MAX + 1` subtrees a full pool can hold.
#[tokio::test]
async fn max_entries_above_the_node_range_is_forwarded_as_unlimited() {
    let node = Arc::new(FakeNode {
        subtrees: Some(GetSubtrees { subtrees: vec![] }),
        ..Default::default()
    });
    let (_dir, streamer) = streamer_with(node.clone());

    streamer
        .get_subtree_roots(Request::new(GetSubtreeRootsArg {
            start_index: 0,
            shielded_protocol: ShieldedProtocol::Orchard as i32,
            max_entries: 70_000,
        }))
        .await
        .unwrap();

    assert_eq!(
        *node.requested_subtree_params.lock().unwrap(),
        Some(("orchard".to_string(), 0, 0))
    );
}

// `max_entries == 0` means "no limit" and is forwarded untouched, so that `get_subtrees` keeps
// omitting the third JSON-RPC parameter instead of capping the response at zero entries.
#[tokio::test]
async fn unset_max_entries_is_forwarded_as_zero() {
    let node = Arc::new(FakeNode {
        subtrees: Some(GetSubtrees { subtrees: vec![] }),
        ..Default::default()
    });
    let (_dir, streamer) = streamer_with(node.clone());

    streamer
        .get_subtree_roots(Request::new(GetSubtreeRootsArg {
            start_index: 0,
            shielded_protocol: ShieldedProtocol::Sapling as i32,
            max_entries: 0,
        }))
        .await
        .unwrap();

    assert_eq!(
        *node.requested_subtree_params.lock().unwrap(),
        Some(("sapling".to_string(), 0, 0))
    );
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
