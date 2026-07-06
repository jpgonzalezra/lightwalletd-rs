//! Unit tests for the subtree-roots method.

use std::sync::Arc;

use tokio_stream::StreamExt;
use tonic::Request;

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
