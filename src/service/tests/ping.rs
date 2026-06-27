//! Unit tests for the `Ping` method (gating and counters).

use std::sync::Arc;

use tonic::{Code, Request};

use crate::proto::compact_tx_streamer_server::CompactTxStreamer;
use crate::proto::{Duration, PingResponse};
use crate::testutil::FakeNode;

use super::streamer_with;

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
