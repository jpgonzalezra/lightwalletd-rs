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

#[tokio::test(start_paused = true)]
async fn ping_cancelled_mid_interval_gives_its_in_flight_slot_back() {
    let (_dir, streamer) = streamer_with(Arc::new(FakeNode::default()));
    let streamer = streamer.with_ping_enabled(true);

    let cancelled = tokio::time::timeout(
        std::time::Duration::from_millis(1),
        streamer.ping(Request::new(Duration {
            interval_us: 60_000_000,
        })),
    )
    .await;
    assert!(
        cancelled.is_err(),
        "the ping must still be sleeping when the timeout drops it"
    );

    let response = streamer
        .ping(Request::new(Duration { interval_us: 0 }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response, PingResponse { entry: 1, exit: 0 });
}
