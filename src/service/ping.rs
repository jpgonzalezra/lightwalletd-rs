//! The `Ping` method (testing only): sleeps for the requested interval, reports calls in flight.

use std::sync::atomic::{AtomicI64, Ordering};

use tonic::{Request, Response, Status};

use crate::proto::{Duration, PingResponse};

use super::Streamer;

/// A slot in the in-flight count, given back when the guard goes away. A cancelled call drops the
/// handler future mid-sleep, so nothing after the `.await` runs and `Drop` is the only hook left.
struct InFlight<'a> {
    count: &'a AtomicI64,
    held: bool,
}

impl<'a> InFlight<'a> {
    /// Take a slot and report the count with this request included.
    fn enter(count: &'a AtomicI64) -> (Self, i64) {
        let entry = count.fetch_add(1, Ordering::SeqCst) + 1;
        (Self { count, held: true }, entry)
    }

    /// Give the slot back and report the count without this request. Read and decrement are one
    /// atomic operation, so the number is the count at the instant this call left.
    fn exit(mut self) -> i64 {
        self.held = false;
        self.count.fetch_sub(1, Ordering::SeqCst) - 1
    }
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        if self.held {
            self.count.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

pub(super) async fn ping(
    streamer: &Streamer,
    request: Request<Duration>,
) -> Result<Response<PingResponse>, Status> {
    if !streamer.ping_enable {
        return Err(Status::failed_precondition(
            "Ping not enabled, start lightwalletd with --ping-very-insecure",
        ));
    }
    let interval_us = request.into_inner().interval_us;
    let (in_flight, entry) = InFlight::enter(&streamer.ping_count);
    if interval_us > 0 {
        tokio::time::sleep(std::time::Duration::from_micros(interval_us as u64)).await;
    }
    let exit = in_flight.exit();
    Ok(Response::new(PingResponse { entry, exit }))
}
