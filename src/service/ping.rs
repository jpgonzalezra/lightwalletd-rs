//! The `Ping` method (testing only): sleeps for the requested interval and reports concurrency.

use std::sync::atomic::Ordering;

use tonic::{Request, Response, Status};

use crate::proto::{Duration, PingResponse};

use super::Streamer;

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
    let entry = streamer.ping_count.fetch_add(1, Ordering::SeqCst) + 1;
    if interval_us > 0 {
        tokio::time::sleep(std::time::Duration::from_micros(interval_us as u64)).await;
    }
    // Read the count after the decrement, so `exit` excludes this request.
    let exit = streamer.ping_count.fetch_sub(1, Ordering::SeqCst) - 1;
    Ok(Response::new(PingResponse { entry, exit }))
}
