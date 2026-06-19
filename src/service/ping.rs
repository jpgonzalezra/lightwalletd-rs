//! The `Ping` method (testing only): sleeps for the requested interval and reports concurrency.

use std::sync::atomic::Ordering;

use tonic::{Request, Response, Status};

use crate::proto::{Duration, PingResponse};

use super::Streamer;

pub(super) async fn ping(
    streamer: &Streamer,
    request: Request<Duration>,
) -> Result<Response<PingResponse>, Status> {
    let interval_us = request.into_inner().interval_us;
    let entry = streamer.ping_count.fetch_add(1, Ordering::SeqCst) + 1;
    if interval_us > 0 {
        tokio::time::sleep(std::time::Duration::from_micros(interval_us as u64)).await;
    }
    let exit = streamer.ping_count.load(Ordering::SeqCst);
    streamer.ping_count.fetch_sub(1, Ordering::SeqCst);
    Ok(Response::new(PingResponse { entry, exit }))
}
