//! Prometheus metrics endpoint.
//!
//! The `MetricsLayer` on the gRPC server records per-method request counts and latency histograms
//! into `tonic_prometheus_layer`'s registry; this module serves them in the Prometheus text format
//! over a small HTTP `/metrics` endpoint on a separate port.

use std::net::SocketAddr;

use axum::Router;
use axum::routing::get;

/// Serve `/metrics` on `addr` until the process exits.
pub async fn serve(addr: SocketAddr) -> anyhow::Result<()> {
    let app = Router::new().route("/metrics", get(encode));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

/// Handler returning the current metrics in Prometheus text format.
async fn encode() -> String {
    tonic_prometheus_layer::metrics::encode_to_string().unwrap_or_default()
}
