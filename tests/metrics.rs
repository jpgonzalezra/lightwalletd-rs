//! `grpc_server_connections_current` end to end: the production transport stack, a real client
//! connection, and the `/metrics` endpoint an operator would scrape.
//!
//! The gauge is process-global, so this file keeps a single test; a second one running next to it
//! would count its own connections into the same series.

use std::net::SocketAddr;
use std::time::Duration;

use lightwalletd_rs::config::{
    DEFAULT_KEEPALIVE_INTERVAL_SECS, DEFAULT_KEEPALIVE_TIMEOUT_SECS,
    DEFAULT_MAX_CONCURRENT_STREAMS, ServerLimits,
};
use lightwalletd_rs::proto::compact_tx_streamer_client::CompactTxStreamerClient;
use lightwalletd_rs::proto::compact_tx_streamer_server::CompactTxStreamerServer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::test]
async fn the_connection_gauge_rises_and_falls_with_a_client() {
    let cache_dir = tempfile::tempdir().unwrap();
    let (streamer, _darkside_service, _state, _shutdown) =
        lightwalletd_rs::darkside_components(&cache_dir.path().join("blocks.redb")).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let grpc_addr = listener.local_addr().unwrap();
    let incoming = lightwalletd_rs::metrics::count_connections(
        tokio_stream::wrappers::TcpListenerStream::new(listener),
    );
    let limits = ServerLimits {
        max_concurrent_streams: DEFAULT_MAX_CONCURRENT_STREAMS,
        keepalive_interval: Duration::from_secs(DEFAULT_KEEPALIVE_INTERVAL_SECS),
        keepalive_timeout: Duration::from_secs(DEFAULT_KEEPALIVE_TIMEOUT_SECS),
    };
    tokio::spawn(async move {
        lightwalletd_rs::server_builder(&limits, None)
            .add_service(CompactTxStreamerServer::new(streamer))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    let metrics_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let metrics_addr = metrics_listener.local_addr().unwrap();
    tokio::spawn(lightwalletd_rs::metrics::serve_on(metrics_listener));

    let idle = wait_for_gauge(metrics_addr, 0).await;
    // An eager connect, so the gauge moves on the connection itself rather than on a first request.
    let client = CompactTxStreamerClient::connect(format!("http://{grpc_addr}"))
        .await
        .unwrap();
    let connected = wait_for_gauge(metrics_addr, 1).await;
    drop(client);
    let closed = wait_for_gauge(metrics_addr, 0).await;

    assert_eq!((idle, connected, closed), (Some(0), Some(1), Some(0)));
}

/// Scrape until the gauge reads `expected`, giving up after a few seconds and returning whatever it
/// read last so a failure reports the value instead of a timeout.
async fn wait_for_gauge(addr: SocketAddr, expected: i64) -> Option<i64> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let value = scrape_gauge(addr).await;
        if value == Some(expected) || tokio::time::Instant::now() >= deadline {
            return value;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The value of `grpc_server_connections_current` in the Prometheus text `/metrics` returns, read
/// over a hand-built HTTP/1.0 request so the response ends at EOF.
async fn scrape_gauge(addr: SocketAddr) -> Option<i64> {
    let mut socket = TcpStream::connect(addr).await.ok()?;
    socket
        .write_all(b"GET /metrics HTTP/1.0\r\n\r\n")
        .await
        .ok()?;
    let mut response = String::new();
    socket.read_to_string(&mut response).await.ok()?;
    response
        .lines()
        .find_map(|line| line.strip_prefix("grpc_server_connections_current "))
        .and_then(|value| value.trim().parse().ok())
}
