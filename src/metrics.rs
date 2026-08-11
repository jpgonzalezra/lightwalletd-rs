//! Prometheus metrics: the shared registry, the connection gauge, and the `/metrics` endpoint.
//!
//! The `MetricsLayer` on the gRPC server records per-method request counts and latency histograms
//! into `tonic_prometheus_layer`'s registry; this module serves them in the Prometheus text format
//! over a small HTTP `/metrics` endpoint on a separate port.
//!
//! [`count_connections`] wraps the listener: a `tower` layer sees requests, and one HTTP/2
//! connection carries many, so a connection's lifetime is only visible around the socket.

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::LazyLock;
use std::task::{Context, Poll};

use axum::Router;
use axum::routing::get;
use prometheus::{IntGauge, Registry};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpListener;
use tokio_stream::{Stream, StreamExt};
use tonic::transport::server::Connected;
use tonic_prometheus_layer::metrics::GlobalSettings;

/// The name lightwalletd deployments already scrape, so their dashboards work against this server
/// too.
const CONNECTIONS_NAME: &str = "grpc_server_connections_current";
const CONNECTIONS_HELP: &str = "Number of currently active gRPC client connections.";

/// The registry that both this module and the request layer record into.
///
/// `tonic_prometheus_layer` keeps its own registry private and creates a default one the first time
/// it touches a metric, so ours has to be installed before that happens: [`init`] does it, and every
/// path that can reach a metric calls it first.
static REGISTRY: LazyLock<Registry> = LazyLock::new(|| {
    let settings = GlobalSettings::default();
    let registry = settings.registry.clone();
    if let Err(error) = tonic_prometheus_layer::metrics::try_init_settings(settings) {
        tracing::error!(%error, "the metrics registry was initialized elsewhere first");
    }
    registry
});

/// The connection gauge, `None` if it could not be registered, in which case `/metrics` reports
/// everything else and leaves this series out.
static CONNECTIONS: LazyLock<Option<IntGauge>> = LazyLock::new(|| match register_connections() {
    Ok(gauge) => Some(gauge),
    Err(error) => {
        tracing::error!(%error, "could not register {CONNECTIONS_NAME}; it will not be reported");
        None
    }
});

fn register_connections() -> Result<IntGauge, prometheus::Error> {
    let gauge = IntGauge::new(CONNECTIONS_NAME, CONNECTIONS_HELP)?;
    REGISTRY.register(Box::new(gauge.clone()))?;
    Ok(gauge)
}

/// Install the shared registry and the metrics this module owns.
///
/// Called from every entry point that can touch a metric, well before the first request:
/// [`crate::server_builder`], which both serve paths go through, and [`serve`].
pub fn init() {
    LazyLock::force(&CONNECTIONS);
}

/// Serve `/metrics` on `addr` until the process exits.
pub async fn serve(addr: SocketAddr) -> anyhow::Result<()> {
    serve_on(TcpListener::bind(addr).await?).await
}

/// Serve `/metrics` on a listener that is already bound, so a caller that asked for an ephemeral
/// port knows which one it got.
pub async fn serve_on(listener: TcpListener) -> anyhow::Result<()> {
    init();
    let app = Router::new().route("/metrics", get(encode));
    axum::serve(listener, app).await?;
    Ok(())
}

/// Handler returning the current metrics in Prometheus text format.
async fn encode() -> String {
    tonic_prometheus_layer::metrics::encode_to_string().unwrap_or_default()
}

/// Count every connection `incoming` yields in `grpc_server_connections_current`.
///
/// A connection is counted from the moment it is accepted, which is before the TLS handshake and
/// before any gRPC request rides it: the gauge answers "how many sockets is this process holding",
/// which is what a connection leak shows up in.
pub fn count_connections<S, IO, IE>(
    incoming: S,
) -> impl Stream<Item = Result<CountedConnection<IO>, IE>>
where
    S: Stream<Item = Result<IO, IE>>,
{
    incoming.map(|connection| connection.map(CountedConnection::new))
}

/// An accepted connection, counted for as long as it is alive.
#[derive(Debug)]
pub struct CountedConnection<IO> {
    inner: IO,
}

impl<IO> CountedConnection<IO> {
    fn new(inner: IO) -> Self {
        if let Some(gauge) = &*CONNECTIONS {
            gauge.inc();
        }
        Self { inner }
    }
}

impl<IO> Drop for CountedConnection<IO> {
    fn drop(&mut self) {
        if let Some(gauge) = &*CONNECTIONS {
            gauge.dec();
        }
    }
}

impl<IO: Connected> Connected for CountedConnection<IO> {
    type ConnectInfo = IO::ConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.inner.connect_info()
    }
}

impl<IO: AsyncRead + Unpin> AsyncRead for CountedConnection<IO> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(context, buffer)
    }
}

impl<IO: AsyncWrite + Unpin> AsyncWrite for CountedConnection<IO> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(context, buffer)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }

    // Delegated rather than left to the default implementation, which would turn hyper's vectored
    // writes into one syscall per buffer.
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(context, buffers)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn connections_current() -> i64 {
        CONNECTIONS.as_ref().map_or(0, IntGauge::get)
    }

    #[tokio::test]
    async fn the_gauge_follows_an_accepted_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut incoming = Box::pin(count_connections(
            tokio_stream::wrappers::TcpListenerStream::new(listener),
        ));

        let idle = connections_current();
        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let accepted = incoming.next().await.unwrap().unwrap();
        let connected = connections_current();
        drop(accepted);
        drop(client);

        assert_eq!((idle, connected, connections_current()), (0, 1, 0));
    }
}
