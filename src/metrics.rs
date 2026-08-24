//! Prometheus metrics: the shared registry, the request layer, the connection gauge, and the
//! `/metrics` endpoint.
//!
//! [`BoundedMetricsLayer`] wraps the gRPC server and records per-method request counts and latency
//! histograms into `tonic_prometheus_layer`'s registry. This module serves them in Prometheus text
//! format over a small HTTP `/metrics` endpoint on a separate port.
//!
//! [`count_connections`] wraps the listener: a `tower` layer sees requests, and one HTTP/2
//! connection carries many, so a connection's lifetime is only visible around the socket.

use std::collections::HashSet;
use std::io;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use std::task::{Context, Poll};

use axum::Router;
use axum::routing::get;
use http::{Method, Request};
use prometheus::{IntGauge, Registry};
use prost::Message;
use prost_types::FileDescriptorSet;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpListener;
use tokio_stream::{Stream, StreamExt};
use tonic::transport::server::Connected;
use tonic_prometheus_layer::MetricsFuture;
use tonic_prometheus_layer::metrics::GlobalSettings;
use tower::{Layer, Service};

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

/// The path that stands in for anything this build does not serve. It splits into
/// `grpc_service="unknown"`, `grpc_method="unknown"`.
const UNKNOWN_PATH: &str = "/unknown/unknown";

/// The `method` label for a request that did not arrive as `POST`.
const OTHER_VERB: &str = "OTHER";

/// The compiled-in descriptor set could not be decoded, so there is no list of served methods to
/// bound the labels against.
#[derive(Debug, thiserror::Error)]
#[error("the compiled-in gRPC descriptor set could not be decoded")]
pub struct DescriptorDecodeError(#[from] prost::DecodeError);

/// Records the same series `tonic_prometheus_layer` does, with the label values taken from a fixed
/// set instead of from the request (ADR 0035).
///
/// A `tower` layer sits above tonic's router, so the only thing in scope is the `http::Request`,
/// and its verb and path are the client's to choose. Recording those verbatim would let anyone mint
/// Prometheus series that are never reclaimed, so the label pair is either a method this build
/// serves or [`UNKNOWN_PATH`]. Unrouted traffic stays visible as volume in that one bucket.
#[derive(Clone)]
pub struct BoundedMetricsLayer {
    served_methods: Arc<HashSet<String>>,
}

impl BoundedMetricsLayer {
    /// Read the set of served methods out of the descriptor sets compiled into this binary.
    ///
    /// Fails when they cannot be decoded, and the caller aborts startup: a server that carried on
    /// would record every request under [`UNKNOWN_PATH`] and report nothing worth scraping.
    pub fn new() -> Result<Self, DescriptorDecodeError> {
        Ok(Self {
            served_methods: Arc::new(served_methods()?),
        })
    }
}

impl<S> Layer<S> for BoundedMetricsLayer {
    type Service = BoundedMetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        BoundedMetricsService {
            inner,
            served_methods: self.served_methods.clone(),
        }
    }
}

/// The service [`BoundedMetricsLayer`] produces. It passes the request through untouched:
/// normalization decides what the call is *recorded* as, never what the router below sees.
#[derive(Clone)]
pub struct BoundedMetricsService<S> {
    inner: S,
    served_methods: Arc<HashSet<String>>,
}

impl<S, RequestBody, ResponseBody> Service<Request<RequestBody>> for BoundedMetricsService<S>
where
    S: Service<Request<RequestBody>, Response = http::Response<ResponseBody>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = MetricsFuture<S::Future>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: Request<RequestBody>) -> Self::Future {
        let (verb, path) =
            normalized_labels(&self.served_methods, request.method(), request.uri().path());
        let separator = label_separator(&path);
        MetricsFuture::new(verb.to_owned(), path, separator, self.inner.call(request))
    }
}

/// The labels one request is recorded under: its own verb and path when it names a method this
/// build serves, and the constant bucket otherwise.
fn normalized_labels(
    served_methods: &HashSet<String>,
    verb: &Method,
    path: &str,
) -> (&'static str, String) {
    if verb != Method::POST {
        // Not a gRPC call at all, so the path says nothing about which method it wants.
        return (OTHER_VERB, UNKNOWN_PATH.to_owned());
    }
    if served_methods.contains(path) {
        (Method::POST.as_str(), path.to_owned())
    } else {
        (Method::POST.as_str(), UNKNOWN_PATH.to_owned())
    }
}

/// Where `/service/method` splits, in the form [`MetricsFuture`] wants it: the index it slices the
/// path with, rather than a second parse of the same string.
fn label_separator(path: &str) -> Option<NonZeroUsize> {
    let after_leading_slash = path.strip_prefix('/')?;
    NonZeroUsize::new(after_leading_slash.find('/')? + 1)
}

/// Every gRPC path this binary can answer: the methods in `service.proto` and `darkside.proto`,
/// plus reflection's own.
///
/// The descriptor set is the one `build.rs` emits and the reflection service already serves, so
/// this set cannot drift from the `.proto` files. It is the union of both serve paths rather than
/// the services a given process registers, because `server_builder` runs before either mode adds
/// its services. A darkside method labelled honestly on a live server costs one series that is
/// already accounted for.
fn served_methods() -> Result<HashSet<String>, DescriptorDecodeError> {
    let mut paths = HashSet::new();
    collect_method_paths(crate::proto::FILE_DESCRIPTOR_SET, &mut paths)?;
    collect_method_paths(tonic_reflection::pb::v1::FILE_DESCRIPTOR_SET, &mut paths)?;
    Ok(paths)
}

fn collect_method_paths(
    descriptor_set: &[u8],
    paths: &mut HashSet<String>,
) -> Result<(), DescriptorDecodeError> {
    for file in FileDescriptorSet::decode(descriptor_set)?.file {
        let package = file.package.unwrap_or_default();
        for service in file.service {
            let service_name = service.name.unwrap_or_default();
            let qualified = if package.is_empty() {
                service_name
            } else {
                format!("{package}.{service_name}")
            };
            for method in service.method {
                let method_name = method.name.unwrap_or_default();
                paths.insert(format!("/{qualified}/{method_name}"));
            }
        }
    }
    Ok(())
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

    const GET_LATEST_BLOCK: &str = "/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLatestBlock";

    fn connections_current() -> i64 {
        CONNECTIONS.as_ref().map_or(0, IntGauge::get)
    }

    #[test]
    fn the_served_methods_span_both_serve_paths_and_reflection() {
        let methods = served_methods().unwrap();
        assert!(
            [
                GET_LATEST_BLOCK,
                "/cash.z.wallet.sdk.rpc.DarksideStreamer/Reset",
                "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo",
            ]
            .iter()
            .all(|path| methods.contains(*path)),
            "{methods:?}"
        );
    }

    #[test]
    fn a_call_to_a_served_method_keeps_its_own_labels() {
        let methods = served_methods().unwrap();

        let labels = normalized_labels(&methods, &Method::POST, GET_LATEST_BLOCK);

        assert_eq!(labels, ("POST", GET_LATEST_BLOCK.to_owned()));
    }

    #[test]
    fn an_invented_path_is_recorded_as_unknown() {
        let methods = served_methods().unwrap();

        let labels = normalized_labels(&methods, &Method::POST, "/A/0");

        assert_eq!(labels, ("POST", UNKNOWN_PATH.to_owned()));
    }

    #[test]
    fn a_verb_that_is_not_the_one_grpc_uses_is_recorded_as_unknown() {
        let methods = served_methods().unwrap();

        let labels = normalized_labels(&methods, &Method::GET, GET_LATEST_BLOCK);

        assert_eq!(labels, (OTHER_VERB, UNKNOWN_PATH.to_owned()));
    }

    /// The separator is an index the recording future slices the path with, so what has to be right
    /// is the split it produces, not the number itself.
    #[test]
    fn the_separator_splits_a_path_into_its_service_and_method() {
        let split = |path: &str| {
            let separator = usize::from(label_separator(path).unwrap());
            (
                path[1..separator].to_owned(),
                path[separator + 1..].to_owned(),
            )
        };

        assert_eq!(
            (split(GET_LATEST_BLOCK), split(UNKNOWN_PATH)),
            (
                (
                    "cash.z.wallet.sdk.rpc.CompactTxStreamer".to_owned(),
                    "GetLatestBlock".to_owned()
                ),
                ("unknown".to_owned(), "unknown".to_owned())
            )
        );
    }

    /// Normalization decides what a call is recorded as and nothing else. The router below has to
    /// see the verb and URI the client sent, or an unrouted path stops being answered the way it is
    /// answered today.
    #[tokio::test]
    async fn the_inner_service_sees_the_request_the_client_sent() {
        let received = Arc::new(std::sync::Mutex::new(None));
        let recorder = received.clone();
        let inner = tower::service_fn(move |request: Request<()>| {
            *recorder.lock().unwrap() = Some((request.method().clone(), request.uri().clone()));
            std::future::ready(Ok::<_, std::convert::Infallible>(http::Response::new(())))
        });
        let mut service = BoundedMetricsLayer::new().unwrap().layer(inner);
        let request = Request::builder()
            .method(Method::PUT)
            .uri("/A/0")
            .body(())
            .unwrap();

        service.call(request).await.unwrap();

        let sent = (Method::PUT, "/A/0".parse::<http::Uri>().unwrap());
        assert_eq!(received.lock().unwrap().take(), Some(sent));
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
