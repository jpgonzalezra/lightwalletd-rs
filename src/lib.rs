//! lightwalletd-rs: a Rust lightwalletd for Zcash, usable as a library.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use http::{HeaderName, Method, header};
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tonic_web::GrpcWebLayer;
use tower::layer::util::{Identity as NoLayer, Stack};
use tower::util::{Either, option_layer};
use tower_http::cors::{AllowOrigin, CorsLayer};

pub mod cache;
pub mod config;
pub mod darkside;
pub mod node;
pub mod proto;
pub mod service;
pub mod snapshot;

mod compact;
mod encoding;
mod fetch;
mod filter;
mod ingestor;
mod metrics;
#[cfg(test)]
mod testutil;

use cache::Cache;
use config::Config;
use node::{GetBlockchainInfo, NodeRpc};
use proto::compact_tx_streamer_server::CompactTxStreamerServer;
use proto::darkside_streamer_server::DarksideStreamerServer;

/// Run the server with the resolved configuration until shutdown.
pub async fn run(config: Config) -> anyhow::Result<()> {
    std::fs::create_dir_all(&config.data_dir)?;

    if let Some(metrics_addr) = config.metrics_bind {
        tracing::info!(metrics_bind = %metrics_addr, "serving Prometheus metrics on /metrics");
        tokio::spawn(async move {
            if let Err(error) = metrics::serve(metrics_addr).await {
                tracing::error!(%error, "metrics server failed");
            }
        });
    }

    let mut server = server_builder(&config.limits, config.grpc_web.as_ref());
    if config.grpc_web.is_some() {
        tracing::info!(
            grpc_bind = %config.grpc_bind,
            "serving gRPC-web on the gRPC port; the server now accepts HTTP/1.1 as well as HTTP/2"
        );
    }
    match &config.tls {
        config::TlsConfig::Enabled { cert, key } => {
            let identity = Identity::from_pem(std::fs::read(cert)?, std::fs::read(key)?);
            server = server.tls_config(ServerTlsConfig::new().identity(identity))?;
        }
        config::TlsConfig::GeneratedInsecure { cert_pem, key_pem } => {
            // The loud warning is logged from `Cli::resolve` (config.rs), which runs before the
            // subscriber-consuming `run` is even called, so it is not repeated here.
            let identity = Identity::from_pem(cert_pem.as_bytes(), key_pem.as_bytes());
            server = server.tls_config(ServerTlsConfig::new().identity(identity))?;
        }
        config::TlsConfig::Disabled => {
            tracing::warn!("running without TLS (plaintext) — do not use in production");
        }
    }

    if config.darkside {
        // Mock chain: serve both `CompactTxStreamer` (from the in-memory state) and the
        // `DarksideStreamer` control plane. No real node, no ingestor; the cache stays empty so
        // every block read falls back to the mock node.
        tracing::warn!("running in darkside mode — mock chain, never use in production");
        tracing::info!(grpc_bind = %config.grpc_bind, "lightwalletd-rs darkside starting");

        let (streamer, darkside_service, _state, shutdown) =
            darkside_components(&config.data_dir.join("darkside-blocks.redb"))?;
        let streamer = streamer
            .with_ping_enabled(config.ping_enable)
            .with_donation_address(config.donation_address.clone());

        // Auto-shutdown so a forgotten or leaked darkside process (e.g. a CI job that fails to
        // tear it down) never serves indefinitely — matches the Go reference's fixed 30-minute
        // darkside timeout, which has no way to be disabled (see ADR 0022). Unlike Go's abrupt
        // `Log.Fatal`/process exit, this drives the same graceful-shutdown `Notify` the `Stop` RPC
        // uses, so in-flight requests still drain before the process exits `run` normally.
        let timeout = config.darkside_timeout;
        let timeout_shutdown = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(timeout).await;
            tracing::warn!(
                timeout_secs = timeout.as_secs(),
                "darkside auto-shutdown timeout elapsed; shutting down the mock server"
            );
            timeout_shutdown.notify_one();
        });

        server
            .add_service(CompactTxStreamerServer::new(streamer))
            .add_service(DarksideStreamerServer::new(darkside_service))
            .add_service(reflection_service()?)
            .serve_with_shutdown(config.grpc_bind, darkside_shutdown(shutdown))
            .await?;
    } else {
        // Real node: query the chain, open the cache, spawn the ingestor, serve `CompactTxStreamer`.
        let rpc_client = node::NodeClient::new(&config.node)?;

        // Query the chain (retrying until the node is reachable): its name keys the cache file, and
        // its Sapling activation height is the default place to start ingesting from. Both backends
        // need the RPC reachable — readstate keeps it for tx submission and the mempool.
        let chain_info = connect_with_retry(&rpc_client).await;

        let node: Arc<dyn NodeRpc> = match &config.backend {
            config::BackendConfig::Rpc => Arc::new(rpc_client),
            #[cfg(feature = "readstate")]
            config::BackendConfig::Readstate {
                state_dir,
                indexer_url,
            } => readstate_node(state_dir.clone(), *indexer_url, rpc_client, &chain_info).await?,
            #[cfg(not(feature = "readstate"))]
            config::BackendConfig::Readstate { .. } => anyhow::bail!(
                "--backend readstate requires a build with the `readstate` cargo feature \
                 (cargo build --release --features readstate)"
            ),
        };
        let start_height = config.start_height.unwrap_or_else(|| {
            chain_info
                .upgrades
                .values()
                .find(|u| u.name.eq_ignore_ascii_case("sapling"))
                .map(|u| u.activationheight)
                .unwrap_or(0)
        });

        validate_chain_name(&chain_info.chain)?;

        // `--nocache` opens the cache in a throwaway temp dir instead of under `--data-dir` and
        // skips the ingestor below, so the cache never gains a single block and every read falls
        // through to the node — for debugging only, since it forfeits all caching benefit. The
        // `TempDir` guard is bound for the rest of `run` so its directory is not removed while the
        // cache is still open.
        let (cache, cache_location, _nocache_tempdir) = if config.nocache {
            tracing::warn!(
                "--nocache: running without the on-disk block cache (debugging only); every \
                 block read falls through to the node"
            );
            let tempdir = tempfile::tempdir().context("creating --nocache temp dir")?;
            let cache_path = tempdir
                .path()
                .join(format!("{}-blocks.redb", chain_info.chain));
            let cache = Arc::new(Cache::open(&cache_path)?);
            let location = cache_path.display().to_string();
            (cache, location, Some(tempdir))
        } else {
            let cache_path = config
                .data_dir
                .join(format!("{}-blocks.redb", chain_info.chain));
            let cache = Arc::new(Cache::open(&cache_path)?);

            // A light open-time check: a pre-existing gap or schema-mismatch is localized and
            // truncated here so the ingestor re-ingests from that height instead of serving
            // corrupt blocks.
            if let Err(error) = cache.validate_light() {
                tracing::warn!(%error, "cache failed open-time validation; locating corruption");
                if let Some(corrupt) = cache.lowest_corrupt_height()? {
                    tracing::warn!(
                        corrupt,
                        "truncating cache from corrupt height; it will re-ingest"
                    );
                    cache.reorg(corrupt.saturating_sub(1))?;
                }
            }

            // Operator cache-reset levers, applied after corruption recovery: --redownload clears
            // the cache (re-ingesting from start_height); --sync-from-height N drops every cached
            // block at or above N. Both then rebuild from the node.
            if config.redownload {
                tracing::warn!("--redownload: clearing the cache; re-ingesting from start_height");
                cache.truncate_from(0)?;
            } else if let Some(height) = config.sync_from_height {
                tracing::warn!(
                    height,
                    "--sync-from-height: dropping cached blocks at or above height"
                );
                cache.truncate_from(height)?;
            }

            let location = cache_path.display().to_string();
            (cache, location, None)
        };

        // Before the floor is resolved, since a successful import raises it, and before the
        // ingestor starts, so it picks up from the imported tip. `--redownload` has already cleared
        // the cache by now, so combining the two means "discard local, re-bootstrap from the peer".
        if let Some(url) = &config.snapshot_url {
            let interrupted =
                bootstrap_from_snapshot(url, &cache, &node, config.ingest.concurrency).await;
            if interrupted {
                tracing::info!("server stopped before it started serving");
                return Ok(());
            }
        }

        let start_height = effective_start_height(start_height, cache.snapshot_base_height()?);

        tracing::info!(
            grpc_bind = %config.grpc_bind,
            node_url = %config.node.url,
            chain = %chain_info.chain,
            start_height,
            cache = %cache_location,
            "lightwalletd-rs starting"
        );

        if config.nocache {
            tracing::info!("--nocache: ingestor not started");
        } else {
            tokio::spawn(ingestor::run(
                node.clone(),
                cache.clone(),
                start_height,
                config.ingest,
            ));
        }

        if let Some(snapshot_config) = config.snapshot {
            if !snapshot_config.bind.ip().is_loopback() {
                tracing::warn!(
                    snapshot_bind = %snapshot_config.bind,
                    "publishing snapshots on a non-loopback address — anyone who can reach it can \
                     download the whole cache, at whatever bandwidth this host will give them"
                );
            }
            tracing::info!(
                snapshot_bind = %snapshot_config.bind,
                "publishing snapshots on /snapshot/manifest and /snapshot/epoch/{{index}}"
            );
            // The manifest only lists epochs whose digests are stored, so the maintenance walk is
            // what makes anything publishable at all: on a cache ingested by an earlier version it
            // backfills from the base upward, and afterwards it computes one epoch per boundary
            // crossing. Both are the same walk, throttled against the ingestor.
            tokio::spawn(snapshot::export::maintain_digests(
                cache.clone(),
                chain_info.chain.clone(),
                snapshot::export::DigestMaintenance::default(),
            ));
            let snapshot_cache = cache.clone();
            let snapshot_chain = chain_info.chain.clone();
            tokio::spawn(async move {
                if let Err(error) =
                    snapshot::serve::serve(snapshot_cache, snapshot_chain, snapshot_config).await
                {
                    tracing::error!(%error, "snapshot server failed");
                }
            });
        }

        // One shared mempool monitor fans the mempool out to all clients, so node load stays
        // independent of the number of connected wallets.
        let mempool = service::mempool_monitor::start(node.clone());
        let streamer = service::Streamer::new(node, cache, chain_info.chain, None)
            .with_mempool_monitor(mempool)
            .with_ping_enabled(config.ping_enable)
            .with_donation_address(config.donation_address.clone());
        server
            .add_service(CompactTxStreamerServer::new(streamer))
            .add_service(reflection_service()?)
            .serve_with_shutdown(config.grpc_bind, shutdown_signal())
            .await?;
    }
    tracing::info!("server stopped");

    Ok(())
}

/// The middleware stack every served transport carries, from outermost to innermost: CORS, so a
/// browser preflight is answered before anything else sees it; Prometheus metrics; and the gRPC-web
/// translation, so the router below only ever sees a plain gRPC request. The two gRPC-web layers
/// are `Either`s rather than a second builder branch, which keeps one concrete server type whether
/// the transport is on or off.
type TransportLayers = Stack<
    Either<GrpcWebLayer, NoLayer>,
    Stack<tonic_prometheus_layer::MetricsLayer, Stack<Either<CorsLayer, NoLayer>, NoLayer>>,
>;

/// How long a browser may cache a gRPC-web preflight. One round trip per method per browser session
/// is pure overhead on a wallet that calls many methods, and the policy this server answers with is
/// static for the lifetime of the process.
const GRPC_WEB_PREFLIGHT_MAX_AGE: Duration = Duration::from_secs(3600);

/// Build the gRPC server with the limits (ADR 0013) and middleware both serve paths use, optionally
/// carrying the gRPC-web transport (ADR 0026). `pub` so the in-process test harness
/// (`tests/common/mod.rs`) exercises the very same stack a deployment gets, rather than a
/// look-alike assembled in the test.
///
/// Enabling gRPC-web also enables HTTP/1.1, which a browser needs on a plaintext port; over TLS it
/// is redundant, since ALPN settles on HTTP/2 there and gRPC-web rides that just as well.
pub fn server_builder(
    limits: &config::ServerLimits,
    grpc_web: Option<&config::GrpcWebOrigins>,
) -> Server<TransportLayers> {
    Server::builder()
        .concurrency_limit_per_connection(limits.max_concurrent_streams as usize)
        .max_concurrent_streams(Some(limits.max_concurrent_streams))
        .tcp_keepalive(Some(limits.keepalive_interval))
        .http2_keepalive_interval(Some(limits.keepalive_interval))
        .http2_keepalive_timeout(Some(limits.keepalive_timeout))
        .accept_http1(grpc_web.is_some())
        .layer(option_layer(grpc_web.map(cors_layer)))
        .layer(tonic_prometheus_layer::MetricsLayer::new())
        .layer(option_layer(grpc_web.map(|_| GrpcWebLayer::new())))
}

/// The CORS policy the gRPC-web transport answers with.
///
/// `grpc-status` and `grpc-message` are exposed because gRPC carries the call's outcome there
/// whenever the server answers with trailers-only (every early rejection: `Unimplemented`,
/// `InvalidArgument`, a failed deadline). A browser hands JavaScript only the headers a server
/// exposed, so without this the call fails with its reason stripped off, and nothing in the symptom
/// mentions CORS.
fn cors_layer(origins: &config::GrpcWebOrigins) -> CorsLayer {
    let allow_origin = match origins {
        config::GrpcWebOrigins::Any => AllowOrigin::any(),
        config::GrpcWebOrigins::Only(allowed) => {
            // Compared as bytes against the header the browser sent, which is exactly the form
            // `config::validate_origin` already accepted: no parsing here that could fail.
            let allowed = allowed.clone();
            AllowOrigin::predicate(move |origin, _parts| {
                allowed
                    .iter()
                    .any(|candidate| candidate.as_bytes() == origin.as_bytes())
            })
        }
    };
    CorsLayer::new()
        .allow_origin(allow_origin)
        .allow_methods([Method::POST])
        .allow_headers([
            header::CONTENT_TYPE,
            header::ACCEPT,
            // Unused by this server, allowed so a deployment fronted by token auth is not left
            // failing its preflights with no way to widen the policy.
            header::AUTHORIZATION,
            HeaderName::from_static("x-grpc-web"),
            HeaderName::from_static("x-user-agent"),
            HeaderName::from_static("grpc-timeout"),
            HeaderName::from_static("grpc-encoding"),
            HeaderName::from_static("grpc-accept-encoding"),
        ])
        .expose_headers([
            HeaderName::from_static("grpc-status"),
            HeaderName::from_static("grpc-message"),
            HeaderName::from_static("grpc-status-details-bin"),
            HeaderName::from_static("grpc-encoding"),
            HeaderName::from_static("grpc-accept-encoding"),
        ])
        .max_age(GRPC_WEB_PREFLIGHT_MAX_AGE)
}

/// Build the gRPC Server Reflection (v1) service, advertising every method in
/// `service.proto`/`darkside.proto` from the descriptor set `build.rs` emits at compile time.
/// Registered unconditionally (both live and darkside modes) so `grpcurl`/`grpcui`-style tools can
/// discover and describe the API on a running server without a local `.proto` checkout. `pub` so
/// the in-process test harness (`tests/common/mod.rs`) can register the same service.
pub fn reflection_service() -> anyhow::Result<
    tonic_reflection::server::v1::ServerReflectionServer<
        impl tonic_reflection::server::v1::ServerReflection,
    >,
> {
    Ok(tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .build_v1()?)
}

/// Build the `readstate` backend (ADR 0023): open a read-only secondary instance of the zebrad
/// state and keep it at the true chain tip with `TrustedChainSync` over the indexer gRPC. The
/// JSON-RPC client is kept inside the backend for the node-only surfaces (submission, mempool,
/// `getinfo`). Fails fast — with a pointer back to `--backend rpc` — when the state directory is
/// missing or written by an incompatible zebra version.
#[cfg(feature = "readstate")]
async fn readstate_node(
    state_dir: Option<std::path::PathBuf>,
    indexer: std::net::SocketAddr,
    rpc: node::NodeClient,
    chain_info: &GetBlockchainInfo,
) -> anyhow::Result<Arc<dyn NodeRpc>> {
    use zebra_chain::parameters::Network;

    let network = match chain_info.chain.as_str() {
        "main" => Network::Mainnet,
        "test" => Network::new_default_testnet(),
        other => anyhow::bail!("readstate backend does not support chain {other:?}"),
    };
    let mut state_config = zebra_state::Config::default();
    if let Some(dir) = state_dir {
        state_config.cache_dir = dir;
    }

    let (read_state, latest_tip, _chain_tip_change, sync_task) =
        zebra_rpc::sync::init_read_state_with_syncer(state_config.clone(), &network, indexer)
            .await
            .map_err(|error| anyhow::anyhow!("read state init task failed: {error}"))?
            .map_err(|error| {
                anyhow::anyhow!(
                    "opening the zebra read state at {} failed: {error}. The state must belong \
                     to a running zebrad of a compatible version (state format v28 <-> zebra 6.x) \
                     on this host; otherwise use --backend rpc",
                    state_config.cache_dir.display()
                )
            })?;
    // `TrustedChainSync` retries a lost indexer connection internally (its sync loop re-subscribes
    // forever), so the task only completes if it panics or the runtime shuts down. Supervise it
    // anyway: if it ever does die, the tip would freeze while the server keeps serving increasingly
    // stale data — that must be an `error` in the logs, not silence.
    tokio::spawn(async move {
        let result = sync_task.await;
        tracing::error!(
            ?result,
            "zebra state sync task exited; the readstate tip will no longer advance — restart the \
             server (or switch to --backend rpc)"
        );
    });
    tracing::info!(
        state_dir = %state_config.cache_dir.display(),
        indexer = %indexer,
        "readstate backend: serving reads from the zebra state in-process"
    );
    Ok(Arc::new(node::readstate::ZebraStateNode::new(
        read_state, latest_tip, rpc, network,
    )))
}

/// After this many consecutive failures we keep retrying but log at `error!`, so a genuinely
/// misconfigured node (bad URL, wrong credentials) is visible instead of an endless silent `warn!`.
const ESCALATE_AFTER: u32 = 10;

/// Query `getblockchaininfo`, retrying indefinitely with capped exponential backoff until the node
/// answers. The server must not exit just because the node is slow to come up; after
/// [`ESCALATE_AFTER`] consecutive failures the log level rises to `error!` so a node that will never
/// answer under the current config stays visible to monitoring.
async fn connect_with_retry(node: &dyn NodeRpc) -> GetBlockchainInfo {
    let cap = Duration::from_secs(30);
    let mut delay = Duration::from_secs(1);
    let mut attempt = 0u32;
    loop {
        match node.get_blockchain_info().await {
            Ok(info) => {
                if attempt > 0 {
                    tracing::info!(attempt, "node reachable; continuing startup");
                }
                return info;
            }
            Err(error) => {
                attempt += 1;
                if attempt >= ESCALATE_AFTER {
                    tracing::error!(
                        %error,
                        attempt,
                        backoff_secs = delay.as_secs(),
                        "node still unreachable; check node URL/credentials"
                    );
                } else {
                    tracing::warn!(
                        %error,
                        attempt,
                        backoff_secs = delay.as_secs(),
                        "node not reachable; retrying"
                    );
                }
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(cap);
            }
        }
    }
}

/// Import a peer's snapshot into the cache, verifying every block against our own node. Returns
/// whether the process was asked to stop before the import could finish.
///
/// Never fatal: a peer that is unreachable, out of date or dishonest degrades to today's behaviour,
/// where the ingestor fills the cache from the node the slow way.
async fn bootstrap_from_snapshot(
    url: &str,
    cache: &Cache,
    node: &Arc<dyn NodeRpc>,
    concurrency: usize,
) -> bool {
    use snapshot::import::{HttpEpochSource, ImportConfig, import};

    tracing::info!(url, "bootstrapping the cache from a snapshot peer");
    let started = std::time::Instant::now();
    let source = match HttpEpochSource::new(url) {
        Ok(source) => source,
        Err(error) => {
            tracing::error!(%error, url, "snapshot peer unusable; ingesting from the node instead");
            return false;
        }
    };
    let import_config = ImportConfig { concurrency };
    let import = import(&source, cache, node, &import_config);
    tokio::select! {
        result = import => match result {
            Ok(Some(height)) => tracing::info!(
                height,
                elapsed_secs = started.elapsed().as_secs(),
                "snapshot bootstrap finished"
            ),
            Ok(None) => tracing::info!("snapshot bootstrap had nothing to import"),
            Err(error) => {
                tracing::error!(%error, "snapshot bootstrap failed; ingesting from the node instead")
            }
        },
        // An import can run for hours before the gRPC listener binds, which is long enough for an
        // orchestrator's stop timeout to elapse inside it. Dropping the in-flight epoch costs
        // nothing: epochs commit one at a time, so a later run resumes from the last one that
        // landed.
        () = shutdown_signal() => {
            tracing::info!(
                cached_tip = ?cache.latest_height().ok().flatten(),
                "stop requested during the snapshot bootstrap; keeping the epochs already imported"
            );
            return true;
        }
    }
    false
}

/// Raise the ingest floor to the base height of an imported snapshot.
///
/// A bootstrapped cache cannot be rebuilt from below the height its snapshot was based at. Leaving
/// the floor at the configured start height would mean a reorg deep enough to reach it empties the
/// cache (see `ingestor::reorg_to_floor`) and the server silently re-ingests from Sapling
/// activation, discarding the whole import. Clearing the cache with `--redownload` also clears the
/// recorded base, so a deliberate wipe returns the instance to a plain cold start.
fn effective_start_height(configured: u64, snapshot_base: Option<u64>) -> u64 {
    match snapshot_base {
        Some(base) if base > configured => {
            tracing::info!(
                configured,
                snapshot_base = base,
                "raising the ingest floor to the imported snapshot's base height"
            );
            base
        }
        _ => configured,
    }
}

/// Validate a node-supplied chain name before it is used to build the cache file name.
///
/// The node is trusted-local (see ADR 0001), but `getblockchaininfo`'s `chain` field still flows
/// unsanitized into `data_dir.join(format!("{chain}-blocks.redb"))`; a name containing a path
/// separator or `..` could otherwise redirect the cache file outside `data_dir`. Real chain values
/// (`main`, `test`, `regtest`) are all plain alphanumerics, so a conservative charset is safe.
fn validate_chain_name(chain: &str) -> anyhow::Result<()> {
    if chain.is_empty()
        || !chain
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        anyhow::bail!("node returned an invalid chain name: {chain:?}");
    }
    Ok(())
}

/// Wire the darkside mock chain: the shared state, a `DarksideNode` over it, the block cache at
/// `cache_path`, the shutdown notifier, the `DarksideService` control plane, and a `Streamer` bound
/// to the same state. Returned as `(streamer, control service, shared state, shutdown)` so `run`'s
/// darkside branch and the in-process test harness wire identical components to their transport.
pub fn darkside_components(
    cache_path: &Path,
) -> anyhow::Result<(
    service::Streamer,
    darkside::DarksideService,
    darkside::DarksideHandle,
    Arc<tokio::sync::Notify>,
)> {
    let state: darkside::DarksideHandle =
        Arc::new(tokio::sync::Mutex::new(darkside::DarksideState::new()));
    let node: Arc<dyn NodeRpc> = Arc::new(darkside::DarksideNode::new(state.clone()));
    let cache = Arc::new(Cache::open(cache_path)?);
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let darkside_service = darkside::DarksideService::new(state.clone(), shutdown.clone());
    let streamer = service::Streamer::new(node, cache, "main".to_string(), Some(state.clone()));
    Ok((streamer, darkside_service, state, shutdown))
}

/// Resolve when either an OS signal arrives or the `Stop` gRPC fires the shutdown notifier.
async fn darkside_shutdown(notify: Arc<tokio::sync::Notify>) {
    tokio::select! {
        _ = shutdown_signal() => {},
        _ = notify.notified() => tracing::info!("stop requested, draining connections"),
    }
}

/// Resolve when the process receives `SIGINT` (Ctrl-C) or `SIGTERM` (e.g. `docker stop`), so the gRPC
/// server can stop accepting connections and drain the in-flight ones before exiting.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => {
                tracing::error!(%error, "failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("shutdown signal received, draining connections");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use crate::testutil::FakeNode;

    fn fake(failures: u32) -> FakeNode {
        FakeNode {
            blockchain_info: Some(
                serde_json::from_value(serde_json::json!({
                    "chain": "main",
                    "blocks": 4242,
                    "bestblockhash": "00",
                    "consensus": { "chaintip": "00000000" },
                }))
                .unwrap(),
            ),
            blockchain_info_failures: Mutex::new(failures),
            ..Default::default()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn connect_with_retry_succeeds_after_failures_below_escalation() {
        let info = connect_with_retry(&fake(ESCALATE_AFTER - 1)).await;
        assert_eq!(info.blocks, 4242);
    }

    #[tokio::test(start_paused = true)]
    async fn connect_with_retry_keeps_retrying_past_the_escalation_threshold() {
        let info = connect_with_retry(&fake(ESCALATE_AFTER + 3)).await;
        assert_eq!(info.blocks, 4242);
    }

    #[tokio::test]
    async fn an_unreachable_snapshot_peer_leaves_the_cache_alone_and_does_not_abort_startup() {
        let (_dir, cache) = crate::testutil::temp_cache();
        let node: Arc<dyn NodeRpc> = Arc::new(fake(0));

        let interrupted = bootstrap_from_snapshot("http://127.0.0.1:1", &cache, &node, 4).await;

        assert_eq!((interrupted, cache.latest_height().unwrap()), (false, None));
    }

    #[test]
    fn an_imported_snapshot_raises_the_ingest_floor_above_the_configured_start() {
        assert_eq!(effective_start_height(419_200, Some(3_000_000)), 3_000_000);
    }

    #[test]
    fn a_configured_start_above_the_snapshot_base_is_kept() {
        assert_eq!(
            effective_start_height(3_100_000, Some(3_000_000)),
            3_100_000
        );
    }

    #[test]
    fn a_cache_that_was_never_bootstrapped_keeps_the_configured_start() {
        assert_eq!(effective_start_height(419_200, None), 419_200);
    }

    #[test]
    fn validate_chain_name_accepts_real_chain_values() {
        assert!(validate_chain_name("main").is_ok());
        assert!(validate_chain_name("test").is_ok());
        assert!(validate_chain_name("regtest").is_ok());
    }

    #[test]
    fn validate_chain_name_rejects_a_path_traversal_attempt() {
        assert!(validate_chain_name("../evil").is_err());
    }

    #[test]
    fn validate_chain_name_rejects_a_path_separator() {
        assert!(validate_chain_name("a/b").is_err());
    }

    #[test]
    fn validate_chain_name_rejects_an_empty_name() {
        assert!(validate_chain_name("").is_err());
    }
}
