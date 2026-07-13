//! lightwalletd-rs: a Rust lightwalletd for Zcash, usable as a library.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tonic::transport::{Identity, Server, ServerTlsConfig};

pub mod cache;
pub mod config;
pub mod darkside;
pub mod node;
pub mod proto;
pub mod service;

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

    let mut server = Server::builder()
        .concurrency_limit_per_connection(config.limits.max_concurrent_streams as usize)
        .max_concurrent_streams(Some(config.limits.max_concurrent_streams))
        .tcp_keepalive(Some(config.limits.keepalive_interval))
        .http2_keepalive_interval(Some(config.limits.keepalive_interval))
        .http2_keepalive_timeout(Some(config.limits.keepalive_timeout))
        .layer(tonic_prometheus_layer::MetricsLayer::new());
    match &config.tls {
        config::TlsConfig::Enabled { cert, key } => {
            let identity = Identity::from_pem(std::fs::read(cert)?, std::fs::read(key)?);
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

        server
            .add_service(CompactTxStreamerServer::new(streamer))
            .add_service(DarksideStreamerServer::new(darkside_service))
            .serve_with_shutdown(config.grpc_bind, darkside_shutdown(shutdown))
            .await?;
    } else {
        // Real node: query the chain, open the cache, spawn the ingestor, serve `CompactTxStreamer`.
        let node: Arc<dyn NodeRpc> = Arc::new(node::NodeClient::new(&config.node)?);

        // Query the chain (retrying until the node is reachable): its name keys the cache file, and
        // its Sapling activation height is the default place to start ingesting from.
        let chain_info = connect_with_retry(node.as_ref()).await;
        let start_height = config.start_height.unwrap_or_else(|| {
            chain_info
                .upgrades
                .values()
                .find(|u| u.name.eq_ignore_ascii_case("sapling"))
                .map(|u| u.activationheight)
                .unwrap_or(0)
        });

        validate_chain_name(&chain_info.chain)?;
        let cache_path = config
            .data_dir
            .join(format!("{}-blocks.redb", chain_info.chain));
        let cache = Arc::new(Cache::open(&cache_path)?);

        // A light open-time check: a pre-existing gap or schema-mismatch is localized and truncated
        // here so the ingestor re-ingests from that height instead of serving corrupt blocks.
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

        // Operator cache-reset levers, applied after corruption recovery: --redownload clears the
        // cache (re-ingesting from start_height); --sync-from-height N drops every cached block at
        // or above N. Both then rebuild from the node.
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

        tracing::info!(
            grpc_bind = %config.grpc_bind,
            node_url = %config.node.url,
            chain = %chain_info.chain,
            start_height,
            cache = %cache_path.display(),
            "lightwalletd-rs starting"
        );

        tokio::spawn(ingestor::run(
            node.clone(),
            cache.clone(),
            start_height,
            config.ingest.clone(),
        ));

        // One shared mempool monitor fans the mempool out to all clients, so node load stays
        // independent of the number of connected wallets.
        let mempool = service::mempool_monitor::start(node.clone());
        let streamer = service::Streamer::new(node, cache, chain_info.chain, None)
            .with_mempool_monitor(mempool)
            .with_ping_enabled(config.ping_enable)
            .with_donation_address(config.donation_address.clone());
        server
            .add_service(CompactTxStreamerServer::new(streamer))
            .serve_with_shutdown(config.grpc_bind, shutdown_signal())
            .await?;
    }
    tracing::info!("server stopped");

    Ok(())
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
