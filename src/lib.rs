//! lightwalletd-rs: a Rust lightwalletd for Zcash, usable as a library.

use std::path::Path;
use std::sync::Arc;

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
use node::NodeRpc;
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

    let mut server = Server::builder().layer(tonic_prometheus_layer::MetricsLayer::new());
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

        server
            .add_service(CompactTxStreamerServer::new(streamer))
            .add_service(DarksideStreamerServer::new(darkside_service))
            .serve_with_shutdown(config.grpc_bind, darkside_shutdown(shutdown))
            .await?;
    } else {
        // Real node: query the chain, open the cache, spawn the ingestor, serve `CompactTxStreamer`.
        let node: Arc<dyn NodeRpc> = Arc::new(node::NodeClient::new(&config.node));

        // Query the chain once: its name keys the cache file, and its Sapling activation height is
        // the default place to start ingesting from.
        let chain_info = node.get_blockchain_info().await?;
        let start_height = config.start_height.unwrap_or_else(|| {
            chain_info
                .upgrades
                .values()
                .find(|u| u.name.eq_ignore_ascii_case("sapling"))
                .map(|u| u.activationheight)
                .unwrap_or(0)
        });

        let cache_path = config
            .data_dir
            .join(format!("{}-blocks.redb", chain_info.chain));
        let cache = Arc::new(Cache::open(&cache_path)?);

        tracing::info!(
            grpc_bind = %config.grpc_bind,
            node_url = %config.node.url,
            chain = %chain_info.chain,
            start_height,
            cache = %cache_path.display(),
            "lightwalletd-rs starting"
        );

        tokio::spawn(ingestor::run(node.clone(), cache.clone(), start_height));

        let streamer = service::Streamer::new(node, cache, chain_info.chain, None);
        server
            .add_service(CompactTxStreamerServer::new(streamer))
            .serve_with_shutdown(config.grpc_bind, shutdown_signal())
            .await?;
    }
    tracing::info!("server stopped");

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
