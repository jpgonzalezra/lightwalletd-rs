use std::sync::Arc;

use clap::Parser;
use tonic::transport::Server;
use tracing_subscriber::EnvFilter;

mod cache;
mod compact;
mod config;
mod fetch;
mod ingestor;
mod node;
mod proto;
mod service;

use cache::Cache;
use config::Cli;
use node::NodeRpc;
use proto::compact_tx_streamer_server::CompactTxStreamerServer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = Cli::parse().resolve()?;
    let node: Arc<dyn NodeRpc> = Arc::new(node::NodeClient::new(&config.node));

    // Query the chain once: its name keys the cache file, and its Sapling activation height is the
    // default place to start ingesting from.
    let chain_info = node.get_blockchain_info().await?;
    let start_height = config.start_height.unwrap_or_else(|| {
        chain_info
            .upgrades
            .values()
            .find(|u| u.name.eq_ignore_ascii_case("sapling"))
            .map(|u| u.activationheight)
            .unwrap_or(0)
    });

    std::fs::create_dir_all(&config.data_dir)?;
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

    let streamer = service::Streamer::new(node, cache, chain_info.chain);
    Server::builder()
        .add_service(CompactTxStreamerServer::new(streamer))
        .serve(config.grpc_bind)
        .await?;

    Ok(())
}
