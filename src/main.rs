use clap::Parser;
use tonic::transport::Server;
use tracing_subscriber::EnvFilter;

mod compact;
mod config;
mod node;
mod proto;
mod service;

use config::Cli;
use proto::compact_tx_streamer_server::CompactTxStreamerServer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = Cli::parse().resolve()?;
    tracing::info!(
        grpc_bind = %config.grpc_bind,
        node_url = %config.node.url,
        "lightwalletd-rs starting"
    );

    let node = node::NodeClient::new(&config.node);
    let streamer = service::Streamer::new(node);

    Server::builder()
        .add_service(CompactTxStreamerServer::new(streamer))
        .serve(config.grpc_bind)
        .await?;

    Ok(())
}
