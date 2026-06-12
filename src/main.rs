use clap::Parser;
use tracing_subscriber::EnvFilter;

mod config;
mod proto;

use config::Cli;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let config = Cli::parse().resolve()?;
    tracing::info!(
        grpc_bind = %config.grpc_bind,
        node_url = %config.node.url,
        "lightwalletd-rs starting"
    );
    Ok(())
}
