//! Service provider side of the spike: accepts inbound mixnet streams and pipes each one to an
//! upstream TCP endpoint, which is the ordinary gRPC listener.
//!
//! Prints its own Nym address on startup; that is what the dialer needs.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use nym_sdk::mixnet::{MixnetClient, MixnetClientBuilder, StoragePaths};
use tokio::net::TcpStream;

#[derive(Parser)]
#[command(about = "Serve an upstream TCP endpoint over the mixnet")]
struct Arguments {
    /// Upstream every accepted stream is piped to.
    #[arg(long, env = "SP_UPSTREAM", default_value = "127.0.0.1:9067")]
    upstream: String,

    /// Directory holding the client identity. Without it the address is ephemeral and rotates on
    /// every restart, which makes the service provider unfindable.
    #[arg(long, env = "SP_STATE_DIR")]
    state_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let arguments = Arguments::parse();

    let mut client = match &arguments.state_dir {
        Some(directory) => {
            let paths = StoragePaths::new_from_dir(directory)
                .context("preparing the client state directory")?;
            MixnetClientBuilder::new_with_default_storage(paths)
                .await
                .context("building a client with persistent storage")?
                .build()
                .context("assembling the client")?
                .connect_to_mixnet()
                .await
                .context("connecting to the mixnet")?
        }
        None => MixnetClient::connect_new()
            .await
            .context("connecting an ephemeral client to the mixnet")?,
    };

    // Printed rather than logged so it survives whatever the log filter is set to.
    println!("NYM_ADDRESS={}", client.nym_address());

    let mut listener = client.listener().context("taking the stream listener")?;
    tracing::info!(upstream = %arguments.upstream, "accepting mixnet streams");

    while let Some(mut stream) = listener.accept().await {
        let upstream = arguments.upstream.clone();
        tokio::spawn(async move {
            match TcpStream::connect(&upstream).await {
                Ok(mut connection) => {
                    match tokio::io::copy_bidirectional(&mut stream, &mut connection).await {
                        Ok((from_client, from_upstream)) => tracing::info!(
                            from_client,
                            from_upstream,
                            "stream finished"
                        ),
                        Err(error) => tracing::warn!(%error, "stream aborted"),
                    }
                }
                Err(error) => tracing::error!(%error, %upstream, "upstream unreachable"),
            }
        });
    }

    tracing::info!("listener closed");
    Ok(())
}
