//! Dialer side of the spike: exposes a local TCP port and, for each inbound connection, opens a
//! mixnet stream to the service provider and copies bytes both ways.
//!
//! `--reply-surbs` is the variable under test. Every reply packet the far side sends consumes one
//! reply block, and a `GetBlockRange` needs hundreds to thousands of them against a default of 10,
//! so what this flag really sweeps is how often the replenishment round trip fires.

use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use nym_sdk::mixnet::{MixnetClient, Recipient};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

/// What the SDK attaches when `--reply-surbs` is left unset. Mirrored here only so the startup log
/// reports the value actually in effect.
const SDK_DEFAULT_REPLY_SURBS: u32 = 10;

#[derive(Parser)]
#[command(about = "Reach a mixnet service provider from a local TCP port")]
struct Arguments {
    /// Nym address the service provider printed on startup.
    #[arg(long, env = "DIAL_SP")]
    service_provider: String,

    /// Local address the measurement tools connect to.
    #[arg(long, env = "DIAL_BIND", default_value = "0.0.0.0:9068")]
    bind: String,

    /// Reply blocks attached to each outbound message; unset leaves the SDK default.
    #[arg(long, env = "DIAL_REPLY_SURBS")]
    reply_surbs: Option<u32>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let arguments = Arguments::parse();

    let recipient: Recipient = arguments
        .service_provider
        .parse()
        .context("parsing the service provider address")?;

    let client = MixnetClient::connect_new()
        .await
        .context("connecting to the mixnet")?;
    let client = Arc::new(Mutex::new(client));

    let listener = TcpListener::bind(&arguments.bind)
        .await
        .with_context(|| format!("binding {}", arguments.bind))?;
    tracing::info!(
        bind = %arguments.bind,
        reply_surbs = arguments.reply_surbs.unwrap_or(SDK_DEFAULT_REPLY_SURBS),
        "dialing the mixnet for each local connection"
    );

    loop {
        let (mut local, peer) = listener.accept().await.context("accepting locally")?;
        let client = Arc::clone(&client);
        let reply_surbs = arguments.reply_surbs;
        tokio::spawn(async move {
            // Held only while the stream is being opened, so concurrent connections serialise on
            // the open and then run in parallel.
            let opened = client.lock().await.open_stream(recipient, reply_surbs).await;
            match opened {
                Ok(mut stream) => match tokio::io::copy_bidirectional(&mut local, &mut stream).await
                {
                    Ok((sent, received)) => {
                        tracing::info!(%peer, sent, received, "stream finished")
                    }
                    Err(error) => tracing::warn!(%peer, %error, "stream aborted"),
                },
                Err(error) => tracing::error!(%peer, %error, "opening the mixnet stream failed"),
            }
        });
    }
}
