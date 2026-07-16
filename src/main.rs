use std::path::Path;

use anyhow::Context;
use clap::Parser;
use lightwalletd_rs::config::Cli;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    init_logging(&cli.log_level, cli.log_file.as_deref())?;

    let config = cli.resolve()?;
    lightwalletd_rs::run(config).await
}

/// Initialize the global `tracing` subscriber from `--log-level`/`--log-file`.
///
/// An explicit `RUST_LOG` environment variable always wins over `--log-level`, matching the usual
/// `tracing-subscriber` convention (and documented on the flag itself). With `--log-file` unset,
/// output is the existing human-readable text on stderr; with it set, output is JSON lines
/// appended to that file — matching the Go reference, which switches its logrus output to JSON
/// when `--log-file` is given.
fn init_logging(log_level: &str, log_file: Option<&Path>) -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(log_level))
        .with_context(|| format!("invalid --log-level {log_level:?}"))?;

    match log_file {
        Some(path) => {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .with_context(|| format!("opening --log-file at {}", path.display()))?;
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .json()
                .with_writer(file)
                .init();
        }
        None => {
            tracing_subscriber::fmt().with_env_filter(filter).init();
        }
    }
    Ok(())
}
