//! Runtime configuration: CLI flags plus an optional `zcash.conf` file.
//!
//! Flags always take precedence over values read from `zcash.conf`.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Parser;

/// Command-line arguments.
#[derive(Debug, Parser)]
#[command(name = "lightwalletd-rs", about = "A Rust lightwalletd for Zcash")]
pub struct Cli {
    /// Address the gRPC server listens on.
    #[arg(long, default_value = "127.0.0.1:9067")]
    pub grpc_bind: SocketAddr,

    /// Full JSON-RPC URL of the zebrad node (overrides `--rpc-host`/`--rpc-port`).
    #[arg(long)]
    pub rpc_url: Option<String>,

    /// zebrad RPC host, used when `--rpc-url` is not given.
    #[arg(long, default_value = "127.0.0.1")]
    pub rpc_host: String,

    /// zebrad RPC port, used when `--rpc-url` is not given.
    #[arg(long, default_value_t = 8232)]
    pub rpc_port: u16,

    /// RPC username (overrides the value from `--zcash-conf`).
    #[arg(long)]
    pub rpc_user: Option<String>,

    /// RPC password (overrides the value from `--zcash-conf`).
    #[arg(long)]
    pub rpc_password: Option<String>,

    /// Path to a `zcash.conf` to read `rpcuser`/`rpcpassword`/`rpcbind`/`rpcport` from.
    #[arg(long)]
    pub zcash_conf: Option<PathBuf>,
}

/// Resolved runtime configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Address the gRPC server listens on.
    pub grpc_bind: SocketAddr,
    /// How to reach the backend node.
    pub node: NodeConfig,
}

/// How to reach the zebrad JSON-RPC endpoint.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// Base URL, e.g. `http://127.0.0.1:8232`.
    pub url: String,
    /// HTTP Basic auth username.
    pub user: String,
    /// HTTP Basic auth password.
    pub password: String,
}

impl Cli {
    /// Resolve CLI flags (and an optional `zcash.conf`) into a [`Config`].
    pub fn resolve(self) -> Result<Config> {
        let conf = match &self.zcash_conf {
            Some(path) => parse_zcash_conf(path)?,
            None => ZcashConf::default(),
        };

        let user = self.rpc_user.or(conf.rpcuser).unwrap_or_default();
        let password = self.rpc_password.or(conf.rpcpassword).unwrap_or_default();

        let url = match self.rpc_url {
            Some(url) => url,
            None => {
                let host = conf.rpcbind.unwrap_or(self.rpc_host);
                let port = conf.rpcport.unwrap_or(self.rpc_port);
                format!("http://{host}:{port}")
            }
        };

        Ok(Config {
            grpc_bind: self.grpc_bind,
            node: NodeConfig {
                url,
                user,
                password,
            },
        })
    }
}

/// The subset of `zcash.conf` fields we read.
#[derive(Debug, Default, PartialEq, Eq)]
struct ZcashConf {
    rpcuser: Option<String>,
    rpcpassword: Option<String>,
    rpcbind: Option<String>,
    rpcport: Option<u16>,
}

/// Parse the `key=value` lines of a `zcash.conf`, ignoring comments and blank lines.
fn parse_zcash_conf(path: &Path) -> Result<ZcashConf> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading zcash.conf at {}", path.display()))?;
    let mut conf = ZcashConf::default();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let value = value.trim().to_string();
        match key.trim() {
            "rpcuser" => conf.rpcuser = Some(value),
            "rpcpassword" => conf.rpcpassword = Some(value),
            "rpcbind" => conf.rpcbind = Some(value),
            "rpcport" => conf.rpcport = value.parse().ok(),
            _ => {}
        }
    }
    Ok(conf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn parse_zcash_conf_reads_known_keys_and_skips_comments() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(
            f,
            "# a comment\n\nrpcuser=alice\nrpcpassword = s3cret\nrpcport=18232\nunknown=ignored\n"
        )
        .unwrap();

        let conf = parse_zcash_conf(f.path()).unwrap();

        assert_eq!(
            conf,
            ZcashConf {
                rpcuser: Some("alice".to_string()),
                rpcpassword: Some("s3cret".to_string()),
                rpcbind: None,
                rpcport: Some(18232),
            }
        );
    }
}
