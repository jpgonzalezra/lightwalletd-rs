//! Runtime configuration: CLI flags plus an optional `zcash.conf` file.
//!
//! Flags always take precedence over values read from `zcash.conf`.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use zcash_address::unified::Encoding;

/// Default per-connection in-flight request / HTTP-2 stream cap.
pub const DEFAULT_MAX_CONCURRENT_STREAMS: u32 = 256;
/// Default keepalive ping interval, in seconds, on an idle connection.
pub const DEFAULT_KEEPALIVE_INTERVAL_SECS: u64 = 60;
/// Default time, in seconds, to wait for a keepalive ack before dropping a connection.
pub const DEFAULT_KEEPALIVE_TIMEOUT_SECS: u64 = 20;

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

    /// Directory for the on-disk block cache.
    #[arg(long, default_value = "./lightwalletd-rs-data")]
    pub data_dir: PathBuf,

    /// Height to start ingesting from when the cache is empty (defaults to Sapling activation).
    #[arg(long)]
    pub start_height: Option<u64>,

    /// Drop cached blocks at or above this height at startup, then re-ingest them from the node.
    #[arg(long)]
    pub sync_from_height: Option<u64>,

    /// Clear the whole cache at startup and re-ingest from scratch. Takes precedence over
    /// `--sync-from-height`.
    #[arg(long)]
    pub redownload: bool,

    /// Path to a PEM TLS certificate (required unless `--no-tls-very-insecure`).
    #[arg(long)]
    pub tls_cert: Option<PathBuf>,

    /// Path to the PEM TLS private key (required unless `--no-tls-very-insecure`).
    #[arg(long)]
    pub tls_key: Option<PathBuf>,

    /// Run the gRPC server without TLS (plaintext). Insecure — development only.
    #[arg(long = "no-tls-very-insecure")]
    pub no_tls: bool,

    /// Address to serve Prometheus metrics on (`/metrics`); metrics are disabled if unset.
    #[arg(long)]
    pub metrics_bind: Option<SocketAddr>,

    /// Run as a darkside mock server (no real node) for deterministic wallet tests. Insecure —
    /// testing only; never deploy in production.
    #[arg(long = "darkside-very-insecure")]
    pub darkside: bool,

    /// Enable the `Ping` gRPC (testing/benchmark only). Off by default; insecure — it lets a client
    /// hold server resources, so never enable in production.
    #[arg(long = "ping-very-insecure")]
    pub ping_enable: bool,

    /// Zcash unified address to advertise for donations to this server's operator.
    #[arg(long)]
    pub donation_address: Option<String>,

    /// Max concurrent in-flight requests / HTTP-2 streams a single connection may open.
    #[arg(long, default_value_t = DEFAULT_MAX_CONCURRENT_STREAMS)]
    pub max_concurrent_streams: u32,

    /// Keepalive ping interval (seconds) on an idle connection; an unanswered peer is dropped.
    #[arg(long, default_value_t = DEFAULT_KEEPALIVE_INTERVAL_SECS)]
    pub keepalive_interval_secs: u64,

    /// Time (seconds) to wait for a keepalive ack before dropping the connection.
    #[arg(long, default_value_t = DEFAULT_KEEPALIVE_TIMEOUT_SECS)]
    pub keepalive_timeout_secs: u64,
}

/// Resolved runtime configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Address the gRPC server listens on.
    pub grpc_bind: SocketAddr,
    /// How to reach the backend node.
    pub node: NodeConfig,
    /// Directory for the on-disk block cache.
    pub data_dir: PathBuf,
    /// Height to start ingesting from when the cache is empty.
    pub start_height: Option<u64>,
    /// Drop cached blocks at or above this height at startup, then re-ingest them.
    pub sync_from_height: Option<u64>,
    /// Clear the whole cache at startup and re-ingest from scratch.
    pub redownload: bool,
    /// Whether the gRPC server runs over TLS, and with which certificate.
    pub tls: TlsConfig,
    /// Address to serve Prometheus metrics on, if enabled.
    pub metrics_bind: Option<SocketAddr>,
    /// Whether to run as a darkside mock server instead of proxying a real node.
    pub darkside: bool,
    /// Whether the `Ping` gRPC is enabled (testing/benchmark only); off by default for hardening.
    pub ping_enable: bool,
    /// Donation unified address advertised in `GetLightdInfo`, if configured.
    pub donation_address: Option<String>,
    /// gRPC server resource limits / hardening.
    pub limits: ServerLimits,
}

/// gRPC server resource limits applied to the shared tonic `Server` builder.
#[derive(Debug, Clone)]
pub struct ServerLimits {
    /// Per-connection in-flight request / HTTP-2 stream cap.
    pub max_concurrent_streams: u32,
    /// Keepalive ping interval on an idle connection.
    pub keepalive_interval: Duration,
    /// Time to wait for a keepalive ack before dropping a connection.
    pub keepalive_timeout: Duration,
}

/// How the gRPC server presents itself on the wire.
#[derive(Debug, Clone)]
pub enum TlsConfig {
    /// Serve over TLS with the given PEM certificate and private-key file paths.
    Enabled {
        /// Path to the PEM certificate.
        cert: PathBuf,
        /// Path to the PEM private key.
        key: PathBuf,
    },
    /// Serve plaintext (no TLS) — insecure, development only.
    Disabled,
}

/// How to reach the zebrad JSON-RPC endpoint.
#[derive(Clone)]
pub struct NodeConfig {
    /// Base URL, e.g. `http://127.0.0.1:8232`.
    pub url: String,
    /// HTTP Basic auth username.
    pub user: String,
    /// HTTP Basic auth password.
    pub password: String,
}

impl std::fmt::Debug for NodeConfig {
    /// Hand-written so a stray `{:?}` on the config can never leak the node credential into a log.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeConfig")
            .field("url", &self.url)
            .field("user", &self.user)
            .field("password", &"***")
            .finish()
    }
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

        let tls = if self.no_tls {
            TlsConfig::Disabled
        } else {
            let message = "TLS is required: pass --tls-cert and --tls-key, or --no-tls-very-insecure for plaintext";
            TlsConfig::Enabled {
                cert: self.tls_cert.context(message)?,
                key: self.tls_key.context(message)?,
            }
        };

        if let Some(address) = &self.donation_address {
            // Decode (not just prefix-check) the unified address so a truncated or mistyped one,
            // which still starts with `u`, is rejected at startup instead of being advertised.
            zcash_address::unified::Address::decode(address).map_err(|error| {
                anyhow::anyhow!("donation-address is not a valid Zcash unified address: {error}")
            })?;
        }

        if self.max_concurrent_streams == 0 {
            anyhow::bail!("--max-concurrent-streams must be greater than 0");
        }
        if self.keepalive_interval_secs == 0 || self.keepalive_timeout_secs == 0 {
            anyhow::bail!(
                "--keepalive-interval-secs and --keepalive-timeout-secs must be greater than 0"
            );
        }

        Ok(Config {
            grpc_bind: self.grpc_bind,
            node: NodeConfig {
                url,
                user,
                password,
            },
            data_dir: self.data_dir,
            start_height: self.start_height,
            sync_from_height: self.sync_from_height,
            redownload: self.redownload,
            tls,
            metrics_bind: self.metrics_bind,
            darkside: self.darkside,
            ping_enable: self.ping_enable,
            donation_address: self.donation_address,
            limits: ServerLimits {
                max_concurrent_streams: self.max_concurrent_streams,
                keepalive_interval: Duration::from_secs(self.keepalive_interval_secs),
                keepalive_timeout: Duration::from_secs(self.keepalive_timeout_secs),
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

    fn cli_with(
        rpc_user: Option<&str>,
        rpc_password: Option<&str>,
        rpc_url: Option<&str>,
        rpc_host: &str,
        rpc_port: u16,
        zcash_conf: Option<PathBuf>,
    ) -> Cli {
        Cli {
            grpc_bind: "127.0.0.1:9067".parse().unwrap(),
            rpc_url: rpc_url.map(str::to_string),
            rpc_host: rpc_host.to_string(),
            rpc_port,
            rpc_user: rpc_user.map(str::to_string),
            rpc_password: rpc_password.map(str::to_string),
            zcash_conf,
            data_dir: PathBuf::from("./data"),
            start_height: None,
            sync_from_height: None,
            redownload: false,
            tls_cert: None,
            tls_key: None,
            no_tls: true,
            metrics_bind: None,
            darkside: false,
            ping_enable: false,
            donation_address: None,
            max_concurrent_streams: DEFAULT_MAX_CONCURRENT_STREAMS,
            keepalive_interval_secs: DEFAULT_KEEPALIVE_INTERVAL_SECS,
            keepalive_timeout_secs: DEFAULT_KEEPALIVE_TIMEOUT_SECS,
        }
    }

    #[test]
    fn resolve_prefers_explicit_flags_over_zcash_conf() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(f, "rpcuser=fileuser\nrpcpassword=filepass\nrpcport=18232\n").unwrap();

        let config = cli_with(
            Some("flaguser"),
            Some("flagpass"),
            None,
            "127.0.0.1",
            8232,
            Some(f.path().to_path_buf()),
        )
        .resolve()
        .unwrap();

        assert_eq!(config.node.user, "flaguser");
        assert_eq!(config.node.password, "flagpass");
        // No rpcbind in the file, so the host falls back to the flag; the port comes from the file.
        assert_eq!(config.node.url, "http://127.0.0.1:18232");
    }

    #[test]
    fn resolve_builds_url_from_host_and_port_when_rpc_url_absent() {
        let config = cli_with(None, None, None, "192.168.0.5", 8232, None)
            .resolve()
            .unwrap();

        assert_eq!(config.node.url, "http://192.168.0.5:8232");
        assert_eq!(config.node.user, "");
        assert_eq!(config.node.password, "");
    }

    #[test]
    fn resolve_with_no_tls_yields_disabled() {
        let config = cli_with(None, None, Some("http://node"), "127.0.0.1", 8232, None)
            .resolve()
            .unwrap();
        assert!(matches!(config.tls, TlsConfig::Disabled));
    }

    #[test]
    fn resolve_requires_a_cert_when_tls_is_enabled() {
        let mut cli = cli_with(None, None, Some("http://node"), "127.0.0.1", 8232, None);
        cli.no_tls = false;
        assert!(cli.resolve().is_err());
    }

    #[test]
    fn resolve_accepts_and_stores_a_valid_donation_address() {
        let valid_ua = crate::testutil::example_unified_address();
        let mut cli = cli_with(None, None, Some("http://node"), "127.0.0.1", 8232, None);
        cli.donation_address = Some(valid_ua.clone());

        let config = cli.resolve().unwrap();

        assert_eq!(config.donation_address.as_deref(), Some(valid_ua.as_str()));
    }

    #[test]
    fn resolve_rejects_a_non_unified_donation_address() {
        let mut cli = cli_with(None, None, Some("http://node"), "127.0.0.1", 8232, None);
        cli.donation_address = Some(crate::testutil::example_taddress());
        assert!(cli.resolve().is_err());
    }

    #[test]
    fn resolve_rejects_a_truncated_donation_address() {
        // A valid unified address missing its last character still starts with `u`, but its
        // checksum no longer verifies, so decoding must reject it.
        let valid_ua = crate::testutil::example_unified_address();
        let mut cli = cli_with(None, None, Some("http://node"), "127.0.0.1", 8232, None);
        cli.donation_address = Some(valid_ua[..valid_ua.len() - 1].to_string());
        assert!(cli.resolve().is_err());
    }

    #[test]
    fn resolve_uses_default_server_limits() {
        let config = cli_with(None, None, Some("http://node"), "127.0.0.1", 8232, None)
            .resolve()
            .unwrap();
        assert_eq!(
            config.limits.max_concurrent_streams,
            DEFAULT_MAX_CONCURRENT_STREAMS
        );
        assert_eq!(config.limits.keepalive_interval, Duration::from_secs(60));
        assert_eq!(config.limits.keepalive_timeout, Duration::from_secs(20));
    }

    #[test]
    fn resolve_overrides_server_limits_from_flags() {
        let mut cli = cli_with(None, None, Some("http://node"), "127.0.0.1", 8232, None);
        cli.max_concurrent_streams = 512;
        cli.keepalive_interval_secs = 90;
        cli.keepalive_timeout_secs = 30;
        let config = cli.resolve().unwrap();
        assert_eq!(config.limits.max_concurrent_streams, 512);
        assert_eq!(config.limits.keepalive_interval, Duration::from_secs(90));
        assert_eq!(config.limits.keepalive_timeout, Duration::from_secs(30));
    }

    #[test]
    fn resolve_rejects_zero_max_concurrent_streams() {
        let mut cli = cli_with(None, None, Some("http://node"), "127.0.0.1", 8232, None);
        cli.max_concurrent_streams = 0;
        assert!(cli.resolve().is_err());
    }

    #[test]
    fn resolve_rejects_zero_keepalive() {
        let mut cli = cli_with(None, None, Some("http://node"), "127.0.0.1", 8232, None);
        cli.keepalive_interval_secs = 0;
        assert!(cli.resolve().is_err());
    }

    #[test]
    fn node_config_debug_redacts_password() {
        let node = NodeConfig {
            url: "http://127.0.0.1:8232".to_string(),
            user: "user".to_string(),
            password: "supersecret".to_string(),
        };
        let rendered = format!("{node:?}");
        assert!(rendered.contains("***"));
        assert!(!rendered.contains("supersecret"));
    }
}
