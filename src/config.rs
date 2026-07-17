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
/// Default blocks fetched and committed per ingest window while catching up to the node tip.
pub const DEFAULT_INGEST_WINDOW: usize = 64;
/// Default concurrent block fetches from the node while catching up.
pub const DEFAULT_INGEST_CONCURRENCY: usize = 8;
/// Default keepalive ping interval, in seconds, on an idle connection.
pub const DEFAULT_KEEPALIVE_INTERVAL_SECS: u64 = 60;
/// Default time, in seconds, to wait for a keepalive ack before dropping a connection.
pub const DEFAULT_KEEPALIVE_TIMEOUT_SECS: u64 = 20;
/// Default darkside auto-shutdown timeout, in minutes — matches the Go reference's fixed default.
pub const DEFAULT_DARKSIDE_TIMEOUT_MINUTES: u64 = 30;
/// Default tracing filter when neither `--log-level` nor `RUST_LOG` is given.
pub const DEFAULT_LOG_LEVEL: &str = "info";

/// A CLI-parsed string whose `Debug` prints `"***"`, so a secret flag can never leak via a stray
/// `{:?}` on [`Cli`] (which derives `Debug` for clap's error messages).
#[derive(Clone)]
pub struct RedactedString(String);

impl std::fmt::Debug for RedactedString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "***")
    }
}

impl std::str::FromStr for RedactedString {
    type Err = std::convert::Infallible;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(RedactedString(value.to_string()))
    }
}

impl RedactedString {
    /// Consume the wrapper and return the underlying secret.
    fn into_inner(self) -> String {
        self.0
    }
}

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

    /// zebrad RPC host, used when `--rpc-url` is not given (overrides `zcash.conf`; defaults to
    /// 127.0.0.1 when neither flag nor `zcash.conf` provide it).
    #[arg(long)]
    pub rpc_host: Option<String>,

    /// zebrad RPC port, used when `--rpc-url` is not given (overrides `zcash.conf`; defaults to
    /// 8232 when neither flag nor `zcash.conf` provide it).
    #[arg(long)]
    pub rpc_port: Option<u16>,

    /// RPC username (overrides the value from `--zcash-conf`).
    #[arg(long)]
    pub rpc_user: Option<RedactedString>,

    /// RPC password (overrides the value from `--zcash-conf`).
    #[arg(long)]
    pub rpc_password: Option<RedactedString>,

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

    /// Path to a PEM TLS certificate (required unless `--no-tls-very-insecure` or
    /// `--gen-cert-very-insecure`).
    #[arg(long)]
    pub tls_cert: Option<PathBuf>,

    /// Path to the PEM TLS private key (required unless `--no-tls-very-insecure` or
    /// `--gen-cert-very-insecure`).
    #[arg(long)]
    pub tls_key: Option<PathBuf>,

    /// Run the gRPC server without TLS (plaintext). Insecure — development only.
    #[arg(long = "no-tls-very-insecure")]
    pub no_tls: bool,

    /// Generate an in-memory self-signed TLS certificate at startup instead of reading
    /// `--tls-cert`/`--tls-key` from disk. Insecure — the certificate is not trusted by anything,
    /// so it only helps a client that already skips verification; development only. Mutually
    /// exclusive with `--tls-cert`/`--tls-key` and `--no-tls-very-insecure`.
    #[arg(long = "gen-cert-very-insecure")]
    pub gen_cert: bool,

    /// Address to serve Prometheus metrics on (`/metrics`). On by default, matching the Go
    /// reference's fixed `:9068`; disable with `--no-metrics`.
    #[arg(long, default_value = "127.0.0.1:9068")]
    pub metrics_bind: SocketAddr,

    /// Disable the Prometheus metrics HTTP server.
    #[arg(long)]
    pub no_metrics: bool,

    /// Run as a darkside mock server (no real node) for deterministic wallet tests. Insecure —
    /// testing only; never deploy in production.
    #[arg(long = "darkside-very-insecure")]
    pub darkside: bool,

    /// In darkside mode, shut the mock server down after this many minutes so a forgotten or
    /// leaked CI job cannot serve indefinitely (matches the Go reference's fixed 30-minute
    /// default; Go has no way to disable it, so neither do we — pass a very large value for an
    /// effectively unbounded local session). Ignored outside darkside mode.
    #[arg(long, default_value_t = DEFAULT_DARKSIDE_TIMEOUT_MINUTES)]
    pub darkside_timeout_minutes: u64,

    /// Run without the on-disk block cache: every block read falls through to the node instead of
    /// being served from `--data-dir`. Debugging only — throughput suffers badly against a real
    /// chain, since nothing is cached between requests.
    #[arg(long)]
    pub nocache: bool,

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

    /// Blocks fetched and committed per ingest window while catching up to the node tip.
    #[arg(long, default_value_t = DEFAULT_INGEST_WINDOW, env = "LWD_INGEST_WINDOW")]
    pub ingest_window: usize,

    /// Concurrent block fetches from the node while catching up.
    #[arg(long, default_value_t = DEFAULT_INGEST_CONCURRENCY, env = "LWD_INGEST_CONCURRENCY")]
    pub ingest_concurrency: usize,

    /// Tracing filter (a level like "info"/"debug", or full `EnvFilter` directives such as
    /// "lightwalletd_rs=debug,warn"). An explicit `RUST_LOG` environment variable always takes
    /// precedence over this flag, matching the usual `tracing-subscriber` convention.
    #[arg(long, default_value = DEFAULT_LOG_LEVEL, env = "LWD_LOG_LEVEL")]
    pub log_level: String,

    /// Write JSON lines to this file instead of human-readable text on stderr (matches the Go
    /// reference's `--log-file`, which switches its logrus output to JSON).
    #[arg(long, env = "LWD_LOG_FILE")]
    pub log_file: Option<PathBuf>,

    /// How to reach chain data: `rpc` proxies every call over JSON-RPC; `readstate` serves reads
    /// from a co-located zebrad's state in-process (ADR 0023; requires the `readstate` build
    /// feature, a same-host zebrad, and `--zebra-indexer-url`), keeping JSON-RPC only for
    /// transaction submission, the mempool, and `getinfo`.
    #[arg(long, value_enum, default_value_t = Backend::Rpc)]
    pub backend: Backend,

    /// Path of the zebrad cache directory holding the state to read (`readstate` backend only).
    /// Defaults to zebra's own default cache directory when unset.
    #[arg(long)]
    pub zebra_state_dir: Option<PathBuf>,

    /// Address of the zebrad indexer gRPC (`indexer_listen_addr` in zebrad.toml), used by the
    /// `readstate` backend to follow the non-finalized chain. Required with `--backend readstate`.
    #[arg(long)]
    pub zebra_indexer_url: Option<SocketAddr>,
}

/// Which backend serves chain data (see `--backend`). CLI-side selector only; [`Cli::resolve`]
/// pairs it with the settings each backend needs into a [`BackendConfig`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Backend {
    /// Every call goes to the node over JSON-RPC (works with any reachable zebrad).
    Rpc,
    /// Reads come from the zebrad state in-process; JSON-RPC keeps the node-only surfaces.
    Readstate,
}

/// The resolved backend selection, carrying the settings the chosen backend needs — so a
/// `readstate` backend without an indexer address is unrepresentable past [`Cli::resolve`].
#[derive(Debug, Clone)]
pub enum BackendConfig {
    /// Every call goes to the node over JSON-RPC (works with any reachable zebrad).
    Rpc,
    /// Reads come from the zebrad state in-process; JSON-RPC keeps the node-only surfaces.
    Readstate {
        /// zebrad cache directory holding the state to read (zebra's own default when `None`).
        state_dir: Option<PathBuf>,
        /// zebrad indexer gRPC address, used to follow the non-finalized chain to the true tip.
        indexer_url: SocketAddr,
    },
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
    /// How long a darkside server runs before auto-shutting down (see `--darkside-timeout-minutes`).
    /// Ignored outside darkside mode.
    pub darkside_timeout: Duration,
    /// Run without the on-disk block cache (see `--nocache`): the cache is opened in a throwaway
    /// temp dir and the ingestor is not spawned, so every read falls through to the node.
    pub nocache: bool,
    /// Whether the `Ping` gRPC is enabled (testing/benchmark only); off by default for hardening.
    pub ping_enable: bool,
    /// Donation unified address advertised in `GetLightdInfo`, if configured.
    pub donation_address: Option<String>,
    /// gRPC server resource limits / hardening.
    pub limits: ServerLimits,
    /// Ingestor catch-up tuning.
    pub ingest: IngestConfig,
    /// Which backend serves chain data, with the settings it needs.
    pub backend: BackendConfig,
}

/// Ingestor catch-up tuning: how aggressively the cache is filled while behind the node tip.
#[derive(Debug, Clone, Copy)]
pub struct IngestConfig {
    /// Blocks fetched and committed per window (one cache transaction per window).
    pub window: usize,
    /// Concurrent block fetches from the node within a window.
    pub concurrency: usize,
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
#[derive(Clone)]
pub enum TlsConfig {
    /// Serve over TLS with the given PEM certificate and private-key file paths.
    Enabled {
        /// Path to the PEM certificate.
        cert: PathBuf,
        /// Path to the PEM private key.
        key: PathBuf,
    },
    /// Serve over TLS with an in-memory self-signed certificate generated at startup via
    /// `--gen-cert-very-insecure` (see [`Cli::resolve`]).
    GeneratedInsecure {
        /// PEM-encoded self-signed certificate.
        cert_pem: String,
        /// PEM-encoded private key.
        key_pem: String,
    },
    /// Serve plaintext (no TLS) — insecure, development only.
    Disabled,
}

impl std::fmt::Debug for TlsConfig {
    /// Hand-written so a stray `{:?}` can never leak `GeneratedInsecure`'s private key into a log,
    /// the same discipline applied to [`NodeConfig`]'s credentials.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TlsConfig::Enabled { cert, key } => f
                .debug_struct("Enabled")
                .field("cert", cert)
                .field("key", key)
                .finish(),
            TlsConfig::GeneratedInsecure { cert_pem, .. } => f
                .debug_struct("GeneratedInsecure")
                .field("cert_pem", cert_pem)
                .field("key_pem", &"***")
                .finish(),
            TlsConfig::Disabled => write!(f, "Disabled"),
        }
    }
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

        let user = self
            .rpc_user
            .map(RedactedString::into_inner)
            .or(conf.rpcuser)
            .unwrap_or_default();
        let password = self
            .rpc_password
            .map(RedactedString::into_inner)
            .or(conf.rpcpassword)
            .unwrap_or_default();

        let url = match self.rpc_url {
            Some(url) => url,
            None => {
                let host = self
                    .rpc_host
                    .or(conf.rpcbind)
                    .unwrap_or_else(|| "127.0.0.1".to_string());
                let port = self.rpc_port.or(conf.rpcport).unwrap_or(8232);
                format!("http://{host}:{port}")
            }
        };

        if self.gen_cert && self.no_tls {
            anyhow::bail!(
                "--gen-cert-very-insecure and --no-tls-very-insecure select mutually exclusive \
                 transport modes (self-signed TLS vs. plaintext); pass only one"
            );
        }
        if self.gen_cert && (self.tls_cert.is_some() || self.tls_key.is_some()) {
            anyhow::bail!(
                "--gen-cert-very-insecure cannot be combined with --tls-cert/--tls-key; pass \
                 either an on-disk certificate or --gen-cert-very-insecure, not both"
            );
        }

        let tls = if self.no_tls {
            TlsConfig::Disabled
        } else if self.gen_cert {
            tracing::warn!(
                "--gen-cert-very-insecure: generating an in-memory self-signed TLS certificate \
                 for \"localhost\" — trusted by nothing, development only, never use in production"
            );
            let rcgen::CertifiedKey { cert, signing_key } =
                rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
                    .context("generating a self-signed TLS certificate")?;
            TlsConfig::GeneratedInsecure {
                cert_pem: cert.pem(),
                key_pem: signing_key.serialize_pem(),
            }
        } else {
            let message = "TLS is required: pass --tls-cert and --tls-key, --gen-cert-very-insecure \
                for a self-signed certificate, or --no-tls-very-insecure for plaintext";
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
        if self.ingest_window == 0 || self.ingest_concurrency == 0 {
            anyhow::bail!("--ingest-window and --ingest-concurrency must be greater than 0");
        }
        let backend = match self.backend {
            Backend::Rpc => BackendConfig::Rpc,
            Backend::Readstate => {
                if self.darkside {
                    anyhow::bail!("--backend readstate cannot be combined with darkside mode");
                }
                BackendConfig::Readstate {
                    state_dir: self.zebra_state_dir,
                    indexer_url: self.zebra_indexer_url.context(
                        "--backend readstate requires --zebra-indexer-url (the zebrad \
                         indexer_listen_addr; enable it in zebrad.toml under [rpc])",
                    )?,
                }
            }
        };

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
            metrics_bind: (!self.no_metrics).then_some(self.metrics_bind),
            darkside: self.darkside,
            darkside_timeout: Duration::from_secs(self.darkside_timeout_minutes.saturating_mul(60)),
            nocache: self.nocache,
            ping_enable: self.ping_enable,
            donation_address: self.donation_address,
            limits: ServerLimits {
                max_concurrent_streams: self.max_concurrent_streams,
                keepalive_interval: Duration::from_secs(self.keepalive_interval_secs),
                keepalive_timeout: Duration::from_secs(self.keepalive_timeout_secs),
            },
            ingest: IngestConfig {
                window: self.ingest_window,
                concurrency: self.ingest_concurrency,
            },
            backend,
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

/// An actionable error explaining that `--zcash-conf` expects an ini-style `zcash.conf`, not a
/// zebrad TOML config, and pointing the operator at the flags that work directly with zebrad.
fn not_ini_style_error(path: &Path) -> anyhow::Error {
    anyhow::anyhow!(
        "--zcash-conf {} looks like a zebrad TOML config, not an ini-style zcash.conf \
         (rpcuser/rpcpassword/rpcbind/rpcport key=value pairs); parsing it would silently \
         yield no credentials and fall back to 127.0.0.1:8232 with no auth. For zebrad, drop \
         --zcash-conf and set --rpc-url (or --rpc-host/--rpc-port) and, if zebrad's RPC has \
         auth enabled, --rpc-user/--rpc-password instead.",
        path.display()
    )
}

/// Parse the `key=value` lines of a `zcash.conf`, ignoring comments and blank lines.
///
/// Fails fast, instead of silently extracting nothing, when the file is evidently a zebrad TOML
/// config rather than an ini-style `zcash.conf`: either its extension is `.toml`, or its content
/// has a `[section]` header (TOML syntax that an ini `key=value` parser would just skip over).
fn parse_zcash_conf(path: &Path) -> Result<ZcashConf> {
    if path.extension().and_then(|ext| ext.to_str()) == Some("toml") {
        return Err(not_ini_style_error(path));
    }

    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading zcash.conf at {}", path.display()))?;

    if looks_like_toml(&text) {
        return Err(not_ini_style_error(path));
    }

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
            "rpcport" => {
                conf.rpcport = match value.parse() {
                    Ok(port) => Some(port),
                    Err(error) => {
                        tracing::warn!(%value, %error, "ignoring unparseable rpcport in zcash.conf");
                        None
                    }
                }
            }
            _ => {}
        }
    }
    Ok(conf)
}

/// Whether `text` contains a TOML `[section]` (or `[[array-of-tables]]`) header — evidence the
/// file is a zebrad TOML config rather than an ini-style `zcash.conf`, whose `key=value` lines
/// never start with `[`.
fn looks_like_toml(text: &str) -> bool {
    text.lines().any(|line| {
        let line = line.trim();
        if !(line.starts_with('[') && line.ends_with(']')) {
            return false;
        }
        // A bracketed section header, e.g. `[rpc]` or `[[servers]]`; not e.g. a bare
        // `key=[1, 2, 3]` TOML array assignment, which contains an `=` before the `[`.
        !line.contains('=')
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::Mutex;

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

    #[test]
    fn parse_zcash_conf_rejects_a_toml_extension_file() {
        let f = tempfile::Builder::new().suffix(".toml").tempfile().unwrap();
        std::fs::write(f.path(), "cache_dir = \"/var/cache/zebra\"\n").unwrap();

        let error = parse_zcash_conf(f.path()).unwrap_err();

        let message = error.to_string();
        assert!(message.contains("--rpc-url"));
        assert!(message.contains("--rpc-user"));
        assert!(message.contains("--rpc-password"));
        assert!(message.contains("--rpc-host"));
        assert!(message.contains("--rpc-port"));
    }

    #[test]
    fn parse_zcash_conf_rejects_a_file_with_toml_section_headers() {
        // A zebrad.toml with no recognizable extension (e.g. renamed, or piped in some other
        // way) is still caught by its `[rpc]` section header, which an ini `key=value` parser
        // would otherwise just skip over silently.
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(
            f,
            "[rpc]\nlisten_addr = \"127.0.0.1:8232\"\n\n[state]\ncache_dir = \"/var/cache\"\n"
        )
        .unwrap();

        let error = parse_zcash_conf(f.path()).unwrap_err();

        let message = error.to_string();
        assert!(message.contains("--rpc-url"));
        assert!(message.contains("zebrad"));
    }

    #[test]
    fn parse_zcash_conf_accepts_a_normal_ini_style_file() {
        // A plain zcash.conf, with no TOML section headers, still parses normally.
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(f, "rpcuser=alice\nrpcpassword=s3cret\nrpcport=8232\n").unwrap();

        let conf = parse_zcash_conf(f.path()).unwrap();

        assert_eq!(conf.rpcuser, Some("alice".to_string()));
        assert_eq!(conf.rpcpassword, Some("s3cret".to_string()));
        assert_eq!(conf.rpcport, Some(8232));
    }

    #[test]
    fn resolve_surfaces_the_toml_rejection_error() {
        let f = tempfile::Builder::new().suffix(".toml").tempfile().unwrap();
        std::fs::write(f.path(), "[rpc]\nlisten_addr = \"127.0.0.1:8232\"\n").unwrap();

        let error = cli_with(None, None, None, None, None, Some(f.path().to_path_buf()))
            .resolve()
            .unwrap_err();

        assert!(error.to_string().contains("--rpc-url"));
    }

    fn cli_with(
        rpc_user: Option<&str>,
        rpc_password: Option<&str>,
        rpc_url: Option<&str>,
        rpc_host: Option<&str>,
        rpc_port: Option<u16>,
        zcash_conf: Option<PathBuf>,
    ) -> Cli {
        Cli {
            grpc_bind: "127.0.0.1:9067".parse().unwrap(),
            rpc_url: rpc_url.map(str::to_string),
            rpc_host: rpc_host.map(str::to_string),
            rpc_port,
            rpc_user: rpc_user.map(|value| value.parse().unwrap()),
            rpc_password: rpc_password.map(|value| value.parse().unwrap()),
            zcash_conf,
            data_dir: PathBuf::from("./data"),
            start_height: None,
            sync_from_height: None,
            redownload: false,
            tls_cert: None,
            tls_key: None,
            no_tls: true,
            gen_cert: false,
            metrics_bind: "127.0.0.1:9068".parse().unwrap(),
            no_metrics: false,
            darkside: false,
            darkside_timeout_minutes: DEFAULT_DARKSIDE_TIMEOUT_MINUTES,
            nocache: false,
            ping_enable: false,
            donation_address: None,
            max_concurrent_streams: DEFAULT_MAX_CONCURRENT_STREAMS,
            keepalive_interval_secs: DEFAULT_KEEPALIVE_INTERVAL_SECS,
            keepalive_timeout_secs: DEFAULT_KEEPALIVE_TIMEOUT_SECS,
            ingest_window: DEFAULT_INGEST_WINDOW,
            ingest_concurrency: DEFAULT_INGEST_CONCURRENCY,
            log_level: DEFAULT_LOG_LEVEL.to_string(),
            log_file: None,
            backend: Backend::Rpc,
            zebra_state_dir: None,
            zebra_indexer_url: None,
        }
    }

    #[test]
    fn resolve_defaults_to_the_rpc_backend() {
        let config = cli_with(None, None, Some("http://node"), None, None, None)
            .resolve()
            .unwrap();
        assert!(matches!(config.backend, BackendConfig::Rpc));
    }

    #[test]
    fn resolve_rejects_readstate_without_an_indexer_url() {
        let mut cli = cli_with(None, None, Some("http://node"), None, None, None);
        cli.backend = Backend::Readstate;
        let error = cli.resolve().unwrap_err().to_string();
        assert!(error.contains("--zebra-indexer-url"));
    }

    #[test]
    fn resolve_rejects_readstate_combined_with_darkside() {
        let mut cli = cli_with(None, None, Some("http://node"), None, None, None);
        cli.backend = Backend::Readstate;
        cli.zebra_indexer_url = Some("127.0.0.1:8231".parse().unwrap());
        cli.darkside = true;
        assert!(cli.resolve().is_err());
    }

    #[test]
    fn resolve_accepts_a_configured_readstate_backend() {
        let mut cli = cli_with(None, None, Some("http://node"), None, None, None);
        cli.backend = Backend::Readstate;
        cli.zebra_indexer_url = Some("127.0.0.1:8231".parse().unwrap());
        cli.zebra_state_dir = Some(PathBuf::from("/var/cache/zebra"));
        let config = cli.resolve().unwrap();
        match config.backend {
            BackendConfig::Readstate {
                state_dir,
                indexer_url,
            } => {
                assert_eq!(state_dir, Some(PathBuf::from("/var/cache/zebra")));
                assert_eq!(indexer_url, "127.0.0.1:8231".parse().unwrap());
            }
            other => panic!("expected BackendConfig::Readstate, got {other:?}"),
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
            None,
            Some(9999),
            Some(f.path().to_path_buf()),
        )
        .resolve()
        .unwrap();

        assert_eq!(config.node.user, "flaguser");
        assert_eq!(config.node.password, "flagpass");
        // Explicit non-default port flag beats the conf file's rpcport.
        assert_eq!(config.node.url, "http://127.0.0.1:9999");
    }

    #[test]
    fn resolve_uses_zcash_conf_host_and_port_when_no_flag_given() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(f, "rpcbind=10.0.0.5\nrpcport=18232\n").unwrap();

        let config = cli_with(None, None, None, None, None, Some(f.path().to_path_buf()))
            .resolve()
            .unwrap();

        assert_eq!(config.node.url, "http://10.0.0.5:18232");
    }

    #[test]
    fn resolve_builds_url_from_host_and_port_when_rpc_url_absent() {
        let config = cli_with(None, None, None, Some("192.168.0.5"), Some(8232), None)
            .resolve()
            .unwrap();

        assert_eq!(config.node.url, "http://192.168.0.5:8232");
        assert_eq!(config.node.user, "");
        assert_eq!(config.node.password, "");
    }

    #[test]
    fn resolve_with_no_tls_yields_disabled() {
        let config = cli_with(None, None, Some("http://node"), None, None, None)
            .resolve()
            .unwrap();
        assert!(matches!(config.tls, TlsConfig::Disabled));
    }

    #[test]
    fn resolve_requires_a_cert_when_tls_is_enabled() {
        let mut cli = cli_with(None, None, Some("http://node"), None, None, None);
        cli.no_tls = false;
        assert!(cli.resolve().is_err());
    }

    #[test]
    fn resolve_accepts_and_stores_a_valid_donation_address() {
        let valid_ua = crate::testutil::example_unified_address();
        let mut cli = cli_with(None, None, Some("http://node"), None, None, None);
        cli.donation_address = Some(valid_ua.clone());

        let config = cli.resolve().unwrap();

        assert_eq!(config.donation_address.as_deref(), Some(valid_ua.as_str()));
    }

    #[test]
    fn resolve_rejects_a_non_unified_donation_address() {
        let mut cli = cli_with(None, None, Some("http://node"), None, None, None);
        cli.donation_address = Some(crate::testutil::example_taddress());
        assert!(cli.resolve().is_err());
    }

    #[test]
    fn resolve_rejects_a_truncated_donation_address() {
        // A valid unified address missing its last character still starts with `u`, but its
        // checksum no longer verifies, so decoding must reject it.
        let valid_ua = crate::testutil::example_unified_address();
        let mut cli = cli_with(None, None, Some("http://node"), None, None, None);
        cli.donation_address = Some(valid_ua[..valid_ua.len() - 1].to_string());
        assert!(cli.resolve().is_err());
    }

    #[test]
    fn resolve_uses_default_server_limits() {
        let config = cli_with(None, None, Some("http://node"), None, None, None)
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
        let mut cli = cli_with(None, None, Some("http://node"), None, None, None);
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
        let mut cli = cli_with(None, None, Some("http://node"), None, None, None);
        cli.max_concurrent_streams = 0;
        assert!(cli.resolve().is_err());
    }

    #[test]
    fn resolve_uses_default_ingest_tuning() {
        let config = cli_with(None, None, Some("http://node"), None, None, None)
            .resolve()
            .unwrap();
        assert_eq!(config.ingest.window, DEFAULT_INGEST_WINDOW);
        assert_eq!(config.ingest.concurrency, DEFAULT_INGEST_CONCURRENCY);
    }

    #[test]
    fn resolve_rejects_zero_ingest_tuning() {
        let mut cli = cli_with(None, None, Some("http://node"), None, None, None);
        cli.ingest_window = 0;
        assert!(cli.resolve().is_err());

        let mut cli = cli_with(None, None, Some("http://node"), None, None, None);
        cli.ingest_concurrency = 0;
        assert!(cli.resolve().is_err());
    }

    #[test]
    fn resolve_rejects_zero_keepalive() {
        let mut cli = cli_with(None, None, Some("http://node"), None, None, None);
        cli.keepalive_interval_secs = 0;
        assert!(cli.resolve().is_err());
    }

    #[test]
    fn resolve_metrics_bind_defaults_to_localhost_9068() {
        // Matches the Go reference, which always serves Prometheus on :9068.
        let config = cli_with(None, None, Some("http://node"), None, None, None)
            .resolve()
            .unwrap();
        assert_eq!(config.metrics_bind, Some("127.0.0.1:9068".parse().unwrap()));
    }

    #[test]
    fn resolve_no_metrics_disables_the_metrics_server() {
        let mut cli = cli_with(None, None, Some("http://node"), None, None, None);
        cli.no_metrics = true;
        let config = cli.resolve().unwrap();
        assert_eq!(config.metrics_bind, None);
    }

    #[test]
    fn resolve_metrics_bind_is_overridable() {
        let mut cli = cli_with(None, None, Some("http://node"), None, None, None);
        cli.metrics_bind = "0.0.0.0:9200".parse().unwrap();
        let config = cli.resolve().unwrap();
        assert_eq!(config.metrics_bind, Some("0.0.0.0:9200".parse().unwrap()));
    }

    #[test]
    fn resolve_gen_cert_generates_an_in_memory_self_signed_certificate() {
        let mut cli = cli_with(None, None, Some("http://node"), None, None, None);
        cli.no_tls = false;
        cli.gen_cert = true;
        let config = cli.resolve().unwrap();
        match config.tls {
            TlsConfig::GeneratedInsecure { cert_pem, key_pem } => {
                assert!(cert_pem.contains("BEGIN CERTIFICATE"));
                assert!(key_pem.contains("PRIVATE KEY"));
            }
            other => panic!("expected TlsConfig::GeneratedInsecure, got {other:?}"),
        }
    }

    #[test]
    fn resolve_gen_cert_debug_redacts_the_private_key() {
        let mut cli = cli_with(None, None, Some("http://node"), None, None, None);
        cli.no_tls = false;
        cli.gen_cert = true;
        let config = cli.resolve().unwrap();
        let rendered = format!("{:?}", config.tls);
        assert!(rendered.contains("***"));
        assert!(!rendered.contains("PRIVATE KEY"));
    }

    #[test]
    fn resolve_rejects_gen_cert_combined_with_no_tls_very_insecure() {
        let mut cli = cli_with(None, None, Some("http://node"), None, None, None);
        // cli_with's default already sets no_tls: true.
        cli.gen_cert = true;
        assert!(cli.resolve().is_err());
    }

    #[test]
    fn resolve_rejects_gen_cert_combined_with_explicit_cert_and_key() {
        let mut cli = cli_with(None, None, Some("http://node"), None, None, None);
        cli.no_tls = false;
        cli.gen_cert = true;
        cli.tls_cert = Some(PathBuf::from("cert.pem"));
        cli.tls_key = Some(PathBuf::from("key.pem"));
        assert!(cli.resolve().is_err());
    }

    #[test]
    fn resolve_uses_default_darkside_timeout() {
        let config = cli_with(None, None, Some("http://node"), None, None, None)
            .resolve()
            .unwrap();
        assert_eq!(config.darkside_timeout, Duration::from_secs(30 * 60));
    }

    #[test]
    fn resolve_overrides_darkside_timeout_from_flag() {
        let mut cli = cli_with(None, None, Some("http://node"), None, None, None);
        cli.darkside_timeout_minutes = 5;
        let config = cli.resolve().unwrap();
        assert_eq!(config.darkside_timeout, Duration::from_secs(5 * 60));
    }

    #[test]
    fn resolve_propagates_the_nocache_flag() {
        let mut cli = cli_with(None, None, Some("http://node"), None, None, None);
        cli.nocache = true;
        let config = cli.resolve().unwrap();
        assert!(config.nocache);

        let config = cli_with(None, None, Some("http://node"), None, None, None)
            .resolve()
            .unwrap();
        assert!(!config.nocache);
    }

    /// Serializes tests that read or mutate process-global environment variables, so they cannot
    /// interleave and observe each other's values.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// Take [`ENV_LOCK`], tolerating poison: [`EnvVarGuard`] restores the environment even when a
    /// test body panics, so the shared state behind the lock is always clean and one failed test
    /// must not cascade into confusing secondary failures in every sibling env test.
    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Sets environment variables and removes them again on drop — even when an assertion between
    /// set and cleanup panics — so a failed test cannot leak its vars into sibling tests.
    struct EnvVarGuard {
        keys: Vec<&'static str>,
    }

    impl EnvVarGuard {
        fn set(vars: &[(&'static str, &str)]) -> Self {
            // SAFETY: callers hold ENV_LOCK, serializing all access to these process-global vars.
            unsafe {
                for (key, value) in vars {
                    std::env::set_var(key, value);
                }
            }
            Self {
                keys: vars.iter().map(|(key, _)| *key).collect(),
            }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            // SAFETY: still under the caller's ENV_LOCK (locals drop in reverse declaration order,
            // so this runs before the lock guard is released).
            unsafe {
                for key in &self.keys {
                    std::env::remove_var(key);
                }
            }
        }
    }

    #[test]
    fn cli_parsing_reads_ingest_tuning_from_env_when_flags_are_absent() {
        let _lock = env_lock();
        let _vars = EnvVarGuard::set(&[
            ("LWD_INGEST_WINDOW", "128"),
            ("LWD_INGEST_CONCURRENCY", "4"),
        ]);
        let cli = Cli::try_parse_from(["lightwalletd-rs", "--no-tls-very-insecure"]).unwrap();
        assert_eq!(cli.ingest_window, 128);
        assert_eq!(cli.ingest_concurrency, 4);
    }

    #[test]
    fn cli_parsing_flag_overrides_env_for_ingest_tuning() {
        let _lock = env_lock();
        let _vars = EnvVarGuard::set(&[("LWD_INGEST_WINDOW", "128")]);
        let cli = Cli::try_parse_from([
            "lightwalletd-rs",
            "--no-tls-very-insecure",
            "--ingest-window",
            "16",
        ])
        .unwrap();
        assert_eq!(cli.ingest_window, 16);
    }

    #[test]
    fn cli_parsing_reads_log_flags_from_env_when_flags_are_absent() {
        let _lock = env_lock();
        let _vars = EnvVarGuard::set(&[
            ("LWD_LOG_LEVEL", "debug"),
            ("LWD_LOG_FILE", "/tmp/lwd-test.log"),
        ]);
        let cli = Cli::try_parse_from(["lightwalletd-rs", "--no-tls-very-insecure"]).unwrap();
        assert_eq!(cli.log_level, "debug");
        assert_eq!(cli.log_file, Some(PathBuf::from("/tmp/lwd-test.log")));
    }

    #[test]
    fn cli_parsing_defaults_log_level_to_info_with_no_log_file() {
        // Parsing reads the LWD_* env vars, so this must hold the lock too: without it, a sibling
        // test's LWD_LOG_LEVEL could interleave and flip the "info" assertion sporadically.
        let _lock = env_lock();
        let cli = Cli::try_parse_from(["lightwalletd-rs", "--no-tls-very-insecure"]).unwrap();
        assert_eq!(cli.log_level, "info");
        assert_eq!(cli.log_file, None);
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

    #[test]
    fn cli_debug_redacts_rpc_user_and_password() {
        let cli = cli_with(
            Some("alice"),
            Some("supersecret"),
            Some("http://node"),
            None,
            None,
            None,
        );
        let rendered = format!("{cli:?}");
        assert!(rendered.contains("***"));
        assert!(!rendered.contains("supersecret"));
        assert!(!rendered.contains("alice"));
    }
}
