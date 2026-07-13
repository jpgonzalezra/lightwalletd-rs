# 0022. Close the operational-surface gap with the Go reference

## Context

A review of `lightwalletd-rs` against the Go reference (`cmd/root.go`, `common/logging`) found a cluster
of small operational gaps that individually looked minor but together meant an operator migrating from
Go lightwalletd would lose tooling and safety nets they relied on: no gRPC reflection (`grpcurl list`
needed a local `.proto` checkout), metrics off by default (Go always serves Prometheus on `:9068`), no
file/JSON logging option, no self-signed-cert convenience for local TLS, no `--nocache` debugging mode,
and no auto-shutdown for a darkside mock server left running by a forgotten CI job. None of these are
correctness bugs in the wallet-facing protocol; all of them are operability parity.

## Decision

Close each gap, matching Go's behavior where it makes sense and diverging deliberately (and visibly)
where it does not:

- **gRPC reflection, always on.** `build.rs` now emits a `FileDescriptorSet` via
  `tonic-prost-build`'s `file_descriptor_set_path`; `src/proto.rs` embeds it
  (`proto::FILE_DESCRIPTOR_SET`) and `src/lib.rs::reflection_service()` registers it with
  `tonic-reflection`'s v1 builder in both the live and darkside branches of `run`. Go gates reflection
  behind a log-level check that is true at its own default (`LogLevel >= WarnLevel`); rather than port
  that indirection, reflection is simply always registered — it is metadata about the API shape, not a
  security-sensitive surface, so there is no reason to make it conditional.
- **Metrics default-on at `127.0.0.1:9068`.** `--metrics-bind` changes from `Option<SocketAddr>`
  (unset = disabled) to a plain `SocketAddr` defaulting to `127.0.0.1:9068` — Go's fixed port — with a
  new `--no-metrics` flag to opt out. `Config.metrics_bind` stays `Option<SocketAddr>`, resolved as
  `(!no_metrics).then_some(metrics_bind)`, so `run`'s metrics-serving branch is unchanged.
- **`--log-level` / `--log-file`.** `--log-level` (default `"info"`) feeds a `tracing_subscriber::EnvFilter`;
  an explicit `RUST_LOG` still wins, which is the standard `tracing-subscriber` convention and avoids
  inventing a second precedence rule for the same knob. `--log-file`, when set, switches output to JSON
  lines appended to that file (`tracing-subscriber`'s `json` feature; a plain `std::fs::File` opened
  `create().append()` — no `tracing-appender` dependency needed for a single long-lived file handle),
  matching Go's switch to `logrus.JSONFormatter` on `--log-file`. Both flags live on `Cli` and are
  consumed directly in `main.rs` before `Cli::resolve()` runs (so `resolve()`'s own `tracing::warn!`
  calls, e.g. from `--gen-cert-very-insecure`, are captured by the subscriber they configure); they are
  not part of `Config`, since nothing under `run` needs them.
- **`--gen-cert-very-insecure`.** Generates an in-memory self-signed certificate for `"localhost"` via
  `rcgen::generate_simple_self_signed` at `Cli::resolve` time, stored as
  `TlsConfig::GeneratedInsecure { cert_pem, key_pem }` (a third `TlsConfig` variant alongside `Enabled`
  and `Disabled`). Mutually exclusive with `--tls-cert`/`--tls-key` and `--no-tls-very-insecure`,
  validated in `resolve()` — the same place the existing `Enabled`/`Disabled` split is decided — rather
  than via `clap`'s `conflicts_with`, so the error message can explain *why* (mirrors the existing
  `--sync-from-height`/`--redownload` and donation-address validation already living there). Logs the
  same loud startup warning Go does. `TlsConfig`'s `Debug` is hand-written (as `NodeConfig`'s already is
  for credentials) so the private key can never leak through a stray `{:?}`.
- **`--darkside-timeout-minutes`** (default 30, matching Go's fixed default). Go's `DarksideInit` spawns
  a goroutine that sleeps for the timeout and then calls `Log.Fatal` — an abrupt process exit with no
  drain and no way to disable it. We reuse the graceful-shutdown `Notify` the `Stop` RPC already drives:
  after the timeout, a spawned task logs a warning and calls `notify_one()`, so `serve_with_shutdown`
  drains in-flight requests and `run` returns normally instead of the process being killed mid-response.
  Same operational guarantee (a leaked mock server cannot run forever), safer shutdown. Go has no way to
  disable the timeout, so neither do we; `0` shuts down almost immediately (matching `time.Sleep(0)`) and
  a very large value gives an effectively unbounded local session.
- **`--nocache`.** Skips spawning the ingestor and opens the block cache (via the existing `Cache::open`)
  in a `tempfile::tempdir()` instead of under `--data-dir`, so every read falls through to the node —
  matching Go's `--nocache` serving behavior (no on-disk cache files at all) without adding a second code
  path through `Cache`. Debugging only: throughput without a cache is far worse than the normal path.
- **Env-var tunables.** `clap`'s `env` feature is enabled; `--ingest-window`/`--ingest-concurrency` gain
  `LWD_INGEST_WINDOW`/`LWD_INGEST_CONCURRENCY`, and `--log-level`/`--log-file` gain
  `LWD_LOG_LEVEL`/`LWD_LOG_FILE`. Precedence (explicit flag, then env, then default) is `clap`'s native
  behavior, so no custom merge logic was written.
- **Data-dir divergence, kept deliberately.** Go's default `--data-dir` is `/var/lib/lightwalletd`, which
  requires root (or a pre-created, writable directory) on a stock system. `lightwalletd-rs` keeps its
  existing `./lightwalletd-rs-data` default rather than copying that friction — this is a conscious,
  documented divergence, not an oversight.

Two new dependencies: `tonic-reflection` (same `0.14.6` line as the already-pinned `tonic`/`tonic-prost`,
so no new major-version cohort to track) and `rcgen` (`0.14.8`, default features only — `crypto` +
`pem` + `ring` — no extra feature surface). Both are justified the same way every dependency in this
repo is: `tonic-reflection` is the canonical, first-party (`hyperium/tonic`) implementation of a
protocol we would otherwise have to hand-roll against the same generated types we already produce;
`rcgen` is the standard, current library for exactly one narrow job (emit a self-signed cert + key as
PEM) behind a flag whose entire purpose is "insecure, development only" — reimplementing X.509
generation in-house for a debugging convenience would be strictly worse. `tempfile` moves from
`dev-dependencies` to `dependencies` (same pinned `3.27.0`) since `--nocache` now uses it at runtime, not
just in tests.

## Consequences

- A default `cargo run` now serves Prometheus metrics on `127.0.0.1:9068` and answers gRPC reflection
  queries; both are new listening/response surfaces on by default, mitigated by metrics staying bound to
  loopback by default (`--metrics-bind` is still overridable, same as before) and reflection carrying no
  more information than the checked-in `.proto` files already do.
- `docs/ARCHITECTURE.md`'s "Metrics" and "Running" sections and `README.md`'s flag table needed updating
  to reflect the new default and the reflection-enabled `grpcurl` invocation (no more `-proto`/
  `-import-path`).
- `--gen-cert-very-insecure` and `--nocache` add two more `*-very-insecure`/debugging-only flags to the
  surface established by ADR 0012; they follow the same naming and warning conventions, so the pattern
  stays consistent rather than growing a second convention.
- The darkside auto-shutdown is a behavior change for any script that starts darkside and expects it to
  run indefinitely with no flag: it now exits after 30 minutes by default. This is the intended fix for
  the leaked-mock-server problem the review flagged, and matches Go's own (undisableable) default.
- Existing `Config`/`Cli` tests in `src/config.rs` were extended in place (the `cli_with` test helper
  gained the new fields) rather than duplicated, keeping one source of truth for "what a fully-specified
  `Cli` looks like" in tests.
