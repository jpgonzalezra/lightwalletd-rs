# Changelog

All notable changes to this project are documented here. The format is loosely based on
[Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Cache & ingestor
- **Windowed concurrent ingest** (ADR 0020): catch-up fetches up to `--ingest-window` blocks (default
  64) with `--ingest-concurrency` concurrent node requests (default 8) and commits each window in a
  single cache transaction — one fsync per window instead of per block — closing the initial-sync
  throughput gap with the Go reference. Block parsing moved off the async runtime (`spawn_blocking`).
- Fetch-time txid cross-check: locally computed txids are verified against the node's verbose
  `getblock` txid list; a divergence rejects the block instead of silently corrupting wallet spend
  detection.
- A node reporting a tip *below* the cached tip no longer drains the cache: the cache rolls back only
  if the node's tip hash actually disagrees with the cached block at that height (a re-syncing or
  restarted node now just idles the ingestor).
- A reorg reaching the `--start-height` floor empties the cache and resumes from `start_height` on
  the node's chain, instead of wedging in an error loop while serving a stale tip.

### RPC compliance (vs the Go reference)
- `GetTreeState` now serves by-hash requests (height takes precedence when both are set, matching
  Go); a wrong-length hash is rejected up front with `InvalidArgument`. Go's `SkipHash` retry-walk is
  deliberately not replicated — it is a zcashd-only affordance with no zebrad equivalent.
- `GetSubtreeRoots` against a pre-NU6.3 node returns a clean empty stream when the node rejects the
  `ironwood` pool name ("no roots yet"), instead of surfacing a node error during the rollout window.
- `GetBlockRangeNullifiers` honors the requested `pool_types` (transparent stripped first, matching
  Go) and drops transactions emptied by the pool filter, so response shape matches the reference.
- Coinbase BIP34 heights now decode `OP_0`/`OP_1..OP_16` and map the genesis pseudo-height
  (target-difficulty push) to 0, making blocks 0–16 servable (regtest/full-range serving).
- `getaddresstxids` omits the `"end"` key for open-ended ranges instead of sending `end: 0`.

### Mempool
- Staleness contract (ADR 0021): if the node has been unreachable for over 60 s, `GetMempoolTx` and
  `GetMempoolStream` return `Unavailable` (and open streams terminate) instead of serving an
  increasingly stale last-known-good snapshot with no signal.

### Configuration
- `--zcash-conf` pointed at a TOML file (e.g. a `zebrad.toml`) now fails fast with an actionable
  error instead of silently extracting nothing and falling back to `127.0.0.1:8232` with no auth.

### Operations (ADR 0022: ops-surface parity with the Go reference)
- **Prometheus metrics are now on by default**, bound to `127.0.0.1:9068` (matching the Go
  reference's fixed port). `--metrics-bind` still overrides the address; `--no-metrics` disables the
  metrics server entirely. Previously metrics were off unless `--metrics-bind` was given explicitly.
- **gRPC Server Reflection** is always registered (both live and darkside modes), so
  `grpcurl -plaintext <addr> list`/`describe` work against a running server with no local `.proto`
  checkout needed.
- **`--log-level <level>`** (default `info`) sets the tracing filter; an explicit `RUST_LOG`
  environment variable still takes precedence. **`--log-file <path>`** switches output to JSON lines
  appended to that file instead of human-readable stderr text, matching the Go reference's
  `--log-file`/logrus-JSON behavior.
- **`--gen-cert-very-insecure`** generates an in-memory self-signed TLS certificate at startup
  (via `rcgen`) instead of requiring `--tls-cert`/`--tls-key` on disk. Insecure and mutually
  exclusive with `--tls-cert`/`--tls-key` and `--no-tls-very-insecure`; logs a loud warning on use.
- **`--darkside-timeout-minutes`** (default 30, matching Go's fixed default): darkside mode now
  auto-shuts-down after this long, so a forgotten or leaked mock server (e.g. a stuck CI job) never
  serves indefinitely.
- **`--nocache`** runs without the on-disk block cache (opened in a throwaway temp dir instead, and
  the ingestor is not spawned), so every block read falls through to the node — matching Go's
  `--nocache`. Debugging only.
- **Env-var fallbacks**: `--ingest-window`/`--ingest-concurrency` and `--log-level`/`--log-file` now
  also read `LWD_INGEST_WINDOW`/`LWD_INGEST_CONCURRENCY`/`LWD_LOG_LEVEL`/`LWD_LOG_FILE` when the flag
  is not given; an explicit flag still wins over the environment variable, which wins over the
  default.
- The `./lightwalletd-rs-data` default data directory is kept as a deliberate divergence from Go's
  `/var/lib/lightwalletd` default, which requires root on a stock system.

### Dependencies
- Re-bumped the NU6.3 librustzcash cohort from the pre-release pins to the published finals
  (ADR 0019): `zcash_address 0.13.0`, `zcash_primitives 0.29.0`, `zcash_protocol 0.10.0` (still
  exact-pinned). `zcash_protocol 0.10.0` sets the NU6.3 mainnet activation height (3,428,143); the
  pre-release left it unset. `cargo tree -d` confirms a single version of `zcash_protocol` and
  `zcash_address` post-bump; no source changes were required.

## [0.1.0] — beta

First public release. A caching proxy in front of a `zebrad` node that implements all 20
`CompactTxStreamer` gRPC methods.

### Chain & blocks
- `GetLightdInfo` (from `getinfo` + `getblockchaininfo`) and `GetLatestBlock`.
- Parse raw blocks into `CompactBlock`s via `librustzcash`, validated byte-for-byte against the golden
  fixtures in `testdata/compact_blocks.json`.
- `GetBlock` (by height): verbose `getblock` for the hash and tree sizes, raw `getblock` for the block bytes.
- `GetBlockRange` streams ascending or descending ranges and prunes each block to the requested `poolTypes`.

### Cache & ingestor
- `redb`-backed on-disk cache of compact blocks, keyed by height, with reorg rollback.
- Background ingestor that polls the node, chains blocks by `prevHash`, and fills the cache; `GetBlock` and
  `GetBlockRange` serve from it and fall back to the node.
- Tip-reorg detection by hash: a reorg that replaces the tip block without advancing the height is caught by
  comparing the hash, not just the height.
- Cache self-protection: `add` rejects logically inconsistent writes (height/key mismatch, non-monotonic
  append), and an open-time check verifies the height range has no gaps.
- Cache auto-recovery: on a detected corruption symptom the lowest corrupt height is localized and the cache
  is truncated from there and re-ingested, both at startup and during ingestion (bounded).
- Startup resilience: the initial `getblockchaininfo` is retried indefinitely with capped exponential backoff
  instead of exiting, so the server waits for a node that is slow to come up.

### Transactions & addresses
- `GetTransaction` and `SendTransaction` (node rejections reported in-band in the `SendResponse`).
- `GetTaddressBalance(+Stream)` and `GetAddressUtxos(+Stream)`, with `startHeight`/`maxEntries` filtering.
- `GetTaddressTxids` and `GetTaddressTransactions` (`getaddresstxids` + per-txid `getrawtransaction`).

### Tree state, subtrees & nullifiers
- `GetTreeState` and `GetLatestTreeState`.
- `GetSubtreeRoots` (`z_getsubtreesbyindex`, with the completing block looked up from the cache).
- `GetBlockNullifiers` and `GetBlockRangeNullifiers` (blocks pruned to shielded nullifiers only).

### Mempool
- `GetMempoolTx` (with `exclude_txid_suffixes` and `poolTypes` filtering) and `GetMempoolStream`.
- Shared mempool monitor (live mode): one background task refreshes the mempool at most once every 2 s and fans
  the result out to all clients through a `watch` snapshot, so node load is independent of the number of
  connected wallets (≤2 s staleness).
- Mempool monitor resilience: a transaction that leaves the mempool between the listing and its fetch is skipped
  instead of aborting the refresh tick, and a node outage retains the last good snapshot until the node recovers.

### Operations & hardening
- gRPC server runs over TLS by default (`--tls-cert`/`--tls-key`), with `--no-tls-very-insecure` to run
  plaintext for local development.
- Prometheus metrics: per-method request counts and latency histograms via a gRPC `tower` layer, served at
  `/metrics` when `--metrics-bind` is set.
- Dockerfile (multi-stage, non-root runtime) and a `docker-compose.yml` stack (zebra + lightwalletd-rs).
- Graceful shutdown on `SIGINT`/`SIGTERM`: drains in-flight requests before exiting.
- Per-method input validation rejects malformed arguments up front, and backend JSON-RPC errors are translated
  to the gRPC status code wallets expect (height past the tip → `OutOfRange`, unknown transaction → `NotFound`,
  malformed transparent address → `InvalidArgument`).
- `Ping` (testing/benchmark RPC) is disabled by default and only enabled with `--ping-very-insecure`, since a
  client controls both the sleep duration and the concurrency it observes.

### Testing
- Darkside mode (`--darkside-very-insecure`): an in-memory mock chain served through the `NodeRpc` seam plus a
  `DarksideStreamer` control plane (stage/apply blocks and transactions, reorgs, captured sent transactions,
  staged subtree roots) for deterministic wallet tests. Never use in production.
- Darkside mempool: `GetMempoolTx`/`GetMempoolStream` serve the staging area, so transactions and blocks staged
  without `ApplyStaged` appear as mempool transactions until they are mined.
