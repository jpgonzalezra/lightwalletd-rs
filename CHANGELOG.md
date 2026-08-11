# Changelog

All notable changes to this project are documented here. The format is loosely based on
[Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Fixed
- `GetLightdInfo` reports `lightwalletProtocolVersion`, previously left empty. The field was added to
  `LightdInfo` in lightwallet-protocol v0.4.0, in the same release as `BlockRange.poolTypes`, and is
  the signal a client is required to check before requesting non-default pool types. Empty is
  indistinguishable from a pre-v0.4.0 server, so a correctly-behaving client could not tell that this
  server serves transparent and Ironwood data inside compact blocks, and had to fall back to
  shielded-only scanning. The value is a constant tracking the vendored `proto/` set, currently
  `v0.5.0`, independent of the crate version and not overwritten by the build (ADR 0031).

## [0.1.0] - 2026-08-09

First public release (beta). A caching proxy in front of a `zebrad` node that implements all 20
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
- **Windowed concurrent ingest** (ADR 0020): catch-up fetches up to `--ingest-window` blocks (default
  64) with `--ingest-concurrency` concurrent node requests (default 8) and commits each window in a
  single cache transaction, one fsync per window instead of per block. This closed the initial-sync
  throughput gap with the Go reference. Block parsing moved off the async runtime (`spawn_blocking`).
- Fetch-time txid cross-check: locally computed txids are verified against the node's verbose
  `getblock` txid list; a divergence rejects the block instead of silently corrupting wallet spend
  detection.
- Tip-reorg detection by hash: a reorg that replaces the tip block without advancing the height is caught by
  comparing the hash, not just the height.
- A node reporting a tip *below* the cached tip does not drain the cache: the cache rolls back only
  if the node's tip hash actually disagrees with the cached block at that height (a re-syncing or
  restarted node just idles the ingestor).
- A reorg reaching the `--start-height` floor empties the cache and resumes from `start_height` on
  the node's chain, instead of wedging in an error loop while serving a stale tip.
- Cache self-protection: `add` rejects logically inconsistent writes (height/key mismatch, non-monotonic
  append), and an open-time check verifies the height range has no gaps.
- Cache auto-recovery: on a detected corruption symptom the lowest corrupt height is localized and the cache
  is truncated from there and re-ingested, both at startup and during ingestion (bounded).
- Startup resilience: the initial `getblockchaininfo` is retried indefinitely with capped exponential backoff
  instead of exiting, so the server waits for a node that is slow to come up.

### Snapshot bootstrap
- **Bootstrap a fresh cache from a peer** (ADR 0024) instead of ingesting the whole range from the
  node. `--snapshot-serve` publishes this instance's cached blocks over HTTP as fixed 10,000-block
  epochs (`/snapshot/manifest`, `/snapshot/epoch/{index}`), off by default and capped by
  `--snapshot-max-concurrent-downloads`; `--snapshot-url` imports from such a peer at startup. Every
  imported block is verified four ways, including its hash against the importer's own node, which is
  what ties a snapshot to the real chain: a compact block's hash is asserted by the publisher rather
  than derivable from its contents. A failed or unreachable peer is not fatal, and the import stops
  at the operator's own node tip rather than downloading a range that node cannot verify yet. Epoch
  bodies are compressed only in transit, so the digests stay portable across servers.
- `--snapshot-url` is validated at startup, not at import time, since a bootstrap failure
  degrades to a full ingest and a typo would otherwise cost hours before anyone noticed. A plaintext
  URL is accepted with a warning: the block contents a snapshot carries are the one part no
  verification layer ties back to the operator's node. An import stops cleanly on `SIGTERM`, keeping
  the epochs it had already committed.
- After a successful import the ingestor floors at the snapshot's base height, so a deep reorg cannot
  empty the cache and silently re-sync from Sapling activation. `--redownload` clears that floor with
  the blocks.
- `getblockhash` lookups are issued in batches, which keeps verifying every height affordable.

### Block range continuity
- A served block range is verified to be one chain (ADR 0027). `GetBlockRange` and
  `GetBlockRangeNullifiers` resolve each height from the cache or the node, and during a reorg repair those
  two disagree: the cache can still hold the abandoned fork while the node already serves the new one, so a
  range spanning the boundary could splice them into a chain that never existed. Consecutive blocks are
  checked to connect by hash (ascending, the next block's `prev_hash` against the previous block's `hash`;
  descending, the reverse) and a mismatch ends the stream with `Aborted` instead of serving the splice.
- A discontinuity with a cached block on either side is reported to the ingestor, which truncates the cache
  from that height so re-ingestion refills it from the node's chain. Without it the same seam would fail
  every retry until the ingestor reached those heights on its own. A seam between two node-served blocks is
  the node reorging between two fetches, above the cached tip where nothing is cached to drop: it aborts the
  range but leaves the cache alone. The read path only reports; the ingestor remains the cache's single
  writer, and truncations are bounded at five per ten-minute window so a node the cache cannot reconcile
  with cannot drive an endless truncate/re-ingest cycle.
- Ranges read the cache in MVCC chunks (ADR 0028): 64 consecutive heights per `redb` read transaction,
  released before any node request is awaited, instead of one transaction per height. The cached blocks in a
  chunk come from a single consistent snapshot, so a truncation landing mid-range can no longer be
  straddled, and a long range does less transaction setup.

### Transactions & addresses
- `GetTransaction` and `SendTransaction` (node rejections reported in-band in the `SendResponse`).
- `GetTaddressBalance(+Stream)` and `GetAddressUtxos(+Stream)`, with `startHeight`/`maxEntries` filtering.
- `GetTaddressTxids` and `GetTaddressTransactions` (`getaddresstxids` + per-txid `getrawtransaction`).

### Tree state, subtrees & nullifiers
- `GetTreeState` and `GetLatestTreeState`.
- `GetSubtreeRoots` (`z_getsubtreesbyindex`, with the completing block looked up from the cache).
- `GetSubtreeRoots` bounds the subtree index range at the service boundary (ADR 0030). The node
  addresses subtrees with a `u16`, so a `startIndex` above 65,535 can never succeed and is rejected
  with `InvalidArgument` before any node round-trip, instead of surfacing the node's `Invalid params`
  as a retryable `Unavailable` that a wallet would loop on. A `maxEntries` above that range is a
  ceiling past the domain, which already means "all of them": it maps to the unlimited `0` rather
  than being rejected. Both checks run before the darkside branch, so both backends answer an
  out-of-range request identically.
- `GetBlockNullifiers` and `GetBlockRangeNullifiers` (blocks pruned to shielded nullifiers only).

### Mempool
- `GetMempoolTx` (with `exclude_txid_suffixes` and `poolTypes` filtering) and `GetMempoolStream`.
- Shared mempool monitor (live mode): one background task refreshes the mempool at most once every 2 s and fans
  the result out to all clients through a `watch` snapshot, so node load is independent of the number of
  connected wallets (≤2 s staleness).
- Mempool monitor resilience: a transaction that leaves the mempool between the listing and its fetch is skipped
  instead of aborting the refresh tick, and a brief node outage retains the last good snapshot until the node
  recovers.
- Staleness contract (ADR 0021): if the node has been unreachable for over 60 s, `GetMempoolTx` and
  `GetMempoolStream` return `Unavailable` (and open streams terminate) instead of serving an
  increasingly stale last-known-good snapshot with no signal.

### gRPC-web
- **Serve gRPC-web from the gRPC port** (ADR 0026) behind `--grpc-web`, so a browser wallet reaches
  the server directly instead of through a translating proxy. Off by default, because enabling it
  also makes the listener accept HTTP/1.1. `--grpc-web-allow-origin` (repeatable) restricts the
  transport to an allowlist; with none given every origin is allowed and startup says why that is a
  choice. Origins are validated at startup against the exact `scheme://host[:port]` form a browser
  sends, since a value that cannot match would only surface as an opaque CORS error.
- `grpc-status`, `grpc-message` and `grpc-status-details-bin` are exposed to JavaScript: gRPC carries
  a trailers-only response's outcome in HTTP headers, which a browser hides from a page unless the
  server exposes them. gRPC-web cannot carry a client-streamed request, so `GetTaddressBalanceStream`
  is unreachable from a browser; the unary `GetTaddressBalance` and all server-streaming methods work.
- `contrib/grpc-web-smoke.html`: a dependency-free page that exercises the transport from a real
  browser, which is the only thing that covers the preflight and the exposed-header rules.

### Backends
- **`--backend readstate`** (ADR [0023](docs/decisions/0023-zebra-readstate-backend.md), non-default
  `readstate` cargo feature): a second `NodeRpc` implementation that serves reads (blocks, tree
  states, subtrees, the transparent-address index, mined transactions, tip/chain info) from an
  in-process `zebra_state::ReadStateService` attached to a co-located zebrad's state, paired with
  `zebra_rpc::sync::TrustedChainSync` over the node's indexer gRPC for true-tip fidelity. Writes and
  node-only surfaces (`sendrawtransaction`, the mempool, `getinfo`) stay on JSON-RPC, a hybrid by
  design. `rpc` remains the default and the only supported backend for remote nodes. New flags:
  `--backend {rpc,readstate}`, `--zebra-state-dir`, `--zebra-indexer-url` (required with
  `--backend readstate`); a state-format mismatch against the running zebrad fails fast at startup.
- Wire parity was verified against a live mainnet node: 5,997 compact blocks byte-identical across
  three windows and both pool-type modes, plus clean passes on subtrees, the full address surface,
  `GetTransaction`, and error mapping (80/80 checks after fixes). Two real wire differences were found
  and fixed: an empty (not-yet-active) commitment tree serialized as `""` (rpc) vs `"000000"`
  (readstate), and `GetLightdInfo.upgradeName` rendered as `"NU6.3"` (rpc) vs `"Nu6_3"` (readstate).
- Measured performance envelope (2026-07 mainnet benchmarks,
  `contrib/bench/results/rss-bench-2026-07.md`): read surfaces win decisively (`GetTreeState` 4.1x
  faster, `GetTaddressTxids` up to 7.3x faster, time-to-tip on light recent blocks ~25% faster), but
  ingest is parse-bound and loses on heavy historical blocks: sandblasting-era ingest is ~38% slower,
  and a full genesis→tip sync is ~19% slower overall (1h 38m vs 1h 22m), because the in-process path
  pays zebra's structured-`Block` deserialize plus a re-serialize plus the compact-block parse on one
  process's cores. Operator guidance: `readstate` for steady-state serving; sync once with
  `--backend rpc` then restart with `--backend readstate` for the fastest cold sync (the on-disk cache
  is byte-identical between backends, so no re-sync is needed).
- Fixed a shutdown-path panic found by the benchmark run: a window fetch task cancelled by runtime
  shutdown (or a panicked fetch) took the ingestor down via `.expect(...)` on `JoinError::Cancelled`;
  it now logs and skips the missing height, letting the chained prefix end cleanly instead of
  panicking.

### RPC compliance (vs the Go reference)
- `GetTreeState` serves by-hash requests (height takes precedence when both are set, matching
  Go); a wrong-length hash is rejected up front with `InvalidArgument`. Go's `SkipHash` retry-walk is
  deliberately not replicated: it is a zcashd-only affordance with no zebrad equivalent.
- `GetSubtreeRoots` against a pre-NU6.3 node returns a clean empty stream when the node rejects the
  `ironwood` pool name ("no roots yet"), instead of surfacing a node error during the rollout window.
- `GetBlockRangeNullifiers` honors the requested `pool_types` (transparent stripped first, matching
  Go) and drops transactions emptied by the pool filter, so response shape matches the reference.
- Coinbase BIP34 heights decode `OP_0`/`OP_1..OP_16` and map the genesis pseudo-height
  (target-difficulty push) to 0, making blocks 0–16 servable (regtest/full-range serving).
- `getaddresstxids` omits the `"end"` key for open-ended ranges instead of sending `end: 0`.

### Configuration
- `--zcash-conf` pointed at a TOML file (e.g. a `zebrad.toml`) fails fast with an actionable
  error instead of silently extracting nothing and falling back to `127.0.0.1:8232` with no auth.

### Security
- `GetAddressUtxos` and its streaming variant reject an address list longer than
  `MAX_STREAMED_ADDRESSES` (10,000) with `ResourceExhausted`, before any node call. The backend
  cannot push down `startHeight`/`maxEntries`, so the whole result was materialized before those
  filters applied and one unauthenticated request could force unbounded backend work
  (GHSA-x4m7-3gpp-xc36).
- `GetTaddressBalanceStream` validates each address as it arrives instead of after the whole
  client stream has been received, so a stream is refused at the first malformed address rather than
  after the server has consumed every message (GHSA-x4m7-3gpp-xc36).
- `GetTaddressTransactions`/`GetTaddressTxids` never scan the address index open-endedly
  (ADR [0025](docs/decisions/0025-taddress-range-bounds.md)). A range with no `end` (or an `end` of
  zero, which is how an omitted bound reaches the server) is pinned to the chain tip at request time,
  a span wider than 10,000,000 blocks is rejected with `InvalidArgument` before the node is
  contacted, and a single 30 s deadline covers the tip lookup, the index scan, and the per-txid
  fetches, so an abandoned request cannot keep a node connection busy indefinitely. Clients may keep
  sending open-ended requests; they are answered against the tip as of the moment the request
  arrived (GHSA-x4m7-3gpp-xc36).

### Operations & hardening
- gRPC server runs over TLS by default (`--tls-cert`/`--tls-key`), with `--no-tls-very-insecure` to run
  plaintext for local development.
- `--gen-cert-very-insecure` generates an in-memory self-signed TLS certificate at startup
  (via `rcgen`) instead of requiring `--tls-cert`/`--tls-key` on disk. Insecure and mutually
  exclusive with `--tls-cert`/`--tls-key` and `--no-tls-very-insecure`; logs a loud warning on use.
- Prometheus metrics on by default (ADR 0022, ops-surface parity with the Go reference):
  per-method request counts and latency histograms via a gRPC `tower` layer, served at `/metrics` on
  `127.0.0.1:9068` (matching the Go reference's fixed port); `--metrics-bind` overrides the address,
  `--no-metrics` disables the metrics server entirely.
- gRPC Server Reflection is always registered (both live and darkside modes), so
  `grpcurl -plaintext <addr> list`/`describe` work against a running server with no local `.proto`
  checkout needed.
- `--log-level <level>` (default `info`) sets the tracing filter; an explicit `RUST_LOG`
  environment variable still takes precedence. `--log-file <path>` switches output to JSON lines
  appended to that file instead of human-readable stderr text, matching the Go reference's
  `--log-file`/logrus-JSON behavior.
- `--darkside-timeout-minutes` (default 30, matching Go's fixed default): darkside mode
  auto-shuts-down after this long, so a forgotten or leaked mock server (e.g. a stuck CI job) never
  serves indefinitely.
- `--nocache` runs without the on-disk block cache (opened in a throwaway temp dir instead, and
  the ingestor is not spawned), so every block read falls through to the node, matching Go's
  `--nocache`. Debugging only.
- Env-var fallbacks: `--ingest-window`/`--ingest-concurrency` and `--log-level`/`--log-file`
  also read `LWD_INGEST_WINDOW`/`LWD_INGEST_CONCURRENCY`/`LWD_LOG_LEVEL`/`LWD_LOG_FILE` when the flag
  is not given; an explicit flag still wins over the environment variable, which wins over the
  default.
- The `./lightwalletd-rs-data` default data directory is kept as a deliberate divergence from Go's
  `/var/lib/lightwalletd` default, which requires root on a stock system.
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

### Dependencies
- Exact-pinned NU6.3 librustzcash cohort at the published finals (ADR 0019): `zcash_address 0.13.0`,
  `zcash_primitives 0.29.0`, `zcash_protocol 0.10.0`. `zcash_protocol 0.10.0` sets the NU6.3 mainnet
  activation height (3,428,143); the pre-release pins used during development left it unset.
  `cargo tree -d` confirms a single version of `zcash_protocol` and `zcash_address`.
