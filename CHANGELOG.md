# Changelog

All notable changes to this project are documented here. The format is loosely based on
[Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### P5 — Hardening (in progress)
- `Ping` (testing/benchmark RPC) is disabled by default and only enabled with `--ping-very-insecure`;
  without the flag it returns `FailedPrecondition`. A client controls both the sleep duration and the
  concurrency it observes, so leaving it open is an unnecessary denial-of-service surface.
- gRPC server runs over TLS by default (`--tls-cert`/`--tls-key`), with `--no-tls-very-insecure` to run
  plaintext for local development.
- Prometheus metrics: per-method request counts and latency histograms via a gRPC `tower` layer, served at
  `/metrics` when `--metrics-bind` is set.
- Dockerfile (multi-stage, non-root runtime) and a `docker-compose.yml` stack (zebra + lightwalletd-rs).
- Graceful shutdown on `SIGINT`/`SIGTERM`: drains in-flight requests before exiting.
- Darkside mode (`--darkside-very-insecure`): an in-memory mock chain served through the `NodeRpc` seam plus
  a `DarksideStreamer` control plane (stage/apply blocks and transactions, reorgs, captured sent
  transactions, staged subtree roots) for deterministic wallet tests. Never use in production.
- Darkside mempool: `GetMempoolTx`/`GetMempoolStream` serve the staging area, so transactions and blocks staged
  without `ApplyStaged` appear as mempool transactions until they are mined.
- Backend JSON-RPC errors are translated to the gRPC status code wallets expect, per method: height past the
  tip → `OutOfRange`, unknown transaction → `NotFound`, malformed transparent address → `InvalidArgument`
  (anything unrecognized still maps to `Unavailable`/`Internal`).
- Shared mempool monitor (live mode): one background task refreshes the mempool at most once every 2 s and fans
  the result out to all clients through a `watch` snapshot, so `GetMempoolTx`/`GetMempoolStream` node load is
  independent of the number of connected wallets (each transaction fetched and parsed once per block interval,
  ≤2 s staleness). Darkside keeps the per-request path.
- Mempool monitor resilience: a transaction that leaves the mempool between the listing and its fetch is skipped
  instead of aborting the whole refresh tick, and a node outage retains the last good snapshot until the node
  recovers.
- Startup resilience: the initial `getblockchaininfo` is retried indefinitely with capped exponential backoff
  (escalating to `error!` logs after several attempts) instead of exiting, so the server waits for a node that
  is slow to come up.
- Tip-reorg detection by hash: the ingestor reads the tip height and hash from a single `getblockchaininfo` and
  rolls back a reorg that replaces the tip block without advancing the height (caught by comparing the hash, not
  just the height).
- Cache self-protection: `add` rejects logically inconsistent writes (a block whose height does not match its
  key, or a non-monotonic append) with an error instead of a panic or a silent bad write, and an O(log n)
  open-time check decodes the tip and verifies the height range has no gaps.
- Cache auto-recovery: on a detected corruption symptom the lowest corrupt height is localized (descending from
  the tip for a corrupt suffix, binary search for a gap) and the cache is truncated from there and re-ingested,
  both at startup and during ingestion (bounded, so recovery never spins). A fetched block whose height does not
  match the request is kept on the node backoff rather than treated as cache corruption.

### P4 — Mempool, subtrees, t-addr txns & nullifiers
- `GetBlockNullifiers` and `GetBlockRangeNullifiers` (blocks pruned to shielded nullifiers only).
- `GetTaddressTxids` and `GetTaddressTransactions` (`getaddresstxids` + per-txid `getrawtransaction`).
- `GetSubtreeRoots` (`z_getsubtreesbyindex`, with the completing block looked up from the cache).
- `GetMempoolTx` (with `exclude_txid_suffixes` and `poolTypes` filtering) and `GetMempoolStream`.
- All 18 `CompactTxStreamer` methods are now implemented.

### P3 — Proxies
- `GetTransaction` and `SendTransaction` (with node rejections reported in-band in the `SendResponse`).
- `GetTreeState` and `GetLatestTreeState`.
- `GetTaddressBalance(+Stream)` and `GetAddressUtxos(+Stream)`, with `startHeight`/`maxEntries` filtering.
- `Ping` (testing only).

### P2 — Cache, ingestor & GetBlockRange
- `redb`-backed on-disk cache of compact blocks, keyed by height, with reorg rollback.
- Background ingestor that polls the node, chains blocks by `prevHash`, and fills the cache.
- `GetBlock` and `GetBlockRange` serve from the cache (falling back to the node); `GetBlockRange` streams
  ascending or descending ranges and prunes each block to the requested `poolTypes`.
- New flags: `--data-dir`, `--start-height`.

### P1 — Parser & GetBlock
- Parse raw blocks into `CompactBlock`s via `librustzcash`, validated byte-for-byte against the golden
  fixtures in `testdata/compact_blocks.json`.
- Implemented `GetBlock` (by height): verbose `getblock` for the hash and tree sizes, raw `getblock` for
  the block bytes.

### P0 — Skeleton
- Project scaffold, dependencies, and architecture docs.
- gRPC `CompactTxStreamer` service generated from the `.proto` contract.
- JSON-RPC client for the zebrad backend (generic `raw_request` + typed `getinfo`/`getblockchaininfo`).
- Configuration from CLI flags and an optional `zcash.conf`.
- Implemented `GetLightdInfo` and `GetLatestBlock`; remaining methods return `unimplemented`.
