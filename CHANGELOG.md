# Changelog

All notable changes to this project are documented here. The format is loosely based on
[Keep a Changelog](https://keepachangelog.com/).

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
