# Changelog

All notable changes to this project are documented here. The format is loosely based on
[Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### P5 — Hardening (in progress)
- gRPC server runs over TLS by default (`--tls-cert`/`--tls-key`), with `--no-tls-very-insecure` to run
  plaintext for local development.
- Prometheus metrics: per-method request counts and latency histograms via a gRPC `tower` layer, served at
  `/metrics` when `--metrics-bind` is set.
- Dockerfile (multi-stage, non-root runtime) and a `docker-compose.yml` stack (zebra + lightwalletd-rs).

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
