# Design: Zebra ReadStateService backend

**Status:** delivered (ADR [0023](../decisions/0023-zebra-readstate-backend.md)) · **Date:** 2026-07-14
**Research base:** zebra v6.0.0 source (`zebra-state 10.1.0`, `zebra-rpc 11.1.0`), lightwalletd-rs @ `ed38ac2`.
**Delivery:** phases P0-P3 (below) are complete — pin bump, backend + unit tests, live-mainnet parity
verification, and benchmarks. See [Measured results (2026-07)](#measured-results-2026-07) for the
headline numbers; full reports in `contrib/bench/results/rss-parity-2026-07.md` and
`rss-bench-2026-07.md`. P4 (this doc, README, ARCHITECTURE, ADR 0023) closes the delivery.

## Problem

Every read lightwalletd-rs performs today goes through zebrad's JSON-RPC: an HTTP round-trip, JSON
envelope, and hex encoding for block bytes (a ~2 MB block costs ~4 MB of hex inside a JSON string,
serialized by the node and parsed by us). For a co-located deployment — the common case — this is
pure overhead. Zebra exposes the same data in-process via `zebra_state::ReadStateService`, a tower
`Service<ReadRequest> → ReadResponse` over its RocksDB state, and supports **read-only secondary
instances** (`zebra_state::init_read_only`) that attach to a running zebrad's cache directory
without interfering with it.

## Requirements

- **R1** Serve every read path from the state service: block fetch (ingest + cache-miss serving),
  tree states, subtree roots, address balance/txids/utxos, mined-transaction lookup, chain info/tip.
- **R2** Wire behavior unchanged: byte-identical compact blocks, identical gRPC statuses and
  semantics. The golden-fixture and parity guarantees must keep holding.
- **R3** Retain JSON-RPC for what state cannot provide: `sendrawtransaction` (tx submission),
  the mempool (`getrawmempool` + mempool tx fetch), and `getinfo` build metadata. The backend is a
  **hybrid**, not a full replacement.
- **R4** True-tip fidelity. Zebra's finalized state lags the best tip by up to
  `MAX_BLOCK_REORG_HEIGHT = 1000` blocks (~21 h); a bare secondary sees only finalized blocks.
  The backend must serve the real tip.
- **R5** Backend is runtime-selectable (`--backend rpc|readstate`); `rpc` stays the default and
  fully supported (remote nodes, no shared filesystem).
- **R6** Fail fast with actionable errors when the readstate backend is misconfigured: missing/
  version-mismatched state directory (state format major must match the running zebrad, v28 for
  zebra 6.x), unreachable indexer gRPC, ephemeral config.
- **R7** The request-mapping layer must be unit-testable without a real RocksDB state.

## Architecture

### The seam holds

ADR 0007 made `dyn NodeRpc` the single node-access seam. The backend is a second implementation of
that trait — the service layer, ingestor, fetch, mempool monitor, and darkside are untouched:

```
             ┌──────────────────────── NodeRpc (trait) ───────────────────────┐
             │                                                                │
   NodeClient (JSON-RPC, default)                        ZebraStateNode (new, feature "readstate")
                                                          ├─ reads  → ReadStateService (in-process)
                                                          ├─ tip    → LatestChainTip (live)
                                                          └─ writes/mempool/getinfo → inner NodeClient
```

### True tip: `init_read_state_with_syncer`

zebra-rpc ships `TrustedChainSync` (`zebra-rpc/src/sync.rs`) for exactly this deployment: it opens
the read-only secondary state **and** subscribes to the primary zebrad's **indexer gRPC**
(`chain_tip_change` + `non_finalized_state_change` streams), committing non-finalized blocks into an
in-process `NonFinalizedState` fanned to the `ReadStateService` via its watch channel, and owning
`try_catch_up_with_primary` on the secondary. `zebra_rpc::sync::init_read_state_with_syncer(state_config,
network, indexer_addr)` returns `(ReadStateService, LatestChainTip, ChainTipChange, sync_task)` —
the single wiring entry point. Requires the zebrad to run with the `indexer` feature (in default
release binaries) and `indexer_listen_addr` set.

### NodeRpc → ReadRequest mapping

| NodeRpc method | Backend source |
|---|---|
| `get_blockchain_info` | `LatestChainTip` (height/hash) + network upgrade table computed from `zebra_chain::parameters::Network` (no RPC) |
| `get_block_verbose(height)` | `ReadRequest::Block` (hash) + `Sapling/Orchard/IronwoodTree` (sizes) + `TransactionIdsForBlock` (txid cross-check list) |
| `get_block_raw(hash)` | `ReadRequest::Block` → `ZcashSerialize` to bytes → **existing parser** (keeps the golden-fixture-verified path; no hex, no JSON, no HTTP) |
| `get_treestate(id)` | `SaplingTree`/`OrchardTree`/`IronwoodTree` frontiers, serialized as zebra-rpc's `z_gettreestate` does |
| `get_subtrees` | `SaplingSubtrees`/`OrchardSubtrees`/`IronwoodSubtrees` |
| `get_address_balance/txids/utxos` | `AddressBalance`/`TransactionIdsByAddresses`/`UtxosByAddresses` |
| `get_raw_transaction(txid)` | `ReadRequest::Transaction` (mined); **RPC fallback** when absent (mempool tx) |
| `send_raw_transaction` | inner `NodeClient` (RPC) |
| `get_raw_mempool` | inner `NodeClient` (RPC) |
| `get_info` | inner `NodeClient` (RPC; startup/GetLightdInfo only) |
| `get_block_count` | `LatestChainTip` |

The mapping layer is generic over `S: tower::Service<ReadRequest, Response = ReadResponse>` so unit
tests drive it with a scripted service (R7); `ZebraStateNode<ReadStateService>` is the production
instantiation.

One deliberate divergence in `get_blockchain_info`: the readstate backend reports
`estimatedheight = blocks` (its own tip), while zebrad's RPC estimates ahead of its tip during
initial sync. `GetLightdInfo.estimatedHeight` — which wallets use for sync progress — therefore
tracks the local state's tip instead of zebrad's network estimate while the node is still syncing.
In steady state (the only supported deployment for this backend: a synced, co-located zebrad) the
two are identical.

### Configuration

- `--backend {rpc,readstate}` (default `rpc`).
- `--zebra-state-dir` (default `~/.cache/zebra`, zebra's default cache dir) — must be the same
  filesystem as the running zebrad's `cache_dir`.
- `--zebra-indexer-url` (host:port of the zebrad indexer gRPC; required for `readstate`).
- The network is taken from the RPC `getblockchaininfo` at startup exactly as today (the RPC client
  exists in both backends), so no new network flag; the state DB is opened for that network.

### Dependency strategy

- Prerequisite: bump the librustzcash pins from `=*-pre.0` to the published finals
  (`zcash_primitives 0.29.0`, `zcash_protocol 0.10.0`, `zcash_address 0.13.0`) — the ADR 0019
  planned re-bump. Zebra 6.0.0 uses exactly this cohort, so both stacks share one dependency graph.
- `zebra-state 10.1.0`, `zebra-rpc 11.1.0`, `zebra-chain` from crates.io behind a **non-default
  cargo feature `readstate`** (RocksDB and the zebra tree add minutes of build time and MBs of
  binary; the default build stays lean and CI keeps a no-feature lane).

### Error handling and failure modes

- New `NodeError::State`/`NodeError::StateInit` variants; the existing per-method-family mapping
  (ADR 0010) extends naturally (missing block → same `OutOfRange`/`NotFound` semantics as RPC `-8`/`-5`).
- Version coupling: the state format major must match the running zebrad (v28 ↔ zebra 6.x). A
  mismatch fails at open with a message telling the operator to match versions or use `--backend rpc`.
- Indexer stream drops: `TrustedChainSync` reconnects and resumes (its design); until the first
  non-finalized block arrives the read state serves the finalized view and the tip watcher keeps the
  finalized tip current.
- The secondary is read-only at the RocksDB level; it cannot corrupt the primary.

### What deliberately does NOT change

Cache, ingestor windowing (ADR 0020), compact-block parser and its golden fixtures, txid cross-check
(now against `TransactionIdsForBlock`), service layer, mempool monitor (ADR 0005/0021), darkside,
resource caps, and the RPC backend itself.

## Impact on existing ADRs

| ADR | Impact |
|---|---|
| 0001 zebrad-only | Deepened: readstate couples to zebra's state format major; documented, rpc backend unaffected |
| 0007 NodeRpc seam | The enabling decision; backend slots behind it untouched |
| 0010 error mapping | Extended with state-error variants, same status semantics |
| 0012 dependency discipline | Heavy zebra deps accepted behind a non-default feature |
| 0019 librustzcash pinning | Fulfilled: pre-release pins → finals (shared cohort with zebra) |
| 0020 windowed ingest | Unchanged; expected to shift the bottleneck from RPC transport to parse+commit |

## Delivery phases

1. **P0** — pin bump to the final librustzcash cohort (prereq, isolated commit).
2. **P1** — backend: mapping layer, wiring, config, feature gate, unit tests over a scripted service.
3. **P2** — parity verification: byte-compare compact blocks, treestates, subtrees, and address
   responses between backends against the live mainnet node over sampled ranges (incl. sandblasting).
4. **P3** — benchmarks: full sync and range ingests rpc vs readstate; treestate/subtree/address
   latency; serving-under-sync. Results into `contrib/bench/results/`.

## Measured results (2026-07)

P2 and P3 both ran against a live, fully-synced mainnet `zebrad 6.0.0`. Full reports:
[`contrib/bench/results/rss-parity-2026-07.md`](../../contrib/bench/results/rss-parity-2026-07.md) (P2)
and [`contrib/bench/results/rss-bench-2026-07.md`](../../contrib/bench/results/rss-bench-2026-07.md)
(P3).

**Parity (P2).** Compact blocks are byte-identical: **5,997 blocks fetched across three windows and
both pool-type modes, 100% byte-for-byte match**, plus clean passes on subtrees, the full address
surface (balance/txids/utxos, including the 10k-cap and set-order checks), `GetTransaction`, and error
parity. The initial sweep found two real, reproducible wire differences — an empty (not-yet-active)
commitment tree serialized as `""` (rpc) vs `"000000"` (readstate), and `upgradeName` rendered as
`"NU6.3"` (rpc) vs the Rust-enum-spelled `"Nu6_3"` (readstate) — both fixed (commit `3b51c6b`) and
re-verified: **80/80 individual checks pass** (63/80 on the first sweep, the remaining 17 confirmed
fixed on re-check).

**Benchmarks (P3).** Read surfaces win decisively: `GetTreeState` p50 **4.1x faster** (2.89ms → 0.71ms),
`GetTaddressTxids` up to **7.3x faster** on a full unbounded stream, time-to-tip on light recent blocks
**~25% faster**. Ingest is parse-bound and the picture inverts on heavy historical blocks: sandblasting-
era ingest is **~38% slower** than rpc, and a full genesis→tip sync lands **~19% slower overall**
(1h 38m06s vs rpc's 1h 22m30s) even though readstate wins 6 of 7 500k-height segments individually
(often 1.5-3.6x) — the one segment it loses (1.5M-2.0M sandblasting) dominates total wall time. Root
cause: the in-process path pays zebra's structured-`Block` deserialize plus a re-serialize plus the
compact-block parse on this process's cores, where the JSON-RPC path pipelines that work into zebrad's
own process. `GetSubtreeRoots` is the one read surface where readstate is slower (~40%), tracing to a
pre-existing N+1 fetch-per-entry pattern in `src/service/subtrees.rs` that is backend-independent and
is a separate, real latency bug worth fixing regardless of backend choice. Under full-speed concurrent
ingest, cached-read latency for readstate stays in the same band as the rpc/Go reference (p99 ~1.5ms) —
serving does not degrade while the ingestor runs flat out. One rare shutdown-path panic
(`JoinError::Cancelled` on a fetch task cancelled by SIGTERM teardown) was found and fixed in
`src/ingestor.rs` (commit `18cac7e`).

Operator guidance (unchanged from ADR 0023): `readstate` is the better steady-state/serving backend;
for the fastest cold sync, sync once with `--backend rpc` and restart with `--backend readstate` (the
on-disk cache is byte-identical between backends, so no re-sync is needed).
5. **P4** — docs (README, ARCHITECTURE, ADR 0023) and the PR (separate from, stacked on,
   `feat/ingest-performance-and-compliance`).
