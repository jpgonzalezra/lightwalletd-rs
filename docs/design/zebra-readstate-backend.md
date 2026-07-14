# Design: Zebra ReadStateService backend

**Status:** accepted (ADR [0023](../decisions/0023-zebra-readstate-backend.md)) · **Date:** 2026-07-14
**Research base:** zebra v6.0.0 source (`zebra-state 10.1.0`, `zebra-rpc 11.1.0`), lightwalletd-rs @ `0f75316`.

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
5. **P4** — docs (README, ARCHITECTURE, ADR 0023) and the PR (separate from, stacked on,
   `feat/ingest-performance-and-compliance`).
