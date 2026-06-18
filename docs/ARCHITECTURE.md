# Architecture

This is a living document. It is updated at the end of every phase. It describes what `lightwalletd-rs` is, how
data flows through it, and the responsibility of each module.

## Mental model

`lightwalletd-rs` is **not a node and not a wallet**. It is a **caching proxy** that sits between a Zcash full
node and light wallets:

```
            gRPC (CompactTxStreamer)            JSON-RPC (HTTP)
  wallet  <───────────────────────>  lightwalletd-rs  <───────────────────────>  zebrad (full node)
  (Zashi,                              - serves compact blocks                     - has the full chain
   Ywallet,                            - caches them on disk
   SDKs)                               - proxies the rest
```

It does three things:

1. **Ingest** — polls the node for new blocks, parses each raw block and converts it into a `CompactBlock`: a
   pruned form that drops the zk proofs and keeps only what a shielded wallet needs to detect payments/spends and
   update its note-commitment witnesses. This is the whole point: a block shrinks from ~2 MB to a few KB.
2. **Cache** — stores compact blocks on disk to serve them quickly and to handle chain reorgs.
3. **Serve gRPC** — implements the `CompactTxStreamer` service. It streams compact block ranges and **proxies**
   the remaining calls (send transaction, tree state, mempool, transparent-address balances) to the full node.

The gRPC contract is the standard Zcash light-client `.proto` set, so real wallets can talk to this server.

## Backend node

The backend is **`zebrad`**. The connection is plain HTTP `POST` JSON-RPC (no TLS) with HTTP Basic auth, reading
`rpcuser`/`rpcpassword` from flags or a `zcash.conf` file. Default ports: 8232 (mainnet), 18232 (testnet/regtest).

## Crate structure

The crate builds as both a library and a binary. `src/lib.rs` is the library root: it declares the modules and
exposes `run(config) -> anyhow::Result<()>`, the startup entrypoint that wires the gRPC server (TLS, the metrics
layer, the live or darkside service stack) and serves until shutdown. `src/main.rs` is a thin binary wrapper that
parses the CLI, initializes tracing, and calls `run`. Keeping the server in a library makes it embeddable and lets
integration tests link against the crate's API.

The `.proto` files are compiled with both server and client code generated, so the public `proto` module exposes
the `CompactTxStreamerClient` and `DarksideStreamerClient` stubs alongside the server traits.

## Module layout

| Path | Responsibility | Phase |
|---|---|---|
| `src/lib.rs` | Library root: module declarations and the `run` startup entrypoint. | P5 |
| `src/main.rs` | Binary wrapper: parses the CLI, initializes tracing, calls `run`. | P5 |
| `proto/` + `build.rs` + `src/proto.rs` | The `.proto` contract and the `tonic`/`prost` generated code (server and client). | P0 |
| `src/config.rs` | Configuration: CLI flags + `zcash.conf` parsing. | P0 |
| `src/node/` | JSON-RPC client to `zebrad`: the `NodeRpc` trait (typed RPC surface, with a generic `request` helper) and its `NodeClient` implementation. | P0 |
| `src/service.rs` | Implementation of the `CompactTxStreamer` gRPC service. | P0+ |
| `src/compact.rs` | Raw block bytes → `CompactBlock`, via `librustzcash`. | P1 |
| `src/encoding.rs` | Display-order ↔ wire-order (endianness) conversions for hashes and txids. | P3 |
| `src/filter.rs` | Prune a compact block or transaction to the requested value pools (`poolTypes`). | P3 |
| `src/fetch.rs` | Fetch a block from the node and assemble its `CompactBlock` (shared by `GetBlock` and the ingestor). | P2 |
| `src/cache.rs` | On-disk compact-block store (`redb`). | P2 |
| `src/ingestor.rs` | Background task that polls the node and fills the cache; reorg handling. | P2 |
| `src/metrics.rs` | Serves Prometheus metrics over an HTTP `/metrics` endpoint. | P5 |
| `src/darkside.rs` | Darkside test harness: the in-memory mock chain (`DarksideState`), its `NodeRpc` implementation (`DarksideNode`), and the `DarksideStreamer` control service. | P5 |

## Method classification

The 18 `CompactTxStreamer` methods split into two groups:

- **Easy proxies** (one RPC, translated; no cache, no parsing): `GetLatestBlock`, `GetLightdInfo`,
  `GetTransaction`, `SendTransaction`, `GetTaddressBalance(+Stream)`, `GetAddressUtxos(+Stream)`,
  `GetTreeState`/`GetLatestTreeState`, `Ping`.
- **Cache and/or parsing**: `GetBlock(Nullifiers)`, `GetBlockRange(Nullifiers)`, `GetMempoolTx`,
  `GetMempoolStream`, `GetSubtreeRoots`, `GetTaddressTransactions`/`GetTaddressTxids`.

## Design decisions

Short ADRs live under [`docs/decisions/`](decisions/). Notable ones:

- **Local txid computation.** Transaction IDs (including v5 / Orchard ZIP-244) are computed locally from the raw
  block bytes via `librustzcash`, so a single non-verbose `getblock` per block suffices for transaction data. A
  verbose call is still made to obtain the note-commitment tree sizes (`ChainMetadata`), which are not part of the
  raw block.

## Running

```sh
cargo run -- --rpc-url http://127.0.0.1:18232 --rpc-user USER --rpc-password PASS \
  --no-tls-very-insecure
# or point at a zcash.conf:
cargo run -- --zcash-conf ~/.zcash/zcash.conf --no-tls-very-insecure
```

On startup the server resolves the chain (which names the cache file under `--data-dir`) and the height to
start ingesting from (`--start-height`, defaulting to Sapling activation), then spawns the ingestor and serves
gRPC on `--grpc-bind` (default `127.0.0.1:9067`). For a quick plaintext run near the tip:

```sh
cargo run -- --rpc-url http://127.0.0.1:8232 --start-height 3375600 --data-dir /tmp/lwd-data \
  --no-tls-very-insecure
```

Probe it with `grpcurl` (plaintext, since the server above runs with `--no-tls-very-insecure`):

```sh
grpcurl -plaintext 127.0.0.1:9067 cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLightdInfo
grpcurl -plaintext 127.0.0.1:9067 cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLatestBlock
grpcurl -plaintext -d '{"height": 419200}' 127.0.0.1:9067 \
  cash.z.wallet.sdk.rpc.CompactTxStreamer/GetBlock
grpcurl -plaintext -d '{"start":{"height":3375690},"end":{"height":3375695}}' 127.0.0.1:9067 \
  cash.z.wallet.sdk.rpc.CompactTxStreamer/GetBlockRange
```

## TLS

The gRPC server runs over **TLS by default** and requires a PEM certificate and key:

```sh
cargo run -- --rpc-url http://127.0.0.1:8232 --tls-cert cert.pem --tls-key key.pem
```

Probe it over TLS with `grpcurl` (`-cacert` to trust the certificate):

```sh
grpcurl -cacert cert.pem 127.0.0.1:9067 cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLightdInfo
```

For local development only, `--no-tls-very-insecure` runs the server in plaintext (so `grpcurl -plaintext`
works) and logs a warning on startup. **This flag must never be used in production**: without TLS the
wallet↔server traffic is unencrypted and the server is not authenticated, which leaks query metadata and
allows impersonation. The `-very-insecure` suffix follows the upstream convention for dangerous flags.

This TLS protects the **wallet ↔ server** hop. The **server ↔ node** (`zebrad`) connection is plain HTTP on
purpose — it is local and never crosses the open network.

## Metrics

A `tower` layer on the gRPC server records per-method request counts, a latency histogram, and an in-flight
gauge automatically (no per-handler instrumentation). With `--metrics-bind <addr>` set, the metrics are served
in the Prometheus text format at `/metrics` on that address (a separate HTTP port from gRPC); without the flag,
metrics are off.

```sh
cargo run -- --rpc-url http://127.0.0.1:8232 --no-tls-very-insecure --metrics-bind 127.0.0.1:9100
curl http://127.0.0.1:9100/metrics
```

Notable series: `grpc_server_handled_total{grpc_service,grpc_method,grpc_code}` (request count by method and
gRPC status) and `grpc_server_handling_seconds` (latency histogram). The registry is empty until the first gRPC
request, so `/metrics` returns nothing until there has been some traffic.

## Darkside mode

Darkside mode replaces the real node with a controllable, in-memory mock chain, so wallet behaviour can be
exercised deterministically — reorgs, confirmations, and edge cases are scripted by the test rather than
waited for on a live chain. It is enabled with `--darkside-very-insecure` and must never be used in
production. Two gRPC services are served on the same port:

- `CompactTxStreamer` — the normal wallet-facing service, unchanged. The wallet does not know its data is mock.
- `DarksideStreamer` — a control plane the test drives to fabricate the chain.

### How it works

The injection point is the `NodeRpc` seam. In darkside mode the service is built over a `DarksideNode`, a
`NodeRpc` implementation backed by a `Mutex<DarksideState>` (the in-memory chain) in place of the JSON-RPC
`NodeClient`. The cache, the block-serving methods, and the `CompactTxStreamer` implementation are reused
unchanged: the ingestor is not spawned and the cache stays empty, so every block read falls back to the mock
node. The `DarksideStreamer` service shares the same `DarksideState` and mutates it.

State is built with a stage-then-apply model:

- `StageBlocks*` / `StageTransactions*` fill a staging area; nothing is presented yet. `StageBlocksCreate`
  manufactures synthetic empty blocks at consecutive heights.
- `ApplyStaged(height)` merges staged blocks into the active chain (rewriting from the staged block's height,
  which is how a reorg is produced), mines staged transactions into their block by height, re-chains each
  block's previous-hash field, sets the presented tip, and clears the staging area.

Each active block tracks its accumulated Sapling and Orchard commitment-tree sizes, carried forward from the
sizes set by `Reset`; mining a transaction grows the sizes of its block and every later block. Blocks are
held split as `(header, [tx_bytes])` and re-serialized on demand, so a mined transaction is appended without
rewriting length prefixes by hand.

The state keeps three transaction pools, each surfaced by a different RPC:

- The **staging area** (staged blocks plus staged transactions) is the mempool. `GetMempoolTx` lists every
  staged transaction and every transaction of a staged block; `GetMempoolStream` emits only the loose staged
  transactions, which are reported at height 0 (a staged block's transactions carry that block's height).
  `ApplyStaged` drains the staging area into the active chain, so a staged transaction leaves the mempool once
  it is mined.
- The **active chain** is the mined history, served by `GetBlock`/`GetBlockRange`.
- The **`SendTransaction` pool** captures transactions submitted through the production RPC and is read back
  only by `GetIncomingTransactions`; it does not enter the mempool.

### The GetSubtreeRoots exception

Every wallet-facing read is served from `DarksideState` through the `NodeRpc` seam, with one exception:
`GetSubtreeRoots` derives its response from the completing block, which the mock has no good way to fake. So
in darkside mode the subtree roots are staged complete — with their completing block hash and height already
set — via `SetSubtreeRoots`, and served verbatim. This is the only point in `CompactTxStreamer` that is
darkside-aware, reached through an optional handle to the shared state that is `None` on the live path.

### Known limitations

- The chain name and a tree state's `network` field come from `Reset`/config rather than being honoured
  per staged tree state; this is sufficient for the standard "main" test vectors.
- The URL-based staging RPCs (`StageBlocks`/`StageTransactions`) fetch from the given URL with the server's
  HTTP client, which is built without TLS (the backend node is plain HTTP), so they can only fetch over
  `http://`. For remote data served over `https://` — such as the upstream `basic-reorg` test vectors on
  `raw.githubusercontent.com` — fetch it client-side and push it in through the streaming RPCs
  (`StageBlocksStream`/`StageTransactionsStream`), as `contrib/smoke-test.sh` does.

## Block parsing

`src/compact.rs` turns a raw block into a `CompactBlock`. The header is parsed by hand (fixed layout) to
recover the block hash (double SHA-256, little-endian), previous hash, and time; each transaction is parsed
with `librustzcash`, which also yields the correct transaction ID for both legacy and v5 (ZIP-244)
transactions. The compact form keeps only what a shielded wallet needs — Sapling spends/outputs, Orchard
actions, and transparent inputs/outputs — and the block height is read from the coinbase (BIP34).

The note-commitment tree sizes in `ChainMetadata` are not part of the raw block; `GetBlock` fills them in
from the verbose `getblock` response.

The parser is validated byte-for-byte against the golden fixtures in `testdata/compact_blocks.json` (the
reference fixtures carry zeroed txids, so the test normalizes ours before comparing the rest of the
structure, and asserts that a real txid is computed for every transaction).

## Cache and ingestor

The cache (`src/cache.rs`) is a `redb` table keyed by height; each value is a protobuf-encoded `CompactBlock`.
Because the keys are ordered, the tip is cheap to read and a reorg is just "drop every height above N".

The ingestor (`src/ingestor.rs`) runs as a background task. Each step asks the node for the tip height; if the
cache is behind, it fetches the next block, checks that its `prevHash` chains onto the cached tip, and either
appends it or — on a mismatch — rolls back one block and retries. When the cache reaches the tip it polls every
couple of seconds. The cache persists across restarts, so the ingestor resumes from where it left off.

`GetBlock` and `GetBlockRange` read from the cache and fall back to the node on a miss. `GetBlockRange` streams
the range (ascending if `start <= end`, otherwise descending) and prunes each block to the requested
`poolTypes` — an empty list means the legacy default of shielded-only data (transparent inputs/outputs
stripped).

## Testing

Unit tests run against a fake node rather than a live `zebrad`. The `NodeRpc` trait (`src/node/`) is the
seam: `service`, `ingestor`, and `fetch` depend on `Arc<dyn NodeRpc>`, so a test injects a `FakeNode`
(`src/testutil.rs`) with canned responses to characterize the RPC↔gRPC translation, reorg handling, and
block assembly. The `NodeClient` HTTP/JSON layer itself is covered separately with a `wiremock` mock
server, and the parser is pinned byte-for-byte by the golden fixtures in `testdata/`. Darkside reuses the
same seam — its stage/apply engine and `DarksideNode` reads are unit-tested directly, and the end-to-end
path is checked by driving the real `Streamer` (`GetBlockRange`, `GetSubtreeRoots`, `GetMempoolTx`) against a
`DarksideNode` with an empty cache.

## Phase status

- **P0 — Skeleton**: done. The gRPC server serves `GetLightdInfo` (from `getinfo` + `getblockchaininfo`)
  and `GetLatestBlock` (from `getblockchaininfo`); the JSON-RPC client (`src/node`) and configuration
  (`src/config`) are in place.
- **P1 — Parser & GetBlock**: done. `src/compact.rs` parses raw blocks into `CompactBlock`s, and `GetBlock`
  serves a block by height (verbose `getblock` for hash + tree sizes, raw `getblock` for the bytes). Lookup
  by hash is not yet supported.
- **P2 — Cache, ingestor & GetBlockRange**: done. A `redb`-backed cache (`src/cache.rs`) is filled by a
  background ingestor (`src/ingestor.rs`); `GetBlock` and `GetBlockRange` serve from it (falling back to the
  node), and `GetBlockRange` streams with `poolTypes` filtering.
- **P3 — Proxies**: done. `GetTransaction`, `SendTransaction`, `GetTreeState`/`GetLatestTreeState`,
  `GetTaddressBalance(+Stream)`, `GetAddressUtxos(+Stream)`, and `Ping` translate a single node RPC each.
- **P4 — Mempool, subtrees, t-addr txns & nullifiers**: done. `GetBlockNullifiers`/`GetBlockRangeNullifiers`
  (pruned to shielded nullifiers), `GetTaddressTxids`/`GetTaddressTransactions`, `GetSubtreeRoots`
  (`z_getsubtreesbyindex` + the completing block from the cache), `GetMempoolTx`, and `GetMempoolStream` (a
  poll loop that ends when a new block is mined). All `CompactTxStreamer` methods are now implemented.
- **P5 — Hardening**: in progress. TLS, Prometheus metrics, Docker, and graceful shutdown are in place, plus
  darkside mode (`--darkside-very-insecure`): a `DarksideStreamer` control plane over an in-memory mock chain
  served through the `NodeRpc` seam, for deterministic wallet tests. The crate is split into a library
  (`src/lib.rs`, exposing `run`) and a thin binary, with the gRPC client generated alongside the server, so it can
  be driven in-process by integration tests.
