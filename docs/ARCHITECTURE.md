# Architecture

This is a living document. It is updated at the end of every phase. It describes what `lightwalletd-rs` is, how
data flows through it, and the responsibility of each module. For the specifications each module implements, see
[`protocol-references.md`](protocol-references.md).

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
| `src/lib.rs` | Library root: module declarations, the `run` startup entrypoint, and the `darkside_components` constructor that wires the darkside stack (shared by `run` and the test harness). | P5 |
| `src/main.rs` | Binary wrapper: parses the CLI, initializes tracing, calls `run`. | P5 |
| `proto/` + `build.rs` + `src/proto.rs` | The `.proto` contract and the `tonic`/`prost` generated code (server and client). | P0 |
| `src/config.rs` | Configuration: CLI flags + `zcash.conf` parsing. | P0 |
| `src/node/` | JSON-RPC client to `zebrad`: the `NodeRpc` trait (typed RPC surface, with a generic `request` helper) and its `NodeClient` implementation. | P0 |
| `src/service/` | Implementation of the `CompactTxStreamer` gRPC service, split by method family (`chain`, `blocks`, `transactions`, `address`, `mempool`, `treestate`, `subtrees`, `ping`); `mod.rs` holds the `Streamer` and a thin trait impl that dispatches each method to its submodule. | P0+ |
| `src/compact.rs` | Raw block bytes → `CompactBlock`, via `librustzcash`. | P1 |
| `src/encoding.rs` | Display-order ↔ wire-order (endianness) conversions for hashes and txids. | P3 |
| `src/filter.rs` | Prune a compact block or transaction to the requested value pools (`poolTypes`). | P3 |
| `src/fetch.rs` | Fetch a block from the node and assemble its `CompactBlock` (shared by `GetBlock` and the ingestor). | P2 |
| `src/cache.rs` | On-disk compact-block store (`redb`). | P2 |
| `src/ingestor.rs` | Background task that polls the node and fills the cache; reorg handling. | P2 |
| `src/metrics.rs` | Serves Prometheus metrics over an HTTP `/metrics` endpoint. | P5 |
| `src/darkside/` | Darkside test harness, split by responsibility: `error` (error type), `block` (raw-block helpers and the held `ActiveBlock`), `state` (the in-memory mock chain `DarksideState`), `node` (its `NodeRpc` implementation `DarksideNode`), and `service` (the `DarksideStreamer` control plane). | P5 |

## Method classification

The 18 `CompactTxStreamer` methods split into two groups:

- **Easy proxies** (one RPC, translated; no cache, no parsing): `GetLatestBlock`, `GetLightdInfo`,
  `GetTransaction`, `SendTransaction`, `GetTaddressBalance(+Stream)`, `GetAddressUtxos(+Stream)`,
  `GetTreeState`/`GetLatestTreeState`, `Ping`.
- **Cache and/or parsing**: `GetBlock(Nullifiers)`, `GetBlockRange(Nullifiers)`, `GetMempoolTx`,
  `GetMempoolStream`, `GetSubtreeRoots`, `GetTaddressTransactions`/`GetTaddressTxids`.

### Node errors → gRPC status codes

`src/service/errors.rs` translates a backend JSON-RPC error into the gRPC `Status` a wallet expects,
decided per method family rather than collapsing every failure into `Unavailable`:

- height past the chain tip (`-8`) → `OutOfRange` for the block-serving methods;
- unknown transaction (`-5`) → `NotFound` for `GetTransaction` and the per-txid lookups;
- malformed transparent address (`-5`) → `InvalidArgument` for the address methods.

The match is on the numeric JSON-RPC code, not the message text, since error messages are not stable
across node versions. The same code `-5` is method-ambiguous (missing transaction vs. invalid address),
so each method family applies its own mapper. Anything unrecognized keeps the safe default:
`Unavailable` for a node/transport failure, `Internal` for a parse/decode failure.

### Input validation

Each method rejects malformed input with the appropriate `Status` before doing any work — a node
round-trip or opening a stream. For the streaming methods the check runs synchronously in the handler
before the stream is built, so the error surfaces as the RPC status rather than partway through the
stream.

- `GetBlock`, `GetBlockNullifiers`, `GetTreeState` reject an unspecified identifier (height `0` with
  an empty hash) with `InvalidArgument`. (Lookup by an explicit hash is still `Unimplemented`.)
- `GetBlockRange`, `GetBlockRangeNullifiers` require both `start` and `end`; a missing bound is
  `InvalidArgument` rather than silently defaulting to height `0`.
- `GetTransaction` requires a txid of exactly 32 bytes; an absent or wrong-length hash is
  `InvalidArgument`.
- `SendTransaction` rejects empty transaction data.
- `GetMempoolTx` rejects an exclude-suffix longer than 32 bytes and an invalid pool type
  (`PoolType::Invalid`) in the requested pools.
- The transparent-address methods validate the address shape locally — a `t` followed by 34
  alphanumeric characters — before reaching the node, and `GetTaddressTransactions`/`GetTaddressTxids`
  additionally require a block `range` with a `start` height.

The local address check is only a fast format gate: the node stays authoritative on the Base58Check
checksum, so a well-formed address can still be rejected by the node and mapped to `InvalidArgument`
(or `NotFound` for the `-5` "No information available" case) through the error translation above.

## Design decisions

Short ADRs live under [`docs/decisions/`](decisions/). Notable ones:

- **Local txid computation.** Transaction IDs (including v5 / Orchard
  [ZIP-244](protocol-references.md#transaction-format--identifiers)) are computed locally from the raw
  block bytes via `librustzcash`, so a single non-verbose `getblock` per block suffices for transaction data. A
  verbose call is still made to obtain the note-commitment tree sizes (`ChainMetadata`), which are not part of the
  raw block.
- **Shared mempool monitor (live).** A single background task (`src/service/mempool_monitor.rs`) refreshes the
  mempool at most once every 2 s and fans the deduplicated, parsed-once result out to all clients through a
  `tokio::sync::watch` snapshot, so node load is independent of the number of connected wallets: `GetMempoolTx`
  borrows the current snapshot and `GetMempoolStream` subscribes to it. Within a block interval the snapshot is
  append-only (each transaction is fetched and parsed once); a tip change resets it, and wallets see at most 2 s
  of staleness. The refresh is resilient to partial node failures: a transaction that disappears between the
  `getrawmempool` listing and its `getrawtransaction` fetch is logged and skipped, never dropping the rest of the
  tick. A `getrawmempool` failure (e.g. the node is down) aborts only that tick and retains the last good
  snapshot, so clients keep serving it until the node returns and refreshes resume on their own. Darkside keeps
  the per-request path (`Streamer.mempool == None`), where a staged transaction must appear and drain
  synchronously.

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

`redb` already provides page-level integrity (internal checksums) and transactional atomicity, so the cache
adds only the *logical* invariants on top of it. `add` is a strict append: it rejects a block whose own height
does not match its key, or a non-monotonic append, with a `CacheError::Corruption` rather than a panic or a
silent bad write. On open, `validate_light` runs an O(log n) check — it decodes the tip and verifies the height
range has no gaps (`len == last - first + 1`), touching only the first and last entries so the happy path stays
scan-free.

When a symptom is detected (open-time validation, or a decode error during ingestion), `lowest_corrupt_height`
localizes the corruption and the cache is truncated from that height with `reorg`, after which re-ingestion
refills it. Realistic corruption here is a contiguous suffix (an interrupted final write) or a schema-wide
decode failure visible at the tip, so localization matches that shape: a decode/height symptom walks down from
the tip (O(k), k ≈ 1), a gap is binary-searched (O(log n)). An isolated mid-cache corruption is out of scope —
`redb`'s page checksums and transactional, strict-append writes make it practically impossible.

The ingestor (`src/ingestor.rs`) runs as a background task. At startup it resolves the chain with
`connect_with_retry`: `getblockchaininfo` is retried indefinitely with capped exponential backoff (escalating
to `error!` logs after several attempts), so the server waits for a slow-to-start node instead of exiting. Each
step then reads the tip height **and** hash from a single `getblockchaininfo`. If the cache is behind, it
fetches the next block (verifying the returned block's height matches the one requested), checks that its
`prevHash` chains onto the cached tip, and appends it or — on a mismatch — rolls back one block. If the cache is
already at the tip height, it compares the tip *hash*: an equal hash means synced, a differing hash is an
in-place tip reorg and rolls back one block. A cache-corruption error truncates from the corrupt point and
retries immediately (bounded, so recovery can never spin), while node/transport errors back off. When the cache
reaches the tip it polls every couple of seconds. The cache persists across restarts, so the ingestor resumes
from where it left off.

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
- **P5 — Hardening**: in progress. TLS, Prometheus metrics, Docker, graceful shutdown, and per-method request
  input validation (rejecting malformed arguments up front, see [Input validation](#input-validation)) are in
  place, plus darkside mode (`--darkside-very-insecure`): a `DarksideStreamer` control plane over an in-memory mock chain
  served through the `NodeRpc` seam, for deterministic wallet tests. The crate is split into a library
  (`src/lib.rs`, exposing `run`) and a thin binary, with the gRPC client generated alongside the server, so it can
  be driven in-process by integration tests.
