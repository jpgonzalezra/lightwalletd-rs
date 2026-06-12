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

## Module layout

| Path | Responsibility | Phase |
|---|---|---|
| `proto/` + `build.rs` + `src/proto.rs` | The `.proto` contract and the `tonic`/`prost` generated code. | F0 |
| `src/config.rs` | Configuration: CLI flags + `zcash.conf` parsing. | F0 |
| `src/node/` | JSON-RPC client to `zebrad`: a generic `raw_request` plus typed wrappers. | F0 |
| `src/service.rs` | Implementation of the `CompactTxStreamer` gRPC service. | F0+ |
| `src/compact.rs` | Raw block bytes → `CompactBlock`, via `librustzcash`. | F1 |
| `src/cache.rs` | On-disk compact-block store (`redb`). | F2 |
| `src/ingestor.rs` | Background task that polls the node and fills the cache; reorg handling. | F2 |

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
cargo run -- --rpc-url http://127.0.0.1:18232 --rpc-user USER --rpc-password PASS
# or point at a zcash.conf:
cargo run -- --zcash-conf ~/.zcash/zcash.conf
```

The server listens on `--grpc-bind` (default `127.0.0.1:9067`). Probe it with `grpcurl`:

```sh
grpcurl -plaintext 127.0.0.1:9067 cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLightdInfo
grpcurl -plaintext 127.0.0.1:9067 cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLatestBlock
grpcurl -plaintext -d '{"height": 419200}' 127.0.0.1:9067 \
  cash.z.wallet.sdk.rpc.CompactTxStreamer/GetBlock
```

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

## Phase status

- **F0 — Skeleton**: done. The gRPC server serves `GetLightdInfo` (from `getinfo` + `getblockchaininfo`)
  and `GetLatestBlock` (from `getblockchaininfo`); the JSON-RPC client (`src/node`) and configuration
  (`src/config`) are in place.
- **F1 — Parser & GetBlock**: done. `src/compact.rs` parses raw blocks into `CompactBlock`s, and `GetBlock`
  serves a block by height (verbose `getblock` for hash + tree sizes, raw `getblock` for the bytes). Lookup
  by hash is not yet supported.
