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

## Phase status

- **F0 — Skeleton**: in progress. gRPC server + JSON-RPC client, `GetLightdInfo` + `GetLatestBlock`.
