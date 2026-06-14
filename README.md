# lightwalletd-rs

A Rust implementation of [`lightwalletd`](https://github.com/zcash/lightwalletd), the gRPC backend that serves
Zcash blockchain data to shielded light wallets (Zashi, Ywallet, the mobile SDKs).

## What lightwalletd is

It is neither a node nor a wallet: it is a **caching proxy** between a full node
([`zebrad`](https://github.com/ZcashFoundation/zebra)) and wallets. It pulls blocks from the node, converts them
into `CompactBlock`s (a pruned form with the zk proofs stripped out) and streams them over gRPC. See
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Status

Under active development, built in vertical slices.

- [x] P0 — Skeleton: gRPC server + JSON-RPC client, `GetLightdInfo` + `GetLatestBlock`
- [x] P1 — Parser → `CompactBlock` → `GetBlock`
- [x] P2 — Cache + ingestor + `GetBlockRange`
- [x] P3 — Proxies (send, tx, balance, utxos, treestate)
- [x] P4 — Mempool, subtrees, t-addr txns, nullifiers
- [ ] P5 — Hardening (TLS, metrics, darkside, Docker)

## Requirements

- Stable Rust (2024 edition).
- `protoc` (the protobuf compiler) on `PATH`, to compile the `.proto` files.
- A running `zebrad` (testnet/regtest) to test against a real node.

## Usage

```sh
make build      # compile
make test       # run tests
make lint       # clippy -D warnings
make fmt        # check formatting
make run -- --help
```

The gRPC server runs over TLS by default (`--tls-cert` / `--tls-key`). For local development,
`--no-tls-very-insecure` runs it in plaintext — never use that flag in production. See
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md#tls).

## License

MIT (same as upstream).
