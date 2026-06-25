# lightwalletd-rs

A Rust implementation of [`lightwalletd`](https://github.com/zcash/lightwalletd), the gRPC backend that serves
Zcash blockchain data to shielded light wallets (Zashi, Ywallet, the mobile SDKs).

## What lightwalletd is

It is neither a node nor a wallet: it is a **caching proxy** between a full node
([`zebrad`](https://github.com/ZcashFoundation/zebra)) and wallets. It pulls blocks from the node, converts them
into `CompactBlock`s (a pruned form with the zk proofs stripped out) and streams them over gRPC. See
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md). For the Zcash and Bitcoin specifications it implements, see
[`docs/protocol-references.md`](docs/protocol-references.md).

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

### Donation address

`--donation-address u1...` advertises a Zcash unified address in `GetLightdInfo`. Wallets read it to
offer the user the option of donating to whoever operates this server; it is advisory only and carries
no payment logic. The address is decoded at startup, so a malformed or truncated one fails fast rather
than being served.

### Darkside mode

`--darkside-very-insecure` serves a controllable, in-memory mock chain instead of proxying a real node, for
deterministic wallet tests (reorgs, confirmations, edge cases). It exposes a `DarksideStreamer` control plane
alongside the normal `CompactTxStreamer`. This flag is for testing only — never use it in production. See
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md#darkside-mode).

## Testing

`make test` runs the unit tests and a suite of deterministic end-to-end tests (`tests/`) that drive a
real in-process darkside server over gRPC with vendored, network-free data — these run in CI.

`contrib/smoke-test.sh` is a manual, optional check that drives a live darkside binary with `grpcurl`
and `jq` against the `basic-reorg` vector. It requires `grpcurl` and `jq` and downloads data from the
internet, so it is not run in CI.

## Docker

```sh
docker build -t lightwalletd-rs .
docker compose up   # a zebra node + lightwalletd-rs
```

`docker-compose.yml` serves over TLS from a certificate mounted at `./certs` (see the comments in that file
for how to provide one). The zebra node syncs the chain on first run, which takes hours and a large volume.

## License

MIT (same as upstream).
