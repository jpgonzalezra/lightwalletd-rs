# lightwalletd-rs

[![CI](https://github.com/jpgonzalezra/lightwalletd-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/jpgonzalezra/lightwalletd-rs/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A Rust lightwalletd for Zcash: a caching proxy that serves compact blockchain data to shielded light
wallets over gRPC.

> **Beta software.** lightwalletd-rs is under active development and has not been security-audited.
> Expect breaking changes, and run it at your own risk — it is provided "as is", without warranty of
> any kind (see [LICENSE](LICENSE)).

## Overview

`lightwalletd-rs` is neither a node nor a wallet. It is a **caching proxy** between a Zcash full node
([`zebrad`](https://github.com/ZcashFoundation/zebra)) and light wallets:

```
            gRPC (CompactTxStreamer)            JSON-RPC (HTTP)
  wallet  <───────────────────────>  lightwalletd-rs  <───────────────────────>  zebrad (full node)
  (Zcash                               - serves compact blocks                     - has the full chain
   light                               - caches them on disk
   wallets)                            - proxies the rest
```

It ingests blocks from the node and converts each into a `CompactBlock` — a pruned form with the zk proofs
stripped, so a block shrinks from ~2 MB to a few KB — caches them on disk, and streams them to wallets over
the standard Zcash light-client gRPC. The remaining calls (send transaction, tree state, mempool,
transparent-address balances) are proxied to the node.

For the full design see [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md); for the specifications it
implements, [`docs/protocol-references.md`](docs/protocol-references.md).

## Features

- **All 20 `CompactTxStreamer` methods** — blocks, transactions, tree state, subtrees, nullifiers,
  transparent-address balances and txids, and mempool streaming.
- **On-disk compact-block cache** (`redb`) filled by a background ingestor, with reorg rollback and
  automatic recovery from corruption or gaps.
- **TLS by default** — plaintext requires an explicit opt-in flag.
- **Prometheus metrics** — per-method request counts and latency histograms.
- **Hardened by default** — up-front input validation, per-connection stream and keepalive limits, and a
  graceful drain on `SIGINT`/`SIGTERM`.
- **Darkside test mode** — a controllable in-memory mock chain for deterministic wallet tests.

## Requirements

- **Rust** (stable, 2024 edition).
- **`protoc`**, the Protocol Buffers compiler, on `PATH` — the `.proto` contract is compiled at build time.
- A reachable **`zebrad`** node with JSON-RPC enabled (mainnet, testnet, or regtest), synced far enough to
  serve the range you need.

## Quickstart

Build the binary:

```sh
cargo build --release      # or: make build
```

Run it against a local `zebrad`, in plaintext (local development only):

```sh
./target/release/lightwalletd-rs \
  --rpc-url http://127.0.0.1:8232 \
  --rpc-user "$RPC_USER" --rpc-password "$RPC_PASSWORD" \
  --grpc-bind 127.0.0.1:9067 \
  --no-tls-very-insecure
```

On first start it ingests from Sapling activation (or `--start-height`) and fills the on-disk cache under
`--data-dir`; later starts resume from the cache. Once it is serving, point a wallet at
`127.0.0.1:9067` — it speaks the standard Zcash light-client gRPC, so Zcash light wallets connect
unchanged.

For anything beyond local testing, serve over [TLS](#tls) instead of `--no-tls-very-insecure`.

## Configuration

The proxy needs two things: how to reach the node, and where to listen.

**Backend node.** Point it at `zebrad`'s JSON-RPC with `--rpc-url`, or with `--rpc-host` / `--rpc-port`
(defaults `127.0.0.1:8232`). Credentials come from `--rpc-user` / `--rpc-password`, or from a `zcash.conf`
via `--zcash-conf` (which reads `rpcuser` / `rpcpassword` / `rpcbind` / `rpcport`). Flags take precedence
over the file.

| Flag | Default | Purpose |
|---|---|---|
| `--grpc-bind` | `127.0.0.1:9067` | gRPC listen address |
| `--rpc-url` | — | full JSON-RPC URL of the node (overrides `--rpc-host`/`--rpc-port`) |
| `--rpc-user` / `--rpc-password` | — | node RPC credentials (or via `--zcash-conf`) |
| `--zcash-conf` | — | read credentials and host/port from a `zcash.conf` |
| `--data-dir` | `./lightwalletd-rs-data` | directory for the on-disk block cache |
| `--start-height` | Sapling activation | height to ingest from when the cache is empty |
| `--tls-cert` / `--tls-key` | — | PEM certificate / key (required unless `--no-tls-very-insecure`) |
| `--metrics-bind` | — | address to serve Prometheus `/metrics` on (disabled if unset) |

Run `lightwalletd-rs --help` for the full list, including cache resync (`--sync-from-height`,
`--redownload`) and per-connection resource limits (`--max-concurrent-streams`, `--keepalive-*`).

## TLS

The gRPC server runs over TLS by default: `--tls-cert` and `--tls-key` are required unless you pass
`--no-tls-very-insecure` (plaintext — development only, never in production). For local testing you can
generate a self-signed pair:

```sh
openssl req -x509 -newkey rsa:4096 -nodes -keyout key.pem -out cert.pem -days 365 \
  -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
```

See [`docs/ARCHITECTURE.md#tls`](docs/ARCHITECTURE.md#tls) for details.

## Observability

Set `--metrics-bind` to expose Prometheus metrics — per-method request counts and latency histograms — on
`/metrics`:

```sh
lightwalletd-rs ... --metrics-bind 127.0.0.1:9100
```

Metrics are disabled when the flag is unset. See [`docs/ARCHITECTURE.md#metrics`](docs/ARCHITECTURE.md#metrics).

## Docker

```sh
docker build -t lightwalletd-rs .
docker compose up        # a zebra node + lightwalletd-rs
```

`docker-compose.yml` brings up a `zebra` node and the proxy in front of it, serving over TLS from a
certificate mounted at `./certs` (see the comments in that file). The node syncs the chain on first run,
which takes hours and tens to hundreds of GB.

## Development

```sh
make build      # compile
make test       # unit + end-to-end tests
make lint       # clippy -D warnings
make fmt        # check formatting
make verify     # fmt + lint + build + test (the pre-commit check)
```

`make test` runs the unit tests and a suite of deterministic end-to-end tests (`tests/`) that drive an
in-process darkside server over gRPC with vendored, network-free data. `contrib/smoke-test.sh` is an
optional manual check that drives a live darkside binary with `grpcurl` and `jq`; it downloads data from
the internet, so it is not run in CI.

## Advanced

### Darkside mode

`--darkside-very-insecure` serves a controllable, in-memory mock chain instead of proxying a real node, for
deterministic wallet tests (reorgs, confirmations, edge cases). It exposes a `DarksideStreamer` control
plane alongside the normal `CompactTxStreamer`. Testing only — never use it in production. See
[`docs/ARCHITECTURE.md#darkside-mode`](docs/ARCHITECTURE.md#darkside-mode).

### Donation address

`--donation-address u1...` advertises a Zcash unified address in `GetLightdInfo`. Wallets read it to offer
users the option of donating to whoever operates the server; it is advisory only and carries no payment
logic. The address is decoded at startup, so a malformed or truncated one fails fast rather than being
served.

### Ping

`--ping-very-insecure` enables the `Ping` gRPC, a benchmark/testing call. It is off by default: a client
controls both the sleep duration and the concurrency it observes, so leaving it open is a needless
denial-of-service surface.

## Documentation

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — what it is, how data flows, and the responsibility of
  each module.
- [`docs/decisions/`](docs/decisions/README.md) — architecture decision records: the *why* behind the design.
- [`docs/protocol-references.md`](docs/protocol-references.md) — the ZIPs, BIPs, and spec sections each
  module implements.
- [`CHANGELOG.md`](CHANGELOG.md) — release notes.

## Acknowledgments

lightwalletd-rs is inspired by and indebted to the original Go
[`lightwalletd`](https://github.com/zcash/lightwalletd). Its protocol, behavior, and years of accumulated
design decisions were the reference this implementation followed — this project would not have been
possible without it. Thanks to the Zcash community that built and maintains it.

## License

Licensed under the [MIT License](LICENSE).
