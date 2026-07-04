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

## Performance

A reproducible harness in [`contrib/bench/`](contrib/bench/) measures the hot read-path — serving
compact blocks from a warm cache with the node idle — for both this implementation and the reference Go
[`lightwalletd`](https://github.com/zcash/lightwalletd), under identical resource limits. The comparison
here is a deliberate, method-first exception to this project's usual no-comparison stance; the
methodology is recorded in [ADR 0017](docs/decisions/0017-benchmark-methodology.md).

**Environment disclaimer.** Numbers are produced under Docker Desktop on macOS arm64, both proxies
capped at 2 vCPU / 2 GiB. VM overhead degrades absolute figures, but since both pay it equally under the
same limits, the **relative** comparison holds — read the results as relative, not absolute. Pinned
versions: Rust `b992f48`, Go `fdf1af5`, `ghz` `v0.121.0`; base images `rust:1-slim`, `golang:1.25`,
`debian:bookworm-slim`, `python:3-slim`.

Two mainnet profiles are measured: **dense** (post-NU5, blocks 3350000–3361999, with Sapling/Orchard
activity) and **light** (pre-Sapling, blocks 20000–31999, **no shielded content**). Both proxies serve
identical compact blocks (see the fairness note below), so this is a like-for-like comparison. Numbers
below are the **median over 5 reps** (warm-up discarded) at each concurrency, from a full sweep (July
2026, generated by `scripts/aggregate.py`).

At a glance, on both profiles the Rust implementation sustains **higher `GetBlockRange` throughput**
(≈1.2–3× on `dense`, ≈1.9–3.9× on `light`), **lower and far more stable tail latency**, and **smaller
peak RSS**; Go's throughput plateaus by concurrency 2 while Rust keeps scaling until it saturates the
2-vCPU cap.

### dense (post-NU5)

`GetBlock` latency — median p50 / p99 (µs):

| concurrency | rust p50 | rust p99 | go p50 | go p99 |
|---|---|---|---|---|
| 1 | 164 | 224 | 166 | 265 |
| 2 | 185 | 339 | 175 | 644 |
| 4 | 206 | 445 | 230 | 1532 |
| 8 | 226 | 1019 | 280 | 2164 |
| 16 | 278 | 1418 | 363 | 3506 |
| 32 | 474 | 2011 | 612 | 17055 |
| 64 | 886 | 3475 | 1094 | 26818 |

`GetBlockRange` throughput — median blocks/s, each request streaming a range of 1,000 blocks (W = 1000):

| concurrency | rust | go |
|---|---|---|
| 1 | 66,617 | 53,865 |
| 2 | 128,400 | 86,869 |
| 4 | 205,166 | 86,758 |
| 8 | 246,585 | 84,350 |
| 16 | 248,744 | 85,964 |
| 32 | 270,301 | 91,044 |
| 64 | 271,884 | 93,726 |

| impl | peak RSS (MiB) | cache on disk (MiB) | max CPU (cores) |
|---|---|---|---|
| rust | 73.8 | 26.9 | 1.99 |
| go | 179.9 | 19.5 | 2.01 |

### light (pre-Sapling)

Because pre-Sapling blocks carry no Sapling/Orchard data, `GetBlockRange` (shielded-only by default)
serves near-empty blocks here — this profile measures the framing/overhead floor of the wallet-sync path.
The full blocks still carry heavy transparent transaction data (many `vin`/`vout` plus per-transaction
overhead), so **`GetBlock` (which returns the full compact block) and the on-disk footprint are larger
than on `dense`**, not smaller.

`GetBlock` latency — median p50 / p99 (µs):

| concurrency | rust p50 | rust p99 | go p50 | go p99 |
|---|---|---|---|---|
| 1 | 473 | 576 | 502 | 978 |
| 2 | 502 | 934 | 538 | 1469 |
| 4 | 547 | 1680 | 637 | 1976 |
| 8 | 653 | 2240 | 927 | 3130 |
| 16 | 1098 | 3987 | 1601 | 5204 |
| 32 | 1914 | 6461 | 2844 | 12780 |
| 64 | 3304 | 12062 | 5056 | 23310 |

`GetBlockRange` throughput (W = 1000) — median blocks/s:

| concurrency | rust | go |
|---|---|---|
| 1 | 36,735 | 14,375 |
| 2 | 42,718 | 22,251 |
| 4 | 77,568 | 22,622 |
| 8 | 78,495 | 20,374 |
| 16 | 77,994 | 20,999 |
| 32 | 80,508 | 22,259 |
| 64 | 84,713 | 24,274 |

| impl | peak RSS (MiB) | cache on disk (MiB) | max CPU (cores) |
|---|---|---|---|
| rust | 184.8 | 249.0 | 2.09 |
| go | 244.2 | 109.2 | 2.02 |

### Notes

- **Fairness (identical blocks).** `populate.sh` verifies this on every run and refuses to proceed on a
  mismatch: the `GetBlockRange` stream over each full range — all 24,000 blocks across the two profiles —
  hashes identically between Rust and Go (content identity: the responses decode to the same messages,
  not a wire-byte claim). The unary `GetBlock` path was additionally spot-checked at sampled heights.
- **Dual source of truth.** For the unary `GetBlock`, the server-side `grpc_server_handling_seconds`
  histogram (handler time ≈10 µs rust vs ≈25 µs go on dense) corroborates the client-side ordering; the
  ~150 µs client-side floor is the HTTP/2 + container round-trip. For streaming `GetBlockRange` the two
  servers time the handler differently (tonic returns the stream lazily, so it records only stream setup;
  `grpc_prometheus` times the full drain), so the client-side throughput above — where `ghz` drains the
  whole stream identically for both — is the comparable measure.
- **Saturation.** Both proxies reach their 2-vCPU cap at high concurrency (measured, ~2.0 cores; the
  cgroup CPU accounting reads a few percent over the cap — e.g. 2.09 — from sampling jitter), so the
  upper curve is a saturation regime, not linear scaling.
- **Cache on disk.** Measured after population. The Rust `redb` cache is larger than Go's flat
  append-only files (B-tree overhead), most visibly on the transparent-heavy `light` profile. Under
  sustained concurrent read load `redb` grows further and does not shrink (dense: ~28→42 MiB over the
  sweep), while Go's append-only files hold steady.
- **Fidelity.** 0.05% of requests are cancelled at the `ghz -z` duration boundary; the smaller
  `GetBlockRange` window (W = 100) is noisier (±~30%) than W = 1000 / 10000. The harness records the full
  curve (W ∈ {100, 1000, 10000}); reproduce with `run-bench.sh` + `aggregate.py`.

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
