# Hot read-path benchmark harness

A reproducible harness that compares the **hot read-path** of `lightwalletd-rs`
against the reference lightwalletd (Go): serving compact blocks from a warm
on-disk cache with the backend node idle. It measures the pure proxy — read the
cache, deserialize protobuf, re-serialize to the wire, HTTP/2 framing — and
nothing that touches the node.

Everything runs in Docker Compose; nothing is installed on the host. Results feed
the *Performance* section of the top-level README and an accompanying methodology
ADR.

## What it measures

- **Latency** — `GetBlock` (unary), p50/p90/p99/max.
- **Throughput** — `GetBlockRange` (server-streaming), blocks/s and MB/s across a
  concurrency curve, with an empty `poolTypes` (default shielded-only).
- **Footprint** — on-disk cache size per implementation and profile.
- Peak RSS, CPU%, and a server-side latency histogram scraped from `/metrics`.

Out of scope: cold-sync / ingestion time, the passthrough proxies
(`GetTransaction`, `GetTreeState`, subtrees, mempool, t-address — they touch the
node), and TLS (both run plaintext to isolate the proxy).

## Requirements

- Docker Desktop (this harness is developed on macOS arm64).
- A synced `zebrad` reachable over JSON-RPC — used **once**, only to extract the
  dataset. It never participates in a measurement.
- The `ghz` gRPC load client runs as a container on the internal Compose network
  (no host port-forwarding), so it needs no host install.

## Layout

```
contrib/bench/
  docker-compose.bench.yml   # rust-lwd, go-lwd, mock-rpc, ghz on an internal network
  go-lwd.Dockerfile          # clones zcash/lightwalletd @ fdf1af5 and builds
  mock-rpc/{Dockerfile,server.py}
  profiles/{dense.env,light.env}
  scripts/{extract-dataset.sh,populate.sh,run-bench.sh,aggregate.py,plot.py}
  charts/                    # SVG charts committed for the README
  data/                      # extracted dataset artifacts (git-ignored)
  results/                   # per-run outputs (git-ignored)
```

## Profiles

Two mainnet profiles capture different block shapes; each has its own dataset and
its own pair of cache volumes (the Rust cache requires a contiguous height range,
so the two profiles cannot share one cache):

- **dense** — post-NU5, active Orchard/Sapling.
- **light** — pre-Sapling, near-empty compact blocks.

A profile is selected by passing its env file to Compose, which sets `BASE`,
`SPAN`, and the per-profile cache volume names:

```
docker compose -f docker-compose.bench.yml --env-file profiles/dense.env <cmd>
```

## Flow

1. **Extract** the dataset once from your `zebrad` (writes `data/<profile>/`):

   ```
   RPC_URL=http://127.0.0.1:8232 RPC_USER=… RPC_PASSWORD=… \
     scripts/extract-dataset.sh dense
   ```

2. **Populate** each proxy's cache from the mock (leaves the volumes warm):

   ```
   scripts/populate.sh dense
   ```

3. **Run** the load sweep (per implementation in series, never in parallel):

   ```
   scripts/run-bench.sh dense
   ```

4. **Aggregate** the runs into tables, and render the charts:

   ```
   scripts/aggregate.py results/     # Markdown tables
   scripts/plot.py                   # SVG charts -> charts/
   ```

## How the node is neutralized

The dataset is frozen once into each proxy's cache. During measurement the
`mock-rpc` service stays idle, reporting `tip = last dataset height`, so each
proxy's ingestor sees itself synced and only issues a poll every ~2 s without
fetching anything. Every `ghz` request is served 100% from cache. This poll
overhead is identical for both proxies and reflects steady-state production.

Stock lightwalletd (Go) always anchors its cache at genesis and has no flag to
start ingestion at an arbitrary height. To populate a fixed height window without
syncing from block 0, `go-lwd.Dockerfile` applies a minimal build-time patch that
lets the ingestor start at `BASE` (via `LWD_FIRST_HEIGHT`). The patch changes only
where ingestion begins; the measured read path is untouched. This is a conscious,
documented deviation — see the methodology ADR.

## Environment disclaimer

Numbers are produced under Docker Desktop on macOS arm64, with both proxies capped
at **2 vCPU / 2 GiB**. The VM overhead degrades **absolute** figures, but since
both proxies pay the same overhead under identical limits, the **relative**
comparison holds. Treat the results as relative, not absolute. Pinned versions
(Go commit, Rust commit, base images, `ghz`) are recorded in the report.

## Fidelity controls

- Runs are serial, never Go and Rust at once (they share the VM).
- Identical requests, limits (2 vCPU / 2 GiB), network, plaintext, and `warn`
  logging on both.
- `ghz` runs on the Compose network and is monitored so it never becomes the
  bottleneck.
- Warm-up pass discarded; N reps; median + p99 + spread reported, not the mean.
- Fairness: `populate.sh` hashes each proxy's `GetBlockRange` stream over the full
  range and refuses to proceed unless they match, so the load compares identical
  blocks.
- Dual source of truth: client-side (`ghz`) and server-side
  (`grpc_server_handling_seconds`).
