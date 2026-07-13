# 0017. Benchmark the hot read-path against the reference implementation

## Context

We want a defensible, reproducible measurement of the proxy's steady-state read
path — reading a compact block from the warm on-disk cache, deserializing it,
re-serializing it to the wire, and framing it over HTTP/2 — and how it compares to
the Go reference lightwalletd. A naive benchmark is easy to get wrong: the backend
node dominates timings, cold caches measure disk I/O instead of the proxy, TLS and
log verbosity confound the two sides, and Docker Desktop on macOS adds VM overhead
to every absolute number. The two implementations also cache different height
windows for the same block shape, and the Go implementation anchors its cache at
genesis with no flag to start ingestion elsewhere.

## Decision

The harness lives in [`contrib/bench/`](../../contrib/bench) and fixes the method:

- **Hot read-path only.** A dataset is frozen once into each proxy's cache; during
  measurement a `mock-rpc` service stays idle, reporting the dataset tip so each
  ingestor sees itself synced and only idle-polls. Every request is served from
  cache. Cold-sync and the passthrough proxies (which touch the node) are out of
  scope.
- **Two profiles, separate caches.** `dense` (post-NU5, active shielded pools) and
  `light` (pre-Sapling, near-empty blocks) each get their own dataset and cache
  volumes, because the contiguous-range cache cannot hold two disjoint windows.
- **Matched conditions.** Both proxies run plaintext, at `warn` logging, under
  identical limits (2 vCPU / 2 GiB), on one internal Docker network, with the load
  client (`ghz`) as a native-architecture container on that network (no host
  port-forwarding). Runs are strictly serial, never both at once.
- **Dual source of truth.** Client-side latency/throughput from `ghz` is
  cross-checked against each proxy's server-side `grpc_server_handling_seconds`
  histogram; a warm-up pass is discarded and N reps are summarized as median + p99
  + spread, not a lone mean.
- **Go start height.** Because the reference implementation only ingests from
  genesis, its image is built from a pinned upstream commit with a minimal
  build-time patch that lets ingestion start at the dataset base. The patch changes
  only where ingestion begins; the measured read path is untouched.
- **Acknowledged comparison.** The *Performance* section of the README compares
  against the Go implementation. This is a deliberate, scoped exception to the
  project's no-comparison convention, kept neutral and method-first.

## Consequences

- The comparison is **relative, not absolute**: Docker Desktop / macOS VM overhead
  degrades absolute figures, but both proxies pay it equally under identical limits,
  so the relative result holds. The report states this and pins every version (Go
  commit, Rust commit, base images, `ghz`).
- At high concurrency over 2 vCPU the sweep measures **saturation**, not linear
  scaling — the intended steady-state behavior.
- The idle mock-poll and the build-time start-height patch are documented
  deviations from a stock deployment; neither is on the measured path.
- Fidelity rests on two checks the harness runs after populating, before any load
  run. First, with `getblock` denied by the mock, the read path still serves the
  whole range from cache — a miss would fall back to the node and fail loudly (the
  mock's tip RPCs stay up so the reference ingestor, which exits fatally if its tip
  poll fails, keeps running). Second, both proxies' `GetBlockRange` rendered stream
  (decoded to JSON, so this is content identity, not a wire-byte claim) over the
  full range must hash identically, so the comparison is only ever like for like.
- Cold-sync / ingestion throughput remains an explicit non-goal of *this* harness
  (see "Hot read-path only" above), but it is no longer a side concern now that
  [0020](0020-windowed-ingest-batched-commits.md) made catch-up ingest windowed and
  concurrent rather than one block per node round-trip: initial-sync throughput is
  now a tunable, measurable property (`--ingest-window`/`--ingest-concurrency`).
  A separate ingest-throughput benchmark, with its own dataset and fidelity checks,
  is expected as a follow-up; this ADR's method and results are not yet extended to
  cover it.
