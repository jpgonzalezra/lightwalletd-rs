# lightwalletd-rs: readstate vs rpc backend benchmark report

**Date:** 2026-07-14/15 (UTC)
**Repo:** the repository checkout, branch `feat/zebra-readstate-backend`
**Commit under test:** `f7e91ac` ("fix: close the two readstate wire differences found by the parity sweep")
**Build:** `cargo build --release --features readstate` (rustc 1.96.0, cargo 1.96.0)
**Node under test:** live mainnet `zebrad 6.0.0`, RPC at 127.0.0.1:8232, indexer gRPC at 127.0.0.1:8231, synced to tip throughout the run (chain height moved 3,412,482 → 3,412,650+ over the session)
**Shared-host caveat:** TWO zebrad nodes (mainnet + testnet) ran throughout, same as the prior phase-1/phase-2/parity reports — nontrivial background CPU/disk load. Treat all numbers as relative, not absolute. Host: Debian 12 (kernel 6.1.0-40-amd64), 16 CPUs, 28 GiB RAM.
**Tools:** `ghz` v0.121.0 (host binary) for unary calls (`GetTreeState`, `GetBlock`); `grpcurl` (reflection-based) driven by a small Python timing harness (`streaming_probe.py`) for server-streaming calls (`GetSubtreeRoots`, `GetTaddressTxids`), sequential (one call at a time, concurrency 1) in all cases.
**Raw logs:** every number below is traceable to the run's scratch directory, subdirs `step0/`, `b1/`, `b2/`, `b3/`, `b4/`.

---

## Step 0 — parity re-check

Commit `f7e91ac` was supposed to fix the two wire differences the 2026-07-14 parity sweep found (`contrib/bench/results/rss-parity-2026-07.md`): treestate pool-gating (empty pools should be omitted, not serialized as `"000000"`) and upgrade-name branding (`"NU6.3"` not `"Nu6_3"`). Re-ran exactly the failed comparisons, same setup as the original sweep: two servers, `rpc` on :19201 / `readstate` on :19202, `--start-height 3406000`, fresh data dirs, both confirmed at live tip (3,412,483) before any comparison ran.

| Check | Result |
|---|---|
| `GetTreeState` at 12 sampled heights (419200, 700000, 903000, 1046400, 1687104, 2000000, 2726400, 3000000, 3300000, 3400000, 3410000, 3412367) | **12/12 PASS** — byte-identical |
| `GetTreeState` by-hash at 3 of those heights (419200, 700000, 903000) | **3/3 PASS** — byte-identical |
| `GetLatestTreeState` | **PASS** — byte-identical once both servers were confirmed at the same tip height (an initial attempt raced ahead of readstate's catch-up and showed the expected divergent-height response; re-run after confirming both at 3,412,483 was fully identical, zero diffs) |
| `GetLightdInfo` | **PASS** — `upgradeName` is `"NU6.3"` on both backends now; only `estimatedHeight`/`blockHeight` differed by the allowed live-tip timing skew on the first attempt (servers were still catching up 69 blocks apart) |

**Verdict: ALL PREVIOUSLY-FAILED CHECKS NOW PASS (17/17).** Both real wire differences from the 2026-07-14 sweep are closed. Cleared to proceed with benchmarking.

Raw: `the run's scratch directory`

---

## B1 — fixed-range ingest, rpc vs readstate (8-minute windows)

`blocks/s = (final_to − start_height + 1) / 480`, one instance running at a time, fresh data dir per cell (deleted after).

| range | start height | backend | final height | blocks | blocks/s |
|---|---|---|---|---|---|
| R1 (modern) | 1,500,000 | rpc | 1,738,463 | 238,464 | **496.8** |
| R1 (modern) | 1,500,000 | readstate | 1,735,135 | 235,136 | **489.9** |
| R2 (sandblasting) | 1,780,000 | rpc | 1,797,791 | 17,792 | **37.1** |
| R2 (sandblasting) | 1,780,000 | readstate | 1,791,071 | 11,072 | **23.1** |
| R3 (recent, tip-capped) | 3,300,000 | rpc | 3,412,597 | 112,598 | 234.6* |
| R3 (recent, tip-capped) | 3,300,000 | readstate | 3,412,599 | 112,600 | 234.6* |

*R3's blocks/s figure is diluted by ~430s of post-catch-up idle tip-following; the real signal is time-to-tip:*

| R3 time-to-tip (3,300,000 → live tip) | rpc | readstate |
|---|---|---|
| wall time | 50.04 s | **40.02 s** (~25% faster) |
| catch-up rate | ~2,249 blocks/s | ~2,814 blocks/s |

**Reading:** R1 (which straddles the *start* of the sandblasting slowdown — see B2) is a near-wash, readstate ~1.4% slower. R2 (deep sandblasting) is readstate's worst case: **38% slower than rpc** (23.1 vs 37.1 blocks/s). R3 (light, recent blocks) is readstate's best case: **25% faster time-to-tip** than rpc. This is the same pattern B2 shows at full-chain scale.

**Errors:** no crashes, no txid mismatches in any of the 6 runs. `readstate` logs exactly 2 transient `WARN` lines at startup in every run (`read state error: read state has no chain tip yet`, from the ingestor and the mempool monitor), both retrying immediately and succeeding on the next tick — a benign startup race against the in-process `ReadStateService` not yet reporting a tip in the first tens of milliseconds. `rpc` logs are completely clean. No run was materially affected.

Raw: `the run's scratch directory` (`*.log`, `*.meta` per cell)

---

## B2 — full genesis→tip sync, readstate backend

`--start-height 0`, `--backend readstate --zebra-indexer-url 127.0.0.1:8231`, fresh data dir (`the run's scratch directory`, deleted after size was recorded).

- **Total: 5,885.91 s (1 h 38 m 06 s)**, log-based first-ingest → tip (first log line `2026-07-14T22:21:38.818147Z` → tip-reached line `2026-07-14T23:59:43.910960Z`).
- Tip at completion: **3,412,556**.
- **Overall: 579.8 blocks/s** (3,412,556 / 5,885.91).
- Final cache: **45,097,193,472 B = 42.0 GiB** — the *exact same byte count* as the `rpc` backend's B4 full-sync cache in `mainnet-2026-07-phase2.md` (identical on-disk compact-block cache format regardless of backend; both runs landed at essentially the same final chain height).
- **Errors: none.** Full clean-log scan for mismatch/error/fatal/panic/corrupt returned zero lines; the only `warn` match is the expected startup `"running without TLS (plaintext) — do not use in production"`. No txid mismatches from the `get_block_verbose` / `TransactionIdsForBlock` cross-check.

### Per-500k splits vs. the rpc-backend reference run (phase2, `mainnet-2026-07-phase2.md` B4)

| height | readstate cumulative (s) | readstate segment (s) | readstate blocks/s | rpc cumulative (s) | rpc segment (s) | rpc blocks/s |
|---|---|---|---|---|---|---|
| 500,000 | 150.00 | 150.00 | 3,333.3 | 271 | 271 | 1,845 |
| 1,000,000 | 270.00 | 120.00 | 4,166.7 | 486 | 215 | 2,326 |
| 1,500,000 | 408.00 | 138.00 | 3,623.2 | 700 | 214 | 2,336 |
| 2,000,000 | 5,176.00 | **4,768.00** | **104.9** | 4,070 | 3,370 | 148 |
| 2,500,000 | 5,640.00 | 464.00 | 1,078.0 | 4,549 | 479 | 1,044 |
| 3,000,000 | 5,775.00 | 135.00 | 3,703.7 | 4,755 | 205 | 2,439 |
| 3,412,556 (tip) | 5,885.91 | 110.91 | 3,719.6 | 4,928 (tip 3,411,957) | 174 | 2,368 |

### Headline anomaly

readstate wins **6 of 7 segments** — often by 1.5-3.6x (e.g. the final light stretch: 3,719.6 vs 2,368 blocks/s) — because it skips the JSON-RPC round trip and JSON (de)serialization entirely. But it **loses the one segment that dominates total wall time**: the 1.5M-2.0M sandblasting era, where readstate ran at **104.9 blocks/s vs rpc's 148 blocks/s (~29% slower)**. That single segment ate **81% of readstate's total elapsed time** (4,768 / 5,885.91 s) vs **68% for rpc** (3,370 / 4,928 s), so despite winning almost everywhere else, readstate's genesis→tip wall time (5,885.91 s) ends up **~19% slower overall than rpc's** (4,928 s).

Live process inspection during the slow segment showed `lightwalletd-rs` CPU-bound at 400-570% (4-5.7 cores busy), RSS ~3 GiB — consistent with the architectural tradeoff: readstate reconstructs note-commitment-tree updates and compact-block encodings **in-process from raw chain data**, which is genuinely CPU-heavier per huge sandblasting-era block than deserializing zebrad's already-computed `z_gettreestate`/`getblock`-verbose JSON over the rpc path. This is a real tradeoff, not a bug — readstate trades node-RPC-bound latency for local CPU-bound decode cost, and the trade is a clear net win everywhere except the adversarial spam range, which is exactly what B1's R1/R2 cells independently corroborate at smaller scale.

Raw: `the run's scratch directory` (`fullsync.log`, `splits.log`, `SUMMARY.txt`)

---

## B3 — read-surface latency, rpc vs readstate

Fresh instance per backend, cache pre-warmed with `[3,406,000..tip]` first (both reached tip in well under a minute). Same request set (same randomized heights / same 3 addresses) used for both backends. `n` = iteration count, all sequential (concurrency 1).

| probe | method | n | rpc p50 | rpc p99 | readstate p50 | readstate p99 | delta (p50) |
|---|---|---|---|---|---|---|---|
| GetTreeState, random height ∈ [1,687,104, 3,400,000] | ghz unary | 200 | 2.891 ms | 207.885 ms | 0.707 ms | 209.394 ms | **readstate ~4.1x faster** |
| GetSubtreeRoots (sapling, start=0, max=64) | grpcurl streamed, timed | 50 | 11,972.5 ms | 12,123.5 ms | 16,699.3 ms | 16,925.1 ms | **readstate ~40% SLOWER** |
| GetTaddressTxids, `t3cFfPt1...` (hits 10k cap, [3.2M,3.4M]) | grpcurl streamed, timed | 20 | 393.6 ms | 551.1 ms | 335.3 ms | 351.6 ms | readstate ~15% faster |
| GetTaddressTxids, `t1Ku2KLy...` (hits 10k cap) | grpcurl streamed, timed | 20 | 212.9 ms | 230.3 ms | 198.0 ms | 208.8 ms | readstate ~7% faster |
| GetTaddressTxids, `t1MKn34K...` (full stream, no cap) | grpcurl streamed, timed | 20 | 2,763.0 ms | 3,417.3 ms | 379.2 ms | 388.5 ms | **readstate ~7.3x faster** |
| GetBlock (cache hit, control) | ghz unary | 200 | 0.090 ms | 0.344 ms | 0.090 ms | 0.507 ms | parity (control passes) |

### Interpretation

- **GetTreeState — as predicted, the biggest clean win.** readstate serves it from in-process zebra state instead of round-tripping to the node's `z_gettreestate`: p50 drops 4.1x (2.89ms → 0.71ms). p99 is essentially tied (~208ms both) — the tail is dominated by something both backends pay equally for a handful of the 200 randomized heights (likely a cold historical-tree read cost common to both paths), not by the RPC-vs-in-process choice.
- **GetTaddressTxids — a clean, large win**, especially for the address whose 200k-block window returns a real 25k+-record stream rather than an early `ResourceExhausted`: readstate is **7.3x faster** there (2.76s → 0.38s p50). Even the two addresses that fast-fail on the 10k cap are modestly faster on readstate (7-15%), consistent with the address-index scan itself being cheaper in-process.
- **GetSubtreeRoots — the one surprise, and the one place readstate is clearly *worse*.** Root cause (confirmed by reading `src/service/subtrees.rs::get_subtree_roots` and both `NodeClient` implementations): the handler does an **N+1 fetch** — one cheap `z_getsubtreesbyindex`/`SaplingSubtrees`-equivalent call to get the 64 subtree records, then loops and calls `block_at(...)` **per entry** just to resolve the completing block's hash, which on a cache miss fetches and fully parses the whole block (`fetch::compact_block`) purely to extract a hash. On `rpc` that's up to 1 + 64×2 = 129 JSON-RPC round trips to zebrad (~12s total, ~90ms/call). On `readstate` the same N+1 shape exists but each "call" is a local zebra-state read instead of a network round trip — and it's still **slower** (16.7s vs 12.0s), because those 64 completing-block heights are scattered deep in chain history (the samples spanned heights 558,822 to well past 1.7M) and reading+fully-decoding old blocks from zebra's on-disk RocksDB state plus doing the compact-block CPU conversion locally costs more, per block, than zebrad's own (likely better-cached/optimized) RPC serving path for the same lookup. This mirrors the B2 finding almost exactly: readstate wins when the work is "fetch bytes," loses when the work is "decode a full historical block in-process." **This N+1 pattern (in `src/service/subtrees.rs`) is the actual root cause of GetSubtreeRoots being slow on *both* backends — 12-17 seconds for 64 entries is a real latency bug independent of which backend serves it, and is worth fixing upstream regardless of this benchmark.**
- **GetBlock (cache-hit control) — parity confirmed, no regression.** p50 identical (0.090ms both); p99 differs by <0.2ms, within noise. Confirms the shared cache-hit read path is backend-independent as expected — the deltas above are real backend-specific effects, not measurement artifacts.

Raw: `the run's scratch directory` (`*.ghz.json` for unary, `*.json`/`*.stdout` for streamed probes, `*_data.json`/`taddr_*.json` request bodies, server logs)

---

## B4 — under-sync serving check (spot)

readstate backend started fresh on R1 (`--start-height 1,500,000`), given a 30s head start, confirmed ready (`GetBlock` at the top of the probe range, height 1,500,999, succeeded 2s after the head start), then the B3 `GetBlock` control probe (200 sequential ghz calls) was run **while the ingestor was still running at full speed** (log confirms ~1,300-5,500 blocks/s ingest immediately before/after the probe, i.e. never paused).

| | p50 | p99 | requests |
|---|---|---|---|
| readstate, GetBlock, during-sync (this run) | 0.145 ms | **1.499 ms** | 200/200 OK |
| rpc, GetBlock, during-sync (phase2 reference, concurrency 4) | 0.157 ms | 1.547 ms | 769,892 |
| Go, GetBlock, during-sync (phase2 reference, concurrency 4) | 0.157 ms | 1.391 ms | 794,105 |

**Confirmed: p99 (1.499 ms) stays in the same band (~1.5 ms) as the phase-2 reference numbers.** readstate's `spawn_blocking`-backed (or equivalent) read path holds up under full-speed concurrent ingest, same as the rpc backend and Go reference before it — active sync does not meaningfully degrade serving latency for cached reads.

**Anomaly (shutdown-path, not measurement-affecting):** after the probe completed (200/200 OK, data already captured) the benchmark script sent `SIGTERM`. The gRPC server drained and stopped cleanly (`"shutdown signal received"` → `"server stopped"`), but the ingestor kept running one more window afterward and then an ingest fetch task logged an **unhandled panic**:
```
thread 'tokio-rt-worker' (1705343) panicked at src/ingestor.rs:255:39:
fetch task neither panics nor is aborted: JoinError::Cancelled(Id(941329))
```
This did not occur in any other run (B1's 6 cells and B2's full sync, all also killed mid-ingest, show zero panics), so it looks like a rare timing-dependent race: an in-flight concurrent-fetch `JoinHandle` gets cancelled by runtime teardown during SIGTERM handling, and the `.expect(...)` at that call site doesn't treat `JoinError::Cancelled` as an expected outcome of shutdown. It did not affect the B4 latency numbers (already captured before the kill) and the process still exited. Flagging verbatim per the mission brief as a genuine, reproducible-looking (if rare) shutdown-robustness gap worth a look, separate from the readstate-vs-rpc comparison this report is otherwise about.

Raw: `the run's scratch directory` (`server.log`, `readstate-during-sync-getblock.ghz.json`)

---

## Summary

1. **Step 0: both wire-parity bugs from the 2026-07-14 sweep are fixed** — 17/17 previously-failing checks now pass byte-identical.
2. **B1/B2 (ingest throughput): readstate is a large net win except in the sandblasting era**, where it is 30-40% *slower* than rpc despite avoiding the network round trip, because in-process note-commitment-tree/compact-block reconstruction is CPU-heavier per huge historical block than deserializing zebrad's precomputed RPC JSON. Net effect on a full genesis→tip sync: readstate (5,885.91s) ends up ~19% slower in wall time than rpc (4,928s) *purely because the one slow segment dominates the total*, even though readstate wins 6 of 7 segments individually, often by 1.5-3.6x.
3. **B3 (read-surface latency): readstate wins decisively on GetTreeState (4.1x) and GetTaddressTxids (up to 7.3x)** — exactly the surfaces the mission expected the biggest delta on. **GetSubtreeRoots is the one surface where readstate is slower (40%)**, and both backends are slow in absolute terms (12-17s for a 64-entry request) due to a shared N+1 fetch-per-subtree-entry pattern in `src/service/subtrees.rs` that is independent of backend choice and looks like a real latency bug worth fixing regardless of which backend serves it. GetBlock (cache-hit control) shows no backend-dependent regression, as expected.
4. **B4: readstate stays responsive under full-speed concurrent ingest** (p99 1.499ms, same band as the phase-2 rpc/Go reference ~1.5ms) — the concurrent-serving design goal holds for readstate too. One rare, non-measurement-affecting shutdown panic was observed and documented for follow-up.

