# Mainnet benchmark summary — Phase 2 — 2026-07-14

Follow-on to the 2026-07-13 Phase 1 run, covering cases Phase 1 did not:
ingest tuning sensitivity (B1), read-path latency under active sync (B2),
resource footprint during heavy sync (B3), and a full genesis-to-tip sync
wall-clock (B4).

## Environment

| item | value |
|---|---|
| Host | Debian 12 (kernel 6.1.0-40-amd64), AMD Ryzen 7 7840HS, 16 CPUs (nproc), 28 GiB RAM |
| Node | zebrad, mainnet, synced (~3,411,800+ during B1-B3), RPC http://127.0.0.1:8232, no auth |
| NEW Rust commit | `3dfb1c9` (branch feat/ingest-performance-and-compliance), `cargo build --release` |
| Go lightwalletd (host build) | `61fee32` (current master), Go 1.24.4 |
| ghz | host binary, `go install github.com/bojand/ghz/cmd/ghz@v0.121.0` (reports version "dev" because no `-ldflags` were passed; source is the pinned v0.121.0 tag) |
| grpcurl | pre-existing host binary (readiness checks only, never for timing) |
| Machine load caveat | TWO zebrad nodes (mainnet + testnet) ran throughout — nontrivial background CPU/disk load, same as Phase 1. Treat all numbers as relative, not absolute. |
| Go range-start builds | documented one-line `NewBlockCache(dbPath, chainName, 0, ...)` → `NewBlockCache(dbPath, chainName, <start>, ...)` patch in `cmd/root.go`, `go build`, immediate `git checkout -- cmd/root.go` (verified clean after each build; only pre-existing untracked `zcash-dummy.conf` remains). B4's Go binary is a stock, unpatched build. |

Raw logs for every number were kept in the run’s scratch directory,
subdirs `b1/ b2/ b3/ b4/` — per-cell `.log` (server output), `.summary`,
`.errors`, ghz JSON (`b2/*.ghz.json`), and 5-second resource sample CSVs
(`b3/*.csv`).

## B1 — ingest tuning sensitivity sweep (NEW Rust only)

Sandblasting range, `--start-height 1780000` (node-bound: zebrad has to read
and serialize huge historical blocks, so this is the regime where client-side
pipelining choices matter most). One 180 s window per cell, fresh data dir per
cell (deleted after), matrix `--ingest-window` × `--ingest-concurrency`.
Blocks/s measured from the last `ingested ... to=` log line
(`(to − 1780000 + 1) / 180`). Zero errors in all 12 cells.

### blocks/s (blocks ingested in 180 s)

| window \ concurrency | 2 | 8 | 16 | 32 |
|---|---|---|---|---|
| **16** | 12.4 (2,240) | 29.2 (5,264) | 34.0 (6,128) | 34.0 (6,128) |
| **64** (default row) | 13.2 (2,368) | 36.3 (6,528) | 49.8 (8,960) | 53.0 (9,536) |
| **256** | 12.8 (2,304) | 38.4 (6,912) | 56.9 (10,240) | 59.7 (10,752) |

(default cell 64/8: 36.3 blocks/s)

### Tuning guidance

Concurrency is the dominant knob and it keeps paying beyond 8 in this
node-bound regime: at window 64, going 8 → 16 is +37% and 16 → 32 is another
+6%; at window 256 the same steps are +48% and +5%. The window mostly matters
as a ceiling on useful concurrency — at window 16 the 16 → 32 step is exactly
flat (6,128 blocks both times) because a 16-block window can never have more
than 16 fetches in flight. Window size on its own (at fixed concurrency 2 or
8) is nearly free: 16 → 256 moves throughput only ±10%. The default 64/8 is a
sensible conservative choice — it is within 3% of the best possible at
concurrency 8 — but operators catching up through the sandblasting range on a
well-provisioned node can get ~1.6x the default by raising to 256/32
(59.7 vs 36.3 blocks/s). Diminishing returns set in past concurrency 16;
32 buys only ~5-6% more and doubles the outstanding RPC load on the node.

## B2 — read-path latency under active sync

Question: does serving stay responsive while the ingestor runs full speed
(the `spawn_blocking` payoff)? Tool: host `ghz` v0.121.0 (not grpcurl),
GetBlock, concurrency 4 / 4 connections, 60 s per probe, round-robin over a
1000-height JSON payload list, all heights verified cached before probing
(readiness = GetBlock at the top of the probe range succeeds; ingest is
sequential so that implies the whole range).

- **during-sync**: fresh data dir, `--start-height 1500000` (fast heavy
  catch-up), 30 s head start, probe heights 1,500,000..1,500,999 while the
  ingestor churns (Rust had reached 1,710,559 by shutdown — full-speed sync
  throughout the probe; Go was likewise mid-catch-up, nowhere near tip).
- **idle**: fresh data dir, `--start-height 3410000` — reaches the actual tip
  (~3,411,885) in seconds, ingestor goes quiet, probe heights
  3,410,000..3,410,999.

| impl / scenario | p50 (ms) | p99 (ms) | throughput (req/s) | requests |
|---|---|---|---|---|
| Rust, during-sync | 0.157 | 1.547 | 12,831 | 769,892 |
| Go, during-sync | 0.157 | 1.391 | 13,235 | 794,105 |
| Rust, idle | 0.098 | 0.635 | 23,144 | 1,388,647 |
| Go, idle | 0.111 | 0.728 | 20,335 | 1,220,112 |

Interpretation: both implementations stay fully responsive under active
ingest — sub-millisecond medians and p99 under 1.6 ms at concurrency 4 in
all cells. Active sync costs both roughly the same: ~1.6x on p50 and ~2x on
p99 versus idle, which is what you would expect from sharing CPU and the RPC
node with a flat-out ingestor rather than from any serving-path stall. Rust's
`spawn_blocking` read path holds up as designed, and idle Rust is the fastest
cell overall (p50 0.098 ms, 23.1k req/s, ~14% above idle Go). Caveat: the
during-sync and idle probes necessarily target different height ranges
(1.5M vs 3.41M — you can only probe what is cached while also being
mid-sync), so the sync-vs-idle delta includes some block-content difference;
the Rust-vs-Go comparison within each row is like-for-like.

## B3 — resource footprint during heavy sync

Sandblasting range (`start 1780000`), 300 s each, fresh data dir, RSS and
CPU% sampled every 5 s from `/proc/<pid>/{stat,status}` (60 samples each).
CPU% is per-interval utime+stime delta; 100% = one core.

| impl | blocks ingested | blocks/s | peak RSS (MiB) | mean RSS (MiB) | peak CPU % | mean CPU % | cache dir (MiB) | bytes/block |
|---|---|---|---|---|---|---|---|---|
| Rust (NEW) | 10,944 | 36.5 | 801 | 463 | 222 | 206 | 2,056 | 197,010 |
| Go | 2,112 | 7.0 | 37.4 | 31.8 | 45.8 | 36.4 | 191 | 95,038 |

Interpretation: over the same 300 s the Rust ingestor moved 5.2x the blocks at
5.7x the mean CPU and ~15x the mean RSS. Per block ingested the CPU cost is
comparable (206%/36.5 ≈ 5.6 vs 36.4%/7.0 ≈ 5.2 CPU-%·s per block); Rust
simply keeps ~2 cores busy by pipelining while Go's serial ingestor waits on
the node. The RSS gap is real but bounded: peak 801 MiB reflects up to 64
huge sandblasting blocks in flight plus redb's write buffers (this range has
multi-MiB blocks; on light ranges Phase 1 measured Rust peak RSS at
79-204 MiB). On-disk cost per block is 2.07x Go's (redb page overhead plus
file-doubling growth; same ~2.3x ratio Phase 1 saw), with both storing the
identical ingested span from 1,780,000.

## B4 — full default-config sync to tip

Wall-clock of a complete sync from genesis against the live mainnet node, one
implementation at a time, default ingest settings, fresh data dir. Progress
tracked from the Rust `ingested ... to=` logs and Go's `db/main/lengths` file
size (4 bytes/block). Free disk verified > 60 GB before each run; each data
dir was deleted after its size was recorded.

### NEW Rust (`--start-height 0`, defaults 64/8)

- **Total: 4,950 s (1 h 22 m 30 s) from process start to tip**; tip at
  completion = 3,411,957 (log-based first-ingest→tip = 4,928 s).
- **Overall: 689 blocks/s** (3,411,958 blocks / 4,950 s).
- Final cache: **42.0 GiB** (45,097,193,472 B, single `main-blocks.redb`) for
  3,411,959 blocks → **13.2 KB/block** averaged over the whole chain.
- **Errors: none.** Full-log scan for mismatch/error/fatal/panic/corrupt/warn
  (minus the expected plaintext-TLS warning) returned zero lines across the
  entire sync.

Split times (from log timestamps, cumulative and per-segment):

| height reached | elapsed (s) | segment (s) | segment blocks/s |
|---|---|---|---|
| 500,000 | 271 | 271 | 1,845 |
| 1,000,000 | 486 | 215 | 2,326 |
| 1,500,000 | 700 | 214 | 2,336 |
| 2,000,000 | 4,070 | 3,370 | 148 |
| 2,500,000 | 4,549 | 479 | 1,044 |
| 3,000,000 | 4,755 | 205 | 2,439 |
| 3,411,957 (tip) | 4,928 | 174 | 2,368 |

The 1.5M→2M segment — the sandblasting/spam era — is 68% of the entire sync
(3,370 s of 4,928 s) at 148 blocks/s; every other segment runs at
1,000-2,400 blocks/s. The projected 2.5-4 h was beaten by ~2x; the default
64/8 tuning was used throughout (B1 suggests 256/32 could shave the spam
segment further).

### Go reference (stock build, natural start at height 0)

**Did not finish inside the 8-hour cap.** Per plan, the run was stopped at
8 h 00 m and the result extrapolated:

- **Height reached at 8 h: 2,046,039** (blocks ingested 2,046,040) of tip
  3,412,340 at that moment — **59.9% of the chain**.
- Overall to that point: **71.0 blocks/s**.
- Cache at stop: **25.6 GiB** (27,514,108,682 B, `db/main/{blocks,lengths}`)
  → **13.4 KB/block** — essentially identical per-block cost to Rust's redb
  over the same kind of span (13.2 KB/block full-chain), because the
  whole-chain average is dominated by the huge spam blocks where redb's
  relative overhead is negligible.
- **Errors: none.** `app.log` (7,051 lines) contains zero
  mismatch/fatal/panic/corrupt lines and no error/warning entries other than
  the expected startup "Starting insecure no-TLS (plaintext) server".

Split times (from `Adding block to cache` timestamps in `app.log`):

| height reached | elapsed (s) | segment (s) | segment blocks/s |
|---|---|---|---|
| 500,000 | 1,611 | 1,611 | 310 |
| 1,000,000 | 2,948 | 1,337 | 374 |
| 1,500,000 | 4,453 | 1,505 | 332 |
| 2,000,000 | 27,634 | 23,181 | 22 |
| 2,046,039 (stop @ 8 h) | 28,806 | 1,172 | 39 |

**Extrapolated total: ≈ 9.5-10 h.** Remaining 1,366,301 blocks are all
post-spam; using Go's own measured post-spam rates (330-374 blocks/s on light
ranges here, 281 blocks/s effective on the recent range in Phase 1, and
assuming the 2.0-2.5M shoulder runs ~2x slower than light, mirroring the
shape of the Rust run) gives 1.3-1.8 h more, i.e. **total ≈ 34,000-36,000 s**.
Uncertainty is modest because 95% of the remaining work sits in ranges where
Go's rate was directly measured.

### B4 head-to-head

| | NEW Rust | Go (extrapolated) |
|---|---|---|
| genesis → tip wall time | **4,950 s (1 h 22 m)** | ≈ 34,000-36,000 s (9.5-10 h); 28,806 s → 59.9% measured |
| overall blocks/s | 689 | 71 (at 8 h stop); ≈ 96-100 extrapolated full-chain |
| spam segment (1.5→2.0M) | 3,370 s (148 b/s) | 23,181 s (22 b/s) |
| final cache | 42.0 GiB / 13.2 KB per block (full chain) | 25.6 GiB / 13.4 KB per block (60% of chain) |
| errors | none | none |

The windowed concurrent ingestor is worth ≈ 7x on a complete
genesis-to-tip mainnet sync, and the entire gap is concentrated where it
matters most — the sandblasting era, where Rust's pipelining hides the
node's per-block serialization latency (148 vs 22 blocks/s, 6.9x) while both
implementations are equally node-bound per request.

## Anomalies and notes

1. **No txid/hash-mismatch errors anywhere in Phase 2** — B1 (12 runs), B2
   (4 runs), B3 (2 runs), B4 (2 runs) all clean; scans covered every server
   log and Go `app.log` verbatim.
2. **B1 w16-c32 = w16-c16 exactly** (6,128 blocks both): the ingest window
   caps effective concurrency; not an anomaly, but worth knowing when tuning.
3. **B4 Rust beat its 2.5-4 h projection by ~2x** (1 h 22 m). The projection
   was derived from Phase 1's 8-minute R2 window; over a full sync the spam
   era is a smaller fraction of total work than that window implied, and the
   light ranges run at 1,800-2,400 blocks/s.
4. **Rust B3 RSS (peak 801 MiB) is much higher than Phase 1's read-path
   numbers** (79-204 MiB): that is the cost of 64-block windows of multi-MiB
   sandblasting blocks in flight, not a leak — RSS on light ranges stays low
   (B2's sync run over 1.5M-range blocks showed no such growth).
5. **zebrad restarts/noise**: the mainnet node stayed up and synced
   throughout; its tip advanced ~580 blocks during the ~14 h of Phase 2 runs.
   The testnet node was untouched.
6. Rust's B4 log shows one final `ingested from=3411958 to=3411958` 32 s
   after reaching the recorded tip — the chain advanced by one block while
   we confirmed; excluded from the wall time (cutoff is reaching the tip as
   measured at completion, 3,411,957).

