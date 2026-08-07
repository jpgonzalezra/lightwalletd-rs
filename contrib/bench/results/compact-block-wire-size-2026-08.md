# Wire size of compact blocks, by era

Date: 2026-08-01.

How many bytes `GetBlockRange` actually puts on the wire, measured per historical era rather than
extrapolated from cache footprints. Motivated by sizing a fixed-packet, metered transport (a mixnet)
in front of the server, but the numbers stand on their own: they bound what any bandwidth-limited
client pays to sync.

## Method

`examples/wire_size.rs` opens a compact-block cache, reads every stored value through
`Cache::for_each_raw` (no decode round trip), and reports the size distribution, the fixed-size
packet count those bytes imply, and zstd ratios under two schemes.

The stored value is the serialized `CompactBlock` protobuf, which is exactly what `GetBlockRange`
puts on the wire; the only addition is five bytes of gRPC length-prefix framing per message.

Five windows were ingested from a live mainnet `zebrad` (tip 3,432,788) into throwaway data dirs,
one per era, each capped at 150 MiB or 15 minutes. Each window is a contiguous range starting at the
listed height. Caches were measured as copies, never in place.

## Results

| era | heights | blocks | p50 | mean on the wire | p99 | max |
|---|---|---|---|---|---|---|
| Sapling activation | 419,200 to 429,515 | 10,316 | 1,137 | 4,884 | 45,287 | 425,046 |
| pre-spam steady state | 1,000,000 to 1,013,695 | 13,696 | 825 | 3,226 | 23,702 | 217,248 |
| **sandblasting** | 1,780,000 to 1,781,087 | 1,088 | 86,793 | **90,580** | 252,094 | 264,381 |
| post-sandblasting | 2,500,000 to 2,513,503 | 13,504 | 1,485 | 2,316 | 11,674 | 242,378 |
| recent | 3,350,000 to 3,414,703 | 64,704 | 738 | **1,349** | 8,003 | 236,990 |

All figures are bytes per block.

### The sandblasting era dominates everything

At 90,580 bytes per block the sandblasting stretch is **67x the recent era**, and unlike every other
window its median tracks its mean (86,793 vs 90,580), so this is the whole era rather than a few
outliers dragging an average.

This is independently corroborated by the existing ingest benchmark
(`mainnet-2026-07-summary.md`, Part B): the R2 sandblasting window recorded 2.5 GB of data dir for
17,856 blocks, or ~140 KB per block of `redb` footprint against the ~90.6 KB of payload measured
here. The gap is the expected store overhead, and the two numbers were produced by different methods
weeks apart.

The window is short (1,088 blocks) because it hit the 150 MiB size cap in 400 seconds. Given how
tight the distribution is, the mean is well determined even at that sample size.

## Compression

| era | zstd-3 per message (what gRPC compression does) | zstd-3 over the whole stream |
|---|---|---|
| Sapling activation | 1.31x | 1.88x |
| pre-spam steady state | 1.30x | 1.57x |
| sandblasting | 1.02x | 1.03x |
| post-sandblasting | 1.14x | 1.42x |
| recent | 1.06x | 1.37x |

Two conclusions.

**Do not enable gRPC compression.** Per-message compression buys 1.06x on recent blocks and 1.02x on
the era that actually costs bandwidth. Individual compact blocks are too small for a compressor to
find anything, and where they are large they are incompressible note ciphertext.

**Stream-level compression is a modest win, not an enabler.** Compressing the whole stream gets
1.37x on recent blocks, but collapses to 1.03x exactly where it would matter most. It cannot rescue
a sync that is too heavy.

## Fixed-size packets

A transport that bills in fixed packets (a mixnet packet carries a 2048-byte payload) exposes a byte
stream, so packets fill contiguously and there is no alignment to tune. What matters is the packet
count, because in a mixnet every reply packet consumes one single-use reply block against a default
budget of 10.

At the recent-era rate of 1,349 bytes per block:

| operation | blocks | bytes | packets |
|---|---|---|---|
| one day of sync | 1,152 | 1.5 MiB | 759 |
| `GetBlockRange` of 1,000 | 1,000 | 1.3 MiB | 659 |
| `GetBlockRange` of 10,000 | 10,000 | 12.9 MiB | 6,588 |

A single day of catch-up already needs 76x that default budget, so reply-block replenishment is not
an edge case reached during initial sync; it is the ordinary daily path.

## What this implies for initial sync

Applying each window's mean to the span it represents, with era boundaries estimated rather than
measured:

| span | blocks | at | bytes |
|---|---|---|---|
| 419,200 to 1,000,000 | 580,800 | 4,884 | 2.8 GB |
| 1,000,000 to 1,600,000 | 600,000 | 3,226 | 1.9 GB |
| 1,600,000 to 2,000,000 | 400,000 | 90,580 | **36.2 GB** |
| 2,000,000 to 2,500,000 | 500,000 | 2,316 | 1.2 GB |
| 2,500,000 to 3,432,788 | 932,788 | 1,349 | 1.3 GB |
| total from Sapling | 3,013,588 | | **~43 GB** |

**Caveat: the 1,600,000 and 2,000,000 boundaries are guesses.** They were not measured, and they set
83% of the total. Pinning them down needs three or four more sample windows, which is cheap and
should be done before this number is quoted anywhere. The qualitative conclusion does not depend on
them: the sandblasting era dominates by more than an order of magnitude regardless of where exactly
it starts and stops.

At a metered ~1 Mbps, 43 GB is roughly four days of continuous transfer. **A full sync from Sapling
over such a link is not viable**, and no compression scheme changes that.

The picture is very different by wallet birthday, which is the number a user actually experiences:

| wallet birthday | bytes to sync | at ~1 Mbps nominal | at the measured mixnet rate |
|---|---|---|---|
| after 2,500,000 | 1.3 GB | ~2.8 hours | ~7.5 hours |
| after 2,000,000 | 2.4 GB | ~5.4 hours | ~14 hours |
| before the sandblasting era | ~43 GB | not viable | not viable (~11 days) |

**Quote the last column, not the third.** The ~1 Mbps figure is a link's nominal capacity, whereas
`GetBlockRange` measured through an actual mixnet sustained about 48 KB/s (~400 kbps), some 2.5x
less. See [`mixnet-transport-2026-08.md`](mixnet-transport-2026-08.md). Both columns are floors in
any case, since they ignore the establishment failures and retries that transport imposes.

## Takeaways

1. Compression is settled: no gRPC compression, and stream compression is optional and small.
2. Packet alignment is a non-question for a stream-oriented transport.
3. A bandwidth-limited transport can carry every RPC, but bulk initial sync across the sandblasting
   era is not a usable path over one, and documentation should say so rather than implying such a
   transport is a drop-in replacement for the ordinary listener.
4. Recent-era numbers understate historical cost by a wide margin. Any capacity estimate derived
   from the current tip should be treated as a floor.
