# 0040. Bound the cache read chunk by bytes, not only by height count

## Context

[0028](0028-mvcc-chunked-cache-reads.md) has `block_range_stream` read the cached part of a range in
chunks of 64 consecutive heights, one `redb` read transaction each. The chunk is materialized in
full, decoded, and only then served block by block. Its Consequences section notes that peak memory
per in-flight stream goes up 64-fold and calls that "small in absolute terms (a compact block is far
smaller than the full block it summarizes)".

That reasoning holds for one stream reading average recent blocks. Neither half is a given. The
client picks the heights, and the wire-size benchmark
([`contrib/bench/results/compact-block-wire-size-2026-08.md`](../../contrib/bench/results/compact-block-wire-size-2026-08.md))
measures how much that choice is worth:

| era | mean bytes per block | 64 blocks |
|---|---|---|
| recent | 1,349 | 86 KB |
| post-sandblasting | 2,316 | 148 KB |
| pre-spam steady state | 3,226 | 206 KB |
| Sapling activation | 4,884 | 313 KB |
| **sandblasting** | **90,580** | **5.8 MB** |

The sandblasting era is 67x the recent one, and its median tracks its mean, so that is the whole era
and not a few outliers in it. Those heights sit above the default ingest floor, so any synced mainnet
instance holds them.

The client also picks how many streams run at once. `--max-concurrent-streams` is 256 per connection,
and a gRPC server-streaming body is client-paced: `h2` polls it only when the stream has send
capacity, so a client that stops reading leaves the generator suspended at its first yield with the
rest of the chunk still on the heap. One sandblasting block already exceeds the 65,535-byte default
stream window, so this needs no unusual flow-control settings, just a client that does not read.
Nothing reaps a suspended stream: `concurrency_limit_per_connection` is tower's in-flight *request*
limiter and releases its permit when the handler returns the stream, before a byte of body exists.

So the aggregate a host has to survive is chunk bytes x streams x connections, and only the middle
term was bounded. On the numbers above that is roughly 2.8 GB of resident chunk memory per
connection, reachable with a few KB of upload and no download at all. Nothing is corrupted and
nothing wrong is served. The process is killed, and every wallet on the instance loses service.

## Decision

Bound a cache read by bytes as well as by height count: `Cache::read_chunk` stops filling once the
stored blocks reach `CACHE_READ_CHUNK_BYTES` (512 KiB) and reports the sub-range it covered, so the
serving loop resumes from there. `CACHE_READ_CHUNK` (64) becomes the maximum number of heights rather
than the only bound.

The chunk holds the stored bytes and each block is decoded as it is served. The budget is then the
resident size, in the units the wire-size benchmark measures, instead of that size times whatever
decoding inflates it to (around 1.9x for an output-dominated block: three `Vec<u8>` headers and three
rounded-up allocations against three length-delimited fields on the wire).

512 KiB, because:

- It clears the largest compact block on record, 425,046 bytes at Sapling activation, so a chunk
  holds at least one block on size alone. The read keeps its first block whatever the budget says, so
  a larger block appearing later cannot stall a range on that height.
- A full 64-height chunk still fits in every measured era but the sandblasting one, per the table
  above. An ordinary sync keeps the whole chunk and the transaction amortization 0028 wanted: the
  bound only engages where the weight is. In the sandblasting era a chunk becomes about six blocks.
- 512 KiB x 256 streams is 128 MiB per connection, which an operator can size a host against.

A descending range fills from the high end, since that is the end it serves first. Filling from the
low end and cutting the top off would drop exactly the blocks needed next.

The budget is a constant, not a flag, for the reason 0028 gives for the chunk size: it trades two
costs no operator is positioned to weigh.

## Consequences

Resident chunk memory per stream is bounded by a number, not by whatever the requested heights happen
to weigh. Per connection the worst case goes from about 2.8 GB to 128 MiB, and how much a client can
pin no longer depends on which era it asks for.

Ranges in heavy eras cross more chunk boundaries: about eleven reads per 64 heights in the
sandblasting era instead of one. Each seam is a fresh read transaction and a continuity check, both
of which the range already did between chunks ([0027](0027-block-range-continuity.md)), and each
still amortizes over half a megabyte of blocks. Elsewhere the chunking is unchanged.

The intra-chunk MVCC guarantee from 0028 is unchanged in kind and narrower in span: the blocks in a
chunk still come from one point in time, and a shortened chunk means one more seam where 0027's hash
check applies. Nothing moves from "consistent by construction" to "unchecked".

Decoding moves from the read to the yield. A client that abandons a stream after two blocks no longer
pays to decode the rest of the chunk. A block that fails to decode now fails when the stream reaches
it rather than before the chunk serves anything, so the blocks ahead of a corrupt one are delivered.
That is the same partial-then-error shape a discontinuity already produces.

This bounds one stream. The sum across streams is still chunk bytes x streams x connections, with the
last term unbounded, and a suspended stream still has no deadline. Both are worth closing on their
own terms. Neither is something a per-chunk budget can do.
