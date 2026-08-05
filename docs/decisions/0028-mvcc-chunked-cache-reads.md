# 0028. Read the cache in MVCC chunks when serving a range

## Context

Serving a block range read the cache one height at a time, each in its own `redb` read transaction. A
10,000-block range was 10,000 independent point-in-time views, and the ingestor writes to the same
cache concurrently: a reorg truncation landing between two of those reads makes the range straddle
it, returning blocks from before the truncation followed by blocks from after it.

`redb` reads are MVCC, so a single read transaction sees one consistent snapshot of the store for as
long as it is held, whatever the ingestor commits meanwhile. The store already relies on this for the
snapshot export walk. Holding one is not free: an open read transaction keeps `redb` from reclaiming
the pages its snapshot still references, and a gRPC stream advances at the client's pace, so a
transaction spanning a whole range would stay open for as long as the slowest wallet takes to consume
it.

## Decision

Read the cached portion of a range in fixed chunks of consecutive heights (64), one read transaction
per chunk, and resolve from the node only the heights the chunk did not contain.

Within a chunk the blocks come from one snapshot and cannot contradict each other. Across chunk
boundaries and at the cache/node boundary they can, which is what the continuity check in
[0027](0027-block-range-continuity.md) covers: the two decisions are the same guarantee applied where
each is cheap, storage consistency inside the chunk and hash verification at the seams.

Each chunk's transaction is released before any node request is awaited. The node round trip is the
slow part of serving an uncached height, and holding a read transaction across it would pin the
store's page reclamation for the duration with nothing gained.

## Consequences

- The cached portion of a range cannot contradict itself within a chunk, by construction rather than
  by detection.
- One read transaction per 64 heights instead of one per height, so a long range does less
  transaction setup.
- A chunk's blocks are held in memory together, where the per-height reads held one at a time: peak
  memory per in-flight stream goes up 64-fold. That is the price of the intra-chunk guarantee, and it
  is small in absolute terms (a compact block is far smaller than the full block it summarizes), but
  it is a cost, not a saving.
- Page reclamation is delayed by at most one chunk's service time, not by a whole range's, so a slow
  client cannot pin the cache file's growth.
- The chunk size is a fixed constant, not a flag: it trades two costs that no operator is positioned
  to weigh, and both are bounded at any value in a wide range around it.
