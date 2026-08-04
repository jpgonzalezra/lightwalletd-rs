# 0027. Serve only self-consistent block ranges

## Context

`GetBlockRange` and `GetBlockRangeNullifiers` resolve each height in the requested span
independently, from the cache when it holds the block and from the node otherwise
([0004](0004-redb-block-cache.md)). Those two sources are not always describing the same chain.

After a reorg the ingestor rolls the cache back one block per step
([0014](0014-cache-ingestor-resilience.md)), so for as many steps as the reorg is deep the cache
still holds blocks of the abandoned fork while the node already serves the new one. A range that
spans the boundary then splices the two: a parent from the fork that lost, a child from the fork that
won, describing a chain that never existed. Nothing in the response marks the seam, so a wallet has
no reason to distrust it, and the trial-decryption and nullifier state it derives from those blocks
is built on a chain no node agrees with.

The same seam can open inside the cached portion alone, because each height was resolved in its own
read transaction and the ingestor can truncate between two of them. That half is addressed
separately in [0028](0028-mvcc-chunked-cache-reads.md); this record is about what the response
promises regardless of where the blocks came from.

## Decision

Verify that consecutive blocks connect, and fail the stream rather than serve a span that does not.

Each block establishes the hash the next one must carry: ascending, the next block's `prev_hash` must
equal this block's `hash`; descending, the next block's `hash` must equal this block's `prev_hash`.
A mismatch ends the stream with `Aborted`, after the blocks already sent, which are the ones that did
connect.

A detected mismatch also reports the lower of the two heights on a shared repair signal that the
ingestor drains at the top of its loop. The ingestor truncates the cache from that height, so
re-ingestion refills it from the node's chain. Reporting the *lower* height drops both sides of the
seam whichever one is stale. Without this, the failure would persist for as long as the ingestor
needs to reach those heights on its own and every retry in between would hit the same discontinuity;
with it, the client's retry finds a repaired cache. The read path only reports: the ingestor stays the
cache's single writer, and the repair is counted against the same bound as corruption recovery so a
node the cache cannot reconcile with can never drive an endless truncate/re-ingest cycle.

## Consequences

- A served range is internally consistent: every block connects to the one before it, whichever source
  each came from.
- A wallet syncing across a reorg repair sees a transient `Aborted` and retries, instead of silently
  accepting a spliced chain. Retrying is already how a light wallet handles an interrupted range.
- A discontinuity repairs the cache instead of only reporting it, turning a failure that would recur
  on every retry into a single transient one.
- A range served entirely from a stale portion of the cache still passes the check: those blocks do
  connect to each other, they are just the wrong chain. That case resolves when the ingestor works
  past those heights. Closing it would need an anchor fetched from the node per request, which is a
  cost on the sync path that this decision does not take on.
- The check costs one 32-byte comparison per block and no extra node calls.
