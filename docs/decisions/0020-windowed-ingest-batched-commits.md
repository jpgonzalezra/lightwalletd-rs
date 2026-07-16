# 0020. Windowed concurrent ingest with batched cache commits

## Context

The original ingestor advanced one block per step, and each step cost one `getblockchaininfo`, one
verbose `getblock`, one raw `getblock`, and one fsynced redb commit. Steady-state (one block per
75 s) was unaffected, but an initial sync is serial in node round-trip latency plus a per-block
fsync: a full mainnet ingest (~3M blocks) was projected to be dramatically slower than the Go
implementation, whose production ingestor prefetches a sliding window of 64 blocks with 8 concurrent
workers. Block fetches for distinct heights are independent, so the serialization bought nothing.

Additionally, the block parse (librustzcash deserialization plus ZIP-244/229 txid hashing) ran on
the async runtime, and computed txids were never checked against the node's view — a silent
divergence would corrupt wallet spend detection (review item H4).

## Decision

Catch-up ingests in **windows**: up to `--ingest-window` (default 64) consecutive blocks are fetched
concurrently, bounded by `--ingest-concurrency` (default 8) in-flight node requests (a `JoinSet`
gated by a semaphore over the existing `NodeRpc` seam). Results are ordered by height and the
longest prefix that chains (`prevHash` links) onto the cached tip is committed via `Cache::add_batch`
in a **single redb transaction** — one commit and one fsync per window instead of per block. The
tip is re-read (`getblockchaininfo`) once per window instead of once per block.

Failure handling is per-height: a fetch failure or mid-window chain mismatch past a non-empty
chained prefix commits the prefix and lets the next step retry the remainder, so partial windows
still make progress. At the tip, the window naturally degrades to a single block and the
one-block-per-detection reorg semantics are unchanged.

The parse moves to `spawn_blocking`, keeping the gRPC serving path responsive during a full-speed
sync. `fetch` additionally cross-checks the locally computed txids against the verbose `getblock`
txid list (when present) and rejects the block on any mismatch (`TxidMismatch`/`TxCountMismatch`,
mapped to `Internal` — an integrity failure, not a retryable node condition).

Two ingestor behaviors change alongside (review items S2/S3):

- **Node behind cache**: previously treated as a reorg, draining the cache one block per step down
  to the node's height — pointing at a re-syncing node destroyed hours of ingested data. Now the
  node's tip hash is compared with the cached block at that height: equal → idle (keep serving);
  different → genuine reorg, roll back one block.
- **Reorg reaching the `--start-height` floor**: previously refused (`ReorgBelowStartHeight`),
  wedging the ingestor in an error loop while serving a stale tip. Now the cache is emptied and
  re-ingestion resumes from `start_height` on the node's chain — an empty cache chains onto
  anything, matching the Go reference's clamp-empty-resume behavior.

## Consequences

- Initial sync is bounded by `concurrency ×` node round-trips and one fsync per window, closing the
  throughput gap with the Go reference; both knobs are operator-tunable per node capacity.
- Worst-case per-step memory is bounded by `concurrency` raw blocks in flight plus one window of
  compact blocks (a few MB at defaults).
- A partially failed window commits its chained prefix, so flaky nodes degrade throughput rather
  than stall progress.
- `add_batch` enforces the same append invariants as `add` (consecutive heights, extends the tip by
  exactly one) atomically — an aborted batch writes nothing.
- The txid cross-check turns a librustzcash/node consensus divergence (e.g. an NU shipping in the
  node before the crate bump, ADR 0019) into a loud ingest failure instead of silently serving
  wrong txids.
- A crash between window commits loses at most one window of un-fsynced progress — nothing, since
  every commit is still durable; batching only amortizes the cost.
