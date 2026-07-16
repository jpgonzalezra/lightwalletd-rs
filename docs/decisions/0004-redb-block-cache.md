# 0004. On-disk block cache backed by redb

## Context

Compact blocks must be served quickly and survive restarts, and the cache must handle chain reorgs
without an expensive rebuild.

## Decision

Cache compact blocks on disk with `redb`: one protobuf-encoded `CompactBlock` per height, keyed by
height. A reorg is a truncate-from-N — drop every height above the fork point and let the ingestor
refill.

## Consequences

- Ordered keys make the tip cheap to read and a reorg a single range delete.
- `redb` provides page-level integrity and transactional atomicity, so the cache adds only the logical
  invariants on top: `add` is a strict, monotonic append that rejects a height/key mismatch or a
  non-monotonic write with a corruption error rather than persisting it.
- On open, an O(log n) check decodes the tip and verifies the height range has no gaps; a detected
  symptom truncates from the corrupt point and re-ingestion refills.
- Choosing `redb` over a hand-rolled flat-file scheme means most integrity machinery (checksums,
  partial-write detection) comes from the store; only the logical invariants above are added on top.
  The full corruption/reorg resilience model builds on this layout — see
  [0014](0014-cache-ingestor-resilience.md).
- `add` is now the single-block case of `add_batch`, which writes a whole window of consecutive
  blocks in one `redb` transaction — one commit, one fsync, per window rather than per block (see
  [0020](0020-windowed-ingest-batched-commits.md)). This amortizes fsync cost during catch-up; it does
  not weaken durability, since every commit that does happen is still fully synced, and the same
  monotonic-append invariant is enforced across the whole batch atomically (an aborted batch writes
  nothing).
