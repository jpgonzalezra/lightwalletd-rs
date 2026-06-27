# 0014. Cache and ingestor recover from corruption and reorgs locally

## Context

The on-disk cache ([0004](0004-redb-block-cache.md)) must survive an interrupted write, a node that is
slow to start, and chain reorgs — without a full wipe-and-redownload on every symptom.

## Decision

Layer logical invariants on top of `redb`'s page integrity: `add` is a strict, monotonic append, and an
O(log n) open-time check verifies the height range has no gaps. On a detected symptom, localize the
lowest corrupt height and truncate from there, then re-ingest — corruption is modeled as a contiguous
suffix (an interrupted final write) or a schema-wide decode failure visible at the tip, so localization
is cheap. At startup the node connection is retried with capped exponential backoff (escalating the log
level rather than exiting). The ingestor detects same-height tip reorgs by comparing the tip *hash*
(not only the height) and verifies each fetched block's height. Operators can force a re-sync with
`--sync-from-height` or `--redownload`.

## Consequences

- Recovery is localized and bounded — it can never spin at full CPU — and a slow-to-start node is
  waited out instead of crashing the server.
- Same-height tip reorgs are caught immediately, not only once the height advances.
- Arbitrary mid-cache corruption is explicitly out of scope; `redb`'s page checksums and transactional,
  strict-append writes make it impractical.
