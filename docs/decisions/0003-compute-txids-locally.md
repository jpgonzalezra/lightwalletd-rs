# 0003. Compute transaction IDs locally

## Context

A wallet needs the transaction ID of every transaction in a compact block. The node can return txids
through a verbose `getblock`, but verbose responses are large and add load proportional to block size.

## Decision

Compute transaction IDs locally from the raw block bytes via `librustzcash`, including v5 / Orchard
IDs (ZIP-244). A single non-verbose `getblock` per block then supplies all transaction data; one extra
verbose call per block is still made — for the note-commitment tree sizes (`ChainMetadata`), which are
not part of the raw block, and for the canonical block hash, which the raw fetch is keyed by so both
calls refer to the same block even across a reorg.

## Consequences

- Lower RPC load: the bulk per-block fetch is the compact, non-verbose form.
- Correctness depends on the local txid implementation. It is validated byte-for-byte against the
  golden fixtures in `testdata/`, which also assert that a real txid is computed for every transaction.
- The residual verbose call could later be dropped by tracking the note-commitment tree sizes
  incrementally in the ingestor; this optimization is deferred.
