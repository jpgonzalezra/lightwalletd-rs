# 0002. Parse transactions with librustzcash, hand-parse only block framing

## Context

Turning a raw block into a `CompactBlock` requires decoding every transaction, including v5
transactions with Orchard actions. A full transaction parser is large and security-sensitive, and
re-implementing one would duplicate consensus-critical logic.

## Decision

Parse transactions with `librustzcash` (`zcash_primitives` and friends). Hand-parse only the
fixed-layout block header and the block framing — the transaction count and the per-transaction byte
slicing.

## Consequences

- The hand-written surface is small: just the header fields and the framing arithmetic.
- That arithmetic is fuzzed with `proptest` under two strategies — arbitrary byte buffers and mutated
  real blocks. The invariant is that any input yields `Ok` or `Err(ParseError)` (never a panic,
  out-of-range slice, or overflow), and that a successful split reassembles into the original bytes
  exactly. `proptest` was chosen over `cargo-fuzz` so the property tests ride the normal
  `cargo test` / CI pipeline on stable, with no separate fuzz target.
