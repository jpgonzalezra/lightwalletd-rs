# 0010. Map node errors to per-method gRPC status codes

## Context

A backend JSON-RPC error carries a numeric code, but collapsing every failure into a single
`Unavailable` status hides the distinction a wallet needs: a height past the tip, an unknown
transaction, and a malformed address are all different conditions.

## Decision

Translate node errors to a gRPC `Status` per method family, in `src/service/errors.rs`, keyed primarily
to the numeric JSON-RPC code rather than the message text (messages are not stable across node
versions):

- height past the chain tip (`-8`) → `OutOfRange` for the block-serving methods;
- unknown transaction (`-5`) → `NotFound` for transaction lookups;
- transparent address (`-5`) → `InvalidArgument`, except a `-5` carrying "no information available",
  which maps to `NotFound` — the one spot where the message text is consulted.

Because `-5` is method-ambiguous (missing transaction vs. invalid address), the mapping is applied per
family; anything unrecognized keeps a safe default — `Unavailable` for a node/transport error
(including an undecodable node response), and `Internal` for a local block-parse or cache failure.

## Consequences

- Wallets receive the status code each condition warrants.
- The mapping is keyed to the actual numeric codes the backend returns, pinned by unit tests that
  inject a per-RPC error.
- The "`Internal` for a local block-parse or cache failure" default extends to `fetch`'s txid
  cross-check ([0020](0020-windowed-ingest-batched-commits.md)): `FetchError::TxidMismatch` and
  `TxCountMismatch` also map to `Internal`, since a computed-vs-node txid divergence is an integrity
  failure on our side, not a retryable node condition — consistent with, not an exception to, this
  ADR's default.
- "The one spot where the message text is consulted" describes the *status-mapping* helpers in
  `src/service/errors.rs` specifically. `src/service/subtrees.rs` separately matches the
  `"invalid pool name"` substring to turn a pre-NU6.3 node's Ironwood-subtree error into an empty
  stream rather than a `Status` — a message-text check outside this ADR's mapping table, not a second
  exception within it.
