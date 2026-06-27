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
