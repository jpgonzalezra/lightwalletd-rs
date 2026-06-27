# 0006. Darkside mocks the chain at the NodeRpc seam

## Context

Deterministic wallet tests need a controllable chain — scripted reorgs, confirmations, and edge cases
— without waiting on a live node. The wallet-facing service must behave identically whether the data
is real or mock.

## Decision

Inject the mock at the `NodeRpc` seam. In darkside mode the service is built over `DarksideNode`, a
`NodeRpc` implementation backed by an in-memory `DarksideState`, in place of the JSON-RPC client. The
cache, the block-serving methods, and the `CompactTxStreamer` implementation are reused unchanged; a
separate `DarksideStreamer` control plane mutates the shared state.

## Consequences

- Maximal reuse: the wallet-facing path is the production path, so the wallet cannot tell its data is
  mock. The injection reuses the `NodeRpc` seam ([0007](0007-noderpc-seam.md)); no ingestor runs and
  the cache stays empty, so every read falls back to the mock node synchronously.
- Active blocks are held structured (a header plus a list of transaction bytes) and re-serialized on
  demand, so mining a transaction needs no manual length-prefix juggling.
- One exception: `GetSubtreeRoots` is served from roots staged via `SetSubtreeRoots`, the only
  darkside-aware point in `CompactTxStreamer`.
- Darkside is testing-only and must never be enabled in production.
