# 0009. Service is split into per-method-family submodules

## Context

The `CompactTxStreamer` implementation grew past the point where one file was comfortable to navigate,
and the darkside wiring was duplicated between the live startup path and the test harness.

## Decision

Split `src/service/` into one submodule per method family (`chain`, `blocks`, `transactions`,
`address`, `mempool`, `treestate`, `subtrees`, `ping`); `mod.rs` holds the `Streamer` and a thin trait
implementation that dispatches each method to its family. Small cross-cutting helpers like `block_at`,
`decode_hex`, and `mined_height` live in `mod.rs`; error translation lives in `errors.rs`
([0010](0010-node-error-grpc-mapping.md)), and the `BoxStream<T>` alias is shared from `proto`. The
duplicated darkside wiring is extracted into the single `darkside_components` constructor.

## Consequences

- Each method family is found and changed in isolation; no production file is oversized.
- The split is a pure reorganization: the gRPC contract is unchanged, which the over-the-wire E2E
  tests guarantee independently of internal structure.
