# 0016. Place tests by visibility: handler tests grouped by family, internals tested inline

## Context

The service has two kinds of unit test: black-box tests that drive the gRPC handlers through the public
`CompactTxStreamer` trait, and white-box tests of a module's private helpers and constants. They need
different access, so a single placement rule does not fit both.

## Decision

Place each test where its required visibility is satisfied:

- Handler-level tests that only need the public trait (and `pub(super)` items reachable from
  descendants, such as `collect_utxos`, `MAX_TADDRESS_TXIDS`, `node_tree_state_to_proto`) are grouped
  by method family under `src/service/tests/`, one file per family, mirroring the submodule split
  ([0009](0009-service-per-method-family-modules.md)).
- Tests that reach a module's private items — e.g. `validate_block_range` / `MAX_BLOCK_RANGE` in
  `blocks`, `push_bounded` in `address`, `refresh` in `mempool_monitor` — stay in an inline
  `#[cfg(test)] mod tests` in that module's own file.

## Consequences

- Production visibility stays minimal: a private helper or constant is never widened to
  `pub(super)`/`pub(crate)` merely to be reachable from a separate test tree.
- A method family can therefore have tests in two places (for example, `GetBlockRange` argument
  validation in `tests/blocks.rs`, its cap and pool-type checks inline in `blocks.rs`); the deciding
  question is "does the test need private access?".
- This complements the test *layers* in [0015](0015-layered-testing-strategy.md), which is about the
  kinds of test double and seam rather than where the tests live.
