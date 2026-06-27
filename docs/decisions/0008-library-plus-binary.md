# 0008. Ship as a library plus a thin binary

## Context

Rust integration tests compile as a separate crate and can only see a crate's public API. A
binary-only crate exposes nothing, so the gRPC server could not be driven end-to-end from `tests/`.

## Decision

Build the crate as both a library and a binary. `src/lib.rs` is the library root: it declares the
modules and exposes `run(config)` (the startup entrypoint) plus the `darkside_components` constructor
that both `run` and the test harness use to wire the darkside stack. `src/main.rs` is a thin wrapper
that parses the CLI, initializes tracing, and calls `run`. The `.proto` files generate both server and
client code, so tests get `CompactTxStreamerClient`/`DarksideStreamerClient` for free. It stays a
single crate rather than a multi-crate workspace.

## Consequences

- Integration tests link against the crate's API and drive the real server in-process (see
  [0015](0015-layered-testing-strategy.md)).
- The public surface is kept deliberately small — internal-only modules stay private — so the library
  API is intentional, not incidental.
