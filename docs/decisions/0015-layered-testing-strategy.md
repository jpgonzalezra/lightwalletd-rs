# 0015. Layered testing: fakes, golden fixtures, and in-process E2E

## Context

The proxy must be correct about RPC↔gRPC translation, byte-exact parsing, reorg handling, and the
end-to-end wallet contract — all without depending on a live node in CI.

## Decision

Test in layers against the `NodeRpc` seam ([0007](0007-noderpc-seam.md)):

- a `FakeNode` with canned responses covers the `service`, `ingestor`, and `fetch` translation logic;
- a `wiremock` HTTP server covers the `NodeClient` JSON layer (envelopes, Basic auth, error parsing);
- golden fixtures in `testdata/` pin the parser byte-for-byte, and the hand-written framing is
  additionally fuzzed with `proptest` ([0002](0002-parse-blocks-with-librustzcash.md));
- end-to-end tests drive the real server in darkside mode over the wire, in-process, through the public
  library API — deterministic and network-free. A manual `grpcurl` smoke test against real vectors
  stays out of CI.

## Consequences

- CI is deterministic and needs no network or node.
- The E2E tests exercise the production read path, so internal refactors are caught by behaviour, not
  structure.
