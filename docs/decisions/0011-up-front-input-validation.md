# 0011. Reject malformed requests up front

## Context

Relying on proto3 defaults turns a missing field into a degenerate-but-successful response — a
`GetBlockRange` with no bounds would stream from height 0, a `GetBlock` with `height == 0` would return
genesis — instead of an error the caller can act on.

## Decision

Validate each request in its handler before any node round-trip or stream is opened, returning
`InvalidArgument` (or the appropriate status) for malformed input. For streaming methods the check runs
synchronously before the stream is built, so the error surfaces as the RPC status rather than partway
through the stream. Transparent addresses are format-checked locally (a `t` followed by 34 alphanumeric
characters) without adding a regex dependency.

## Consequences

- Malformed requests fail fast and predictably, matching what real wallets expect.
- The local address check is only a fast format gate: a well-formed address can still be rejected by
  the node and mapped through [0010](0010-node-error-grpc-mapping.md), which stays authoritative on the
  checksum.
