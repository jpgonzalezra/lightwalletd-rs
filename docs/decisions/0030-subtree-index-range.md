# 0030. Bound the subtree index range at the service boundary

## Context

`GetSubtreeRoots` carries `startIndex` and `maxEntries` as `uint32`, but the backend node addresses
subtrees with a narrower type: zebrad's `z_get_subtrees_by_index` takes a `NoteCommitmentSubtreeIndex`,
a `u16`. The service forwarded both values unchecked, so anything above 65,535 failed at the node with
`Invalid params`.

That error carries the generic `Misc` JSON-RPC code, which
[0010](0010-node-error-grpc-mapping.md) does not map, so it fell through to the default and reached the
client as `Unavailable`. In gRPC that code is retryable and means the server is down: an SDK backs off
and retries, so malformed client input produced a retry loop against a request that can never succeed,
while marking a healthy server as unavailable to health checks and load balancers.

The node draws the distinction precisely, and it is worth mirroring. An index it can represent but
that lies past the last subtree returns an empty list with no error; only a value it cannot represent
is an error. Verified against a live node (v6.2.2): `start_index` of 5, 5,000 and 65,535 all return
`{"subtrees": []}`, while 65,536 and above return `Invalid params`.

This survived two earlier passes, both of which used the Go implementation as their reference.
[0011](0011-up-front-input-validation.md) audited input validation and treated this method as covered
because it validates the *protocol* field; index ranges were not considered. Go has the same gap, so
parity could not reveal it. Only the node's own parameter type does.

## Decision

Bound the range in the handler, before any node round-trip, per [0011](0011-up-front-input-validation.md).

- A `startIndex` above `u16::MAX` is rejected with `InvalidArgument`. Returning an empty stream would
  also be defensible, but an index that large is unreachable through chain growth (it would take 2^32
  note commitments) and so only ever indicates a client bug. Reporting it as "no data" would hide that
  bug behind a plausible-looking answer.
- A `maxEntries` above `u16::MAX` is mapped to the unlimited `0` rather than rejected. The asymmetry
  is deliberate: an index is a position, so out of range means it does not exist, while a limit is a
  ceiling, so out of range means "all of them" — a request `0` already expresses by omitting the
  limit. Clamping to `u16::MAX` would instead cap the count one short of the 65,536 subtrees a full
  pool can hold (indexes 0 through 65,535).
- Both checks run before the darkside branch, so the mock and node-backed backends answer an
  out-of-range request identically. This follows the reasoning in
  [0025](0025-taddress-range-bounds.md), which likewise bounds at the service layer precisely because
  it is the one place both backends pass through.
- The generic `NodeError` mapping is left alone. Catching `Invalid params` there would mean matching on
  message text, which [0010](0010-node-error-grpc-mapping.md) deliberately avoids because node error
  strings are not stable across versions. Validating the boundary removes the reachable cause instead
  of adding a fragile net downstream of it.

The existing `invalid pool name` handling is unchanged and remains a separate case: it signals a
pre-NU6.3 node that cannot know the pool, not bad client input, and stays an empty stream.

## Consequences

- A wallet sending an out-of-range index now gets a terminal `InvalidArgument` naming the bound,
  instead of a retryable `Unavailable` it would loop on. Nothing that previously succeeded changes.
- The bound is the node's constraint, not ours, so `MAX_SUBTREE_INDEX` is documented as tracking
  zebrad's parameter type. Should the node ever widen it, the constant is the single place to follow.
- An out-of-range request costs no node round-trip at all.
- A residual `Invalid params` from any method still surfaces as `Unavailable`. That is now understood
  to mean a bug on our side rather than client input, and is left for whichever method it appears in
  to handle with the numeric code, per [0010](0010-node-error-grpc-mapping.md).
- The protocol keeps its `uint32` fields. They are defined upstream, and validating against the node's
  real domain is cheaper and less disruptive than changing a wire type shared across implementations.
