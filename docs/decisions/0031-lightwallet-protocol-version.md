# 0031. Report the served lightwallet-protocol version as a constant

## Context

`LightdInfo` carries a `lightwalletProtocolVersion` field (`proto/service.proto:116`), and this server
left it at its default: the empty string.

The field is not informational. `BlockRange.poolTypes` and this field were introduced together, in
lightwallet-protocol v0.4.0, and the protocol ties them explicitly — `proto/service.proto:35-38` makes
checking the server's version a client requirement before setting `poolTypes` to a non-empty value.
The version string is the means by which a client discharges that requirement.

An empty value is therefore not "unknown", it is indistinguishable from a pre-v0.4.0 server. This
server prunes to the requested pools ([0011](0011-up-front-input-validation.md) validates the field,
`src/filter.rs` applies it), returns transparent `vin`/`vout` when `TRANSPARENT` is requested, serves
`ironwoodActions`, `ironwoodTree` and Ironwood subtree roots. A client honoring the requirement could
see none of that and had to fall back to shielded-only scanning, or be told out of band by the
operator to disregard the probe.

The vendored `proto/` set is lightwallet-protocol v0.5.0 verbatim, minus the `go_package` and
`swift_prefix` options, which bind the generated code to other languages, plus one added comment noting
that Ironwood actions reuse the `CompactOrchardAction` encoding. So v0.5.0 is what is served.

## Decision

Report `v0.5.0` from `LIGHTWALLET_PROTOCOL_VERSION` in `src/service/chain.rs`.

- It is a **constant**, not one of the build-stamped values next to it (`GIT_COMMIT` via `build.rs`,
  `CARGO_PKG_VERSION`). Those describe the binary; this describes the protocol served. A value the
  build could overwrite would defeat the purpose, since the protocol version does not move when the
  build does.
- It is **not derived from the crate version** either. The two are independent: a release can add
  caching or operational surface without touching the wire contract, and the protocol can move without
  a release of ours.
- It moves **only once the server actually serves everything the named version specifies**, in lockstep
  with the `proto/` set. Vendoring a newer `.proto` is not enough on its own — the claim is what a
  client acts on.
- The reported string keeps the upstream tag's leading `v`, matching the tag names in
  lightwallet-protocol and the value the Go implementation reports, so clients comparing across servers
  see one form rather than two.

The remaining unset `LightdInfo` fields — `branch`, `buildDate`, `buildUser` — are left alone. They
describe a build, not the protocol, and nothing in the contract depends on them.

## Consequences

- A client can now distinguish this server from one that predates pool-type filtering, and enable
  transparent and Ironwood scanning from the probe alone, with no out-of-band assertion from the
  operator.
- The constant is an assertion the implementation has to keep true. Bumping the vendored `proto/` set
  without bumping it under-reports; bumping it ahead of the implementation tells clients to request
  data this server will not return. A test in `src/service/tests/chain.rs` pins the reported value, so
  the change is deliberate rather than incidental.
- The version is stated in exactly one place. `docs/protocol-references.md` points at it, so the
  vendored protos, the constant and the docs are read together.
