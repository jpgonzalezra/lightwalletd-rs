# 0025. Pin an open-ended transparent-address range to the chain tip and bound its span

## Context

`GetTaddressTransactions` (and its deprecated alias `GetTaddressTxids`) takes a `BlockRange` whose
`end` is optional in the protocol, and light-client SDKs do send requests without one. The service
passed that absence through as height `0`, which both backends read as "no upper bound": the JSON-RPC
backend omits the `end` key from `getaddresstxids`, and the readstate backend substitutes the tip. In
either case the node scans its address index open-endedly, and the scan gets more expensive with every
block the chain grows, for a request whose cost the caller never stated.

Two related gaps sat next to it. An `end` near `u64::MAX` was forwarded to the backend as given, and
nothing bounded how long the resulting work could run: a scan the client had abandoned kept a node
connection busy until the node itself finished. [0013](0013-resource-limits.md) already caps the
per-txid fan-out at `MAX_TADDRESS_TXIDS`, but that cap only applies once the scan has returned, which
is where the cost actually is.

## Decision

Resolve and bound the range in the service layer, before any address-index scan is issued.

- An `end` that is absent **or zero** resolves to the current chain tip, read from
  `getblockchaininfo` at request time. Zero has to count as unset rather than as height zero:
  it is what the wire carries for an omitted bound, and treating it as a real bound would leave the
  open-ended scan in place while looking like it had been fixed.
- A span wider than `MAX_TADDRESS_BLOCK_SPAN` (10,000,000 blocks) is rejected with `InvalidArgument`.
  The cap is deliberately generous, well beyond a full-history scan of the current chain, so it never
  rejects a legitimate wallet request while still rejecting an absurd `end`.
- A single deadline, `TADDRESS_SCAN_DEADLINE` (30 s), covers all the node work one request can
  trigger: the tip lookup, the address-index scan, and the per-txid fetches the response streams.
  Expiry maps to `DeadlineExceeded`.

The backends keep their existing reading of a zero `end` as open-ended; the bound lives at the service
layer, which is the one place both backends pass through, so each now receives a concrete upper bound
and the two agree by construction.

## Consequences

- The observable contract changes: an open-ended request is answered against the tip as of the moment
  the request was received. A transaction mined while the response streams is no longer included, and
  a wallet that wants it issues a new request. In practice the window is small, since the previous
  behavior also scanned to whatever the tip was when the scan reached it.
- Clients may keep sending open-ended requests, so no wallet needs to change.
- An open-ended request costs one extra `getblockchaininfo` round-trip. A request that states its own
  `end` costs nothing extra, and an absurd one is rejected without touching the node at all.
- A client that consumes the response stream very slowly can now be cut off with `DeadlineExceeded`
  after 30 s. That is the intended trade: the alternative is letting an abandoned request pin a node
  connection for as long as the node is willing to work on it.
- The caps stay module-local constants, consistent with [0013](0013-resource-limits.md); making them
  operator-tunable would mean threading a limits struct through the service layer.
