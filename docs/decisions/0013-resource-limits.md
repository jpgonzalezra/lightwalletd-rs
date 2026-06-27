# 0013. Bound the resources a client can hold or accumulate

## Context

A public-facing deployment must not be at the mercy of a few abusive peers. The levers are unbounded
concurrent long-lived streams, connections held open by dead peers, and unbounded per-request
accumulation.

## Decision

Set two kinds of limit. The shared server builder caps in-flight requests and HTTP/2 streams per
connection and applies TCP / HTTP-2 keepalive, all configurable at startup
(`--max-concurrent-streams`, `--keepalive-interval-secs`, `--keepalive-timeout-secs`, defaulting to
256 / 60 s / 20 s). Three per-request caps bound accumulation — `MAX_BLOCK_RANGE`,
`MAX_STREAMED_ADDRESSES`, and `MAX_TADDRESS_TXIDS` (all 10,000) — and remain module-local constants
with generous defaults.

## Consequences

- A dead peer is pinged and dropped, so it cannot pin a long-lived stream; a single request cannot
  trigger an unbounded per-txid fetch loop.
- The server-builder limits are operator-tunable; the per-request caps are deliberately left as
  constants for now, since making them configurable would mean threading a limits struct through the
  service layer.
