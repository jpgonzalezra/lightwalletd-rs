# 0021. Mempool staleness contract: surface a stale snapshot as `Unavailable`

## Context

The shared mempool monitor ([0005](0005-shared-mempool-monitor.md)) refreshes the mempool at most
once every 2 s and fans the result out to `GetMempoolTx` and `GetMempoolStream` through a
`tokio::sync::watch`. When the node RPC fails, `mempool_monitor::start`'s loop logs and retries on
the next tick, deliberately keeping the last good snapshot published so a single transient failure
(one bad tick, a brief restart) does not interrupt service — the failure-isolation and single-poller
design must stay (review finding H3's fix is not a rollback of it).

The gap: nothing bounds how long "keep the last good snapshot" is allowed to mean. If the node stays
down, the monitor keeps retrying forever, the watch channel keeps serving the same value forever, and
both RPCs keep answering with a well-formed, successful response — no error, no staleness signal —
while the underlying data is arbitrarily old. A wallet cannot distinguish "the mempool is genuinely
quiet" from "the backend has been unreachable for an hour." The Go reference does not have this gap:
it polls the node per request, so a node outage surfaces as an RPC error to the caller immediately.
ADR [0010](0010-node-error-grpc-mapping.md) already establishes the convention that a node/transport
failure the service cannot route around maps to `Unavailable`.

## Decision

Stamp every snapshot with the time of the successful refresh that produced it
(`MempoolSnapshot::refreshed_at`, a `tokio::time::Instant` so it advances correctly under paused-time
tests). The snapshot published before the monitor's first successful refresh carries no timestamp and
is treated as maximally stale, since it was never actually fetched from the node.

Define a cutoff, `mempool_monitor::STALENESS_CUTOFF = 60 s`, and `MempoolSnapshot::is_stale()`
(`refreshed_at` is `None`, or more than 60 s old). Sixty seconds is:

- **Well above the healthy case.** The refresh throttle is 2 s, so under a healthy node a snapshot is
  never more than a few seconds old; 60 s gives roughly 30 consecutive failed ticks of margin, so a
  single slow RPC or a couple of transient retries never trips it.
- **Well below one block interval.** Zcash blocks arrive roughly every 75 s. A cutoff under that keeps
  the signal about node reachability, not block cadence: a real outage is surfaced to wallets within
  the same block interval it starts, instead of being mistaken for "no new block yet" for a whole
  interval or more.

`GetMempoolTx` checks the snapshot it is about to serve and returns `Status::unavailable` instead of
the stale data. `GetMempoolStream` checks the same way at subscribe time, and again on every loop
iteration while the stream is open. The loop iteration needed a second change beyond the check itself:
it previously only woke on `watch::Receiver::changed()`, which fires solely on a new publish — exactly
what stops happening once the node is down, since a failed refresh tick is never published. An
already-open stream would therefore hang on `changed()` forever, having gone stale with no signal,
which is the same failure mode as the initial gap just shifted one layer down. The loop now races
`changed()` against a periodic poll (`STALENESS_POLL_INTERVAL`, matched to the 2 s refresh cadence) in
a `tokio::select!`, so a stalled stream notices staleness within about one poll interval and ends with
the same `Unavailable` status rather than continuing to emit an increasingly old view.

Darkside is unaffected: it has no monitor (`Streamer.mempool == None`) and keeps the per-request path,
which is synchronous with the staged state by construction.

## Consequences

- While the node is healthy, behavior is unchanged: snapshots are always well under the cutoff, so
  every check is a no-op single comparison, the 2 s refresh throttle is untouched, and stream
  semantics (one entry per tx, ends on a tip change) are unchanged.
- A prolonged node outage now surfaces to wallets as `Unavailable` on both mempool RPCs — consistent
  with how the rest of the service reports a node it cannot reach ([0010](0010-node-error-grpc-mapping.md))
  — instead of silently aging the last-known-good data forever.
- Recovery is automatic: the very next successful refresh republishes a freshly stamped snapshot, and
  both RPCs immediately resume serving normally with no separate reset step.
- A client connecting in the narrow window before the monitor's first successful refresh now gets
  `Unavailable` instead of a spuriously "empty" mempool; this is more honest, since no such client was
  ever seeing real data during that window, but it is a small behavior change worth calling out.
- `GetMempoolStream` now wakes at least every `STALENESS_POLL_INTERVAL` even when idle, instead of
  purely on new data. The added wakeups are cheap (a comparison against a stored `Instant`) and only
  matter while a stream is open, so the cost is proportional to connected clients, not node load.
