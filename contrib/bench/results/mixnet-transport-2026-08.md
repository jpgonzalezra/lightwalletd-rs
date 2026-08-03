# Serving CompactTxStreamer over a mixnet: measurements

Date: 2026-08-02 to 2026-08-03. Evidence for
[ADR 0029](../../../docs/decisions/0029-mixnet-transport-scope.md). Companion to
[`compact-block-wire-size-2026-08.md`](compact-block-wire-size-2026-08.md), which bounds how many
bytes a sync moves; this one measures what a mixnet does to them.

## Method

Two throwaway binaries (`contrib/nym/`), each a byte pipe, with the mixnet between them:

```
ghz --> [dial] --> Nym mixnet --> [sp] --> lightwalletd-rs
```

Neither understands gRPC. `MixnetStream` implements `AsyncRead + AsyncWrite`, so gRPC travels
through unmodified and the load client needs no changes. `dial` exposes a local TCP port and opens
one mixnet stream per inbound connection; `sp` accepts streams and pipes each to the ordinary gRPC
listener.

Everything ran in containers. Load came from the same `ghz` image the hot read-path harness uses, so
the numbers follow [ADR 0017](../../../docs/decisions/0017-benchmark-methodology.md) and compare with
the existing tables.

`GetBlock` was chosen for latency deliberately: it is served from the cache and never touches the
node, so the backend is out of the measured path. `GetLightdInfo` would have been wrong, since it
issues two node calls per request.

Server: `lightwalletd-rs` on the host with a warm mainnet cache. Client, service provider and load
generator on the same machine, so the only thing between the two ends is the mixnet.

## Unary latency

`GetBlock` from cache, one persistent connection, 200 sequential calls, direct against the listener
and again through the mixnet:

| | direct | mixnet |
|---|---|---|
| fastest | 0.20 ms | 1.01 s |
| p50 | 0.29 ms | **1.39 s** |
| p90 | 0.36 ms | 2.88 s |
| p99 | 1.19 ms | **3.42 s** |
| slowest | 4.91 ms | 4.55 s |

All 200 completed in both runs. Sub-millisecond direct figures confirm the node was not in the path.

A second session the following morning, sampled in 2-minute windows over 36 minutes, measured p50
between 2.3 s and 3.0 s and p99 between 3.3 s and 7.1 s. The same rig, roughly twice as slow: the
transport varies substantially with time of day, and no single session should be treated as
representative.

Published Nym figures suggest an average 15 ms delay per hop, so tens of milliseconds over five
hops. The measured median is roughly twenty times that. The gap is unexplained here; it may be the
rate at which a client is allowed to dispatch packets rather than the mixing delay itself.

## Reply-block budget

Every reply packet consumes one single-use reply block, and `open_stream` attaches a fixed number per
outbound message (default 10). A `GetBlockRange` of 1,000 recent blocks is about 1.3 MiB, roughly 660
packets, so replenishment is not an edge case: even a single day of catch-up needs around 760.

Fixed 1,000-block range, three repetitions per value, dialer recreated per run so each starts from a
clean ephemeral budget, 180 s timeout:

| budget | runs |
|---|---|
| 25 | timeout, 25 s, 25 s |
| 50 | 22 s, (rig failed to start), timeout |
| 100 | (rig failed to start), 26 s, 29 s |
| 200 | 36 s, 38 s, 35 s |
| 400 | 68 s, 59 s, 54 s |

**Above 100 the cost grows roughly linearly with the budget**, reproducibly three times out of three:
200 to 400 adds about 24 s. Sending reply blocks upfront is itself bandwidth, and over-provisioning
is paid for. An earlier single-sample pass at 1,000 did not complete within 30 minutes, consistent
with that line.

**No budget-dependent hang is claimed at the low end.** The two timeouts at 25 and 50 initially read
as one, but the establishment-failure rate measured below (about 17%) predicts roughly 2.5 random
failures across 15 runs, and exactly 2 were observed. With three repetitions per point, their landing
in the low rows is compatible with chance.

At a budget of 100, 1,000 blocks in ~27 s is about 48 KB/s, near 400 kbps: well under the ~1 Mbps
nominal figure but real. That extrapolates to ~30 s for a day of catch-up, ~4 hours for a year of
wallet history, and ~11 days for a full sync from Sapling activation.

## Stream establishment fails about one time in six

Eighteen consecutive 2-minute windows, one connection each, budget 100:

| windows | bytes sent | bytes received |
|---|---|---|
| normal (15) | ~3,000 to 3,800 | ~10,000 to 13,000 |
| failed (3) | **253** | **33** |

253 bytes out and 33 back is one request and no real response; 33 bytes is HTTP/2 control framing,
not a compact block. Each failure surfaced to the caller as one `DeadlineExceeded` followed by one
`Unavailable` ("use of closed network connection").

The service provider reported no errors, and all 18 connections closed cleanly with zero aborts. So
these are not streams dying mid-life. **They open, accept a request, and never deliver anything.**

Three of eighteen, about 17%. A client without detect-and-retry appears broken at that rate.

Separately, the Nym client failed to register with a gateway within two minutes in 2 of 15 attempts,
an unrelated startup failure that matters for operating a long-lived service provider.

## Epochs have no observable effect

Nym's API (`/api/v1/epoch/current`) reports epochs of exactly 3600 s beginning at :26:19 past the
hour, and reply blocks are only valid within their own epoch, so the boundary was a plausible cause
of disruption. Timing the sweep against a known boundary rather than guessing:

The window from 10:24:35 to 10:26:35 contained the 10:26:19 boundary and returned 46 OK calls with
p50 2.38 s. Median latency stayed between 2.3 s and 3.0 s across the whole 36 minutes with no step.

An apparent correlation observed the previous day, before the schedule was known, did not survive
timing the experiment. It was the establishment failure above, which is frequent enough to land near
anything.

## Negative results

Recorded so they are not re-derived:

- **Epoch boundaries produce no client-visible effect**, at least for a client making repeated short
  calls.
- **Compressing compact blocks is not worth it.** Per-message compression, which is what gRPC
  compression does, buys 1.06x on recent blocks and 1.02x on the era that actually costs bandwidth.
  Whole-stream compression reaches 1.37x, a modest saving rather than an enabler. Details in the
  companion report.
- **Packet alignment is a non-question.** The transport exposes a byte stream, so fixed-size packets
  fill contiguously and there is nothing to align. Only the packet count matters, because it is the
  reply-block count.

## Limitations

- One network path, one pair of gateways, two sessions on consecutive days. Between them the median
  nearly doubled, so absolute figures should be treated as one sample of a wide distribution.
- Three repetitions per budget value cannot pin down a failure rate; the 17% figure comes from a
  separate 18-window run and is itself a small sample.
- No single stream lived longer than two minutes, so the behaviour of a long-lived stream crossing an
  epoch boundary remains untested. It matters little in practice, since the establishment-failure
  rate already forces retry and resumption.
- Latency was measured with `GetBlock`; streaming throughput with `GetBlockRange`. Other methods were
  not profiled individually.
