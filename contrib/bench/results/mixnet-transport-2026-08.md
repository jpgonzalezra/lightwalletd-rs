# Serving CompactTxStreamer over a mixnet: measurements

Date: 2026-08-02 to 2026-08-03, with a verification run on 2026-08-17. Evidence for
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
as one, but the 18-window run described below failed three times out of eighteen, about 17%, and
that rate predicts roughly 2.5 random failures across these 15 runs. Exactly 2 were observed. With
three repetitions per point, their landing in the low rows is compatible with chance.

At a budget of 100, 1,000 blocks in ~27 s is about 48 KB/s, near 400 kbps: well under the ~1 Mbps
nominal figure but real. That extrapolates to ~30 s for a day of catch-up, ~4 hours for a year of
wallet history, and ~11 days for a full sync from Sapling activation.

## Streams fail silently, at a rate that moves by an order of magnitude

First seen through the rig: across 18 consecutive 2-minute windows at budget 100, three carried 253
bytes out and 33 back, which is one request and no real response (33 bytes is HTTP/2 control
framing). All 18 connections closed cleanly with zero aborts, so these were not streams dying
mid-life. Three of eighteen is about 17%, which is the rate the budget sweep above is checked
against.

Because that measurement passed through four layers, it could not attribute the fault. A minimal
reproduction (`src/bin/repro.rs` in the rig) removes them all: two mixnet clients in one process, one
echoing 64 bytes and one dialling, no gRPC, no HTTP/2, no proxies, no shared client behind a mutex.
The echo side counts three stages separately, which turns a failure into a direction.

**It reproduces.** The defect is in the SDK or the network, not in the rig.

| run | version | trials | failure rate |
|---|---|---|---|
| 2026-08-03 afternoon | 1.21.4 | 200 | 7.0% |
| 2026-08-03 afternoon | 1.21.5-rc.3 | 200 | 2.0% |
| 2026-08-03 evening | 1.21.5-rc.3 | 100 | 2.0% |
| 2026-08-04 | 1.21.5-rc.3 | 400 | **36.5%** |
| 2026-08-17, budget 100 only | 1.21.5-rc.3 | 100 | 24.0% |

The signature is consistent: the far side's `accept()` fires, the sender's `write_all` and `flush`
both return `Ok`, and the payload never arrives. Neither end errors and neither times out; both hang
until an external deadline. Under the degraded conditions of the last run, loss also appeared on the
return path (34 of 400), where the echo side read and replied successfully and the reply never
arrived.

The rate is **not stationary**, and that matters more than its value. Within the 400-trial run,
failures nearly tripled between the first and second halves (40 then 106). Across days at identical
settings it moved between 2% and 36.5%.

### Still there two weeks later

The 2026-08-17 row is a verification run on the same version, single budget, and it gives the
cleanest attribution of the set: the echo side accepted all 100 streams and read only 76. All 24
failures were outbound, with nothing on the return path and no dialler errors, so there is nothing
approximate to attribute. Latency had drifted the other way, p50 5,677 ms against 4,054 ms at the
same budget on 2026-08-04.

That run also shows how hard the acknowledgement machinery works while failing to converge. Over 100
trials the client logged 2,625 `retransmitting normal packet`, 703 chunks arriving after their ack
was lost, and 793 duplicate fragments. Roughly 26 retransmissions per trial, and 24 payloads still
never arrived and never errored. Raw log:
[`raw/2026-08-17-repro-budget100.txt`](../../nym/raw/2026-08-17-repro-budget100.txt).

### The reply-block budget shifts it, but does not fix it

An earlier sweep ran each budget as its own block, which a drifting network makes uninterpretable.
Rerun with the budgets rotated one per trial inside a single process, so drift hits every value
equally, 100 trials each:

| reply blocks | fragments per message | failures | p50 |
|---|---|---|---|
| 1 | 1 | **51%** | 1,412 ms |
| 20 | 6 | 34% | 1,863 ms |
| 100 | 29 | 35% | 4,054 ms |
| 400 | 115 | **26%** | 7,529 ms |

More reply blocks means fewer failures, monotonically, which is consistent with the far side running
out of them: acknowledgements and retransmissions consume the budget alongside the reply itself. But
the effect is second-order next to conditions, the best observed setting still failed 26% of the
time, and buying that took 5.3x the latency.

**A fragmentation hypothesis was tested and refuted.** Each attached reply block is a precomputed
Sphinx header, so the budget inflates every outbound message: 64 bytes of payload ships as 1, 6, 29
or 115 fragments at the budgets above, and a message only reassembles once every fragment arrives.
That predicted more fragments would mean more failures. The opposite holds: the most fragmented
configuration was the most reliable.

### What the SDK's own logs show

Tracing a 100-trial run at budget 100:

- Every message split into **29 fragments** to carry 64 bytes of payload, the rest being reply blocks.
- Reply blocks accumulated unboundedly on the receiving side, from 100 to **19,903** over 200
  messages, since each message attaches 100 and roughly one is used.
- Acknowledgement loss is routine: 16 chunks arrived as duplicates "because the ack got lost", and 17
  pending acks were already gone when their removal was attempted. Retransmission machinery exists
  and fires (21 events), so most losses do recover.
- Two internal errors surfaced across runs, both from the client's own code:
  `failed to send mixnet packet due to closed channel (outside of shutdown!)` and, on the gateway
  connection, `Broken pipe` / `Connection reset without closing handshake`. The first is the SDK
  flagging its own invariant violation.

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

- One network path, one pair of gateways, one machine, across three days. Conditions moved by more
  than an order of magnitude within that, so every absolute figure here is one sample of a wide and
  drifting distribution rather than a property of the transport.
- The budget comparison is interleaved and therefore internally valid, but it is still a single run
  under one day's conditions. Its ordering should hold; its absolute rates should not be quoted.
- The 17% used to defuse the two low-budget timeouts comes from the 18-window run, which is a small
  sample on top of a rate that moves by an order of magnitude. It supports "compatible with chance"
  and nothing stronger.
- Direction of loss is approximate. The reproduction credits a trial by sampling counters the echo
  side advances from spawned tasks, so a stage arriving after that trial's deadline lands on the
  next one. Totals such as the 34 return-path losses are therefore indicative; the aggregate failure
  rate, which only counts deadlines, is not affected.
- Why the rate varies so much between sessions is unexplained. It is the most important open question
  about this transport and nothing here answers it.
- No single stream lived longer than two minutes, so the behaviour of a long-lived stream crossing an
  epoch boundary remains untested. It matters little in practice, since the establishment-failure
  rate already forces retry and resumption.
- Latency was measured with `GetBlock`; streaming throughput with `GetBlockRange`. Other methods were
  not profiled individually.
