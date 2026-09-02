# Mixnet transport spike

Throwaway rig for measuring what it costs to serve `CompactTxStreamer` over a mixnet. Not part of
the crate, not built by CI, and deliberately its own cargo workspace so `nym-sdk` and its ~750
transitive packages never enter the parent lockfile.

Two halves:

```
measurement tool
      |  plain TCP, 127.0.0.1:9069 on the host
    [dial]      opens a mixnet stream per inbound connection
      |  mixnet
     [sp]       pipes each accepted stream to the upstream
      |  plain TCP, 127.0.0.1:9067
lightwalletd-rs
```

Neither half understands gRPC. A mixnet stream implements `AsyncRead + AsyncWrite`, so gRPC travels
through unmodified and existing tools work against the local port without changes.

## Running

Everything runs in containers; nothing is installed on the host.

Start the server side and note the address it prints:

```
docker compose up --build sp
# NYM_ADDRESS=<identity>.<encryption>@<gateway>
```

Then, with `lightwalletd-rs` listening on the host at `:9067`, start the dialer:

```
SP_ADDRESS=<the address above> docker compose up dial
```

Point any gRPC client at `127.0.0.1:9069`. The dialer listens on 9068 inside its container and
Compose publishes it on 9069, because the server's own metrics endpoint already holds 9068 on the
host. Override with `DIAL_HOST_PORT`.

To sweep the variable under test:

```
SP_ADDRESS=<address> REPLY_SURBS=200 docker compose up dial
```

## What is being measured

Every reply packet the far side sends consumes one reply block. A `GetBlockRange` of 1,000 recent
blocks is roughly 1.3 MiB, which is about 659 packets, against a default budget of 10; even a single
day of catch-up needs around 759. See `contrib/bench/results/compact-block-wire-size-2026-08.md`.

So `REPLY_SURBS` does not really sweep "how many blocks are sent". It sweeps how often
replenishment fires, and each replenishment is an extra round trip across a network that adds
hundreds of milliseconds. The question is whether the curve has a usable knee.

Alongside that: the tail of the latency distribution for a small unary call, and whether a stream
survives crossing an epoch boundary, which lasts an hour and is when reply blocks expire.

## Reproducing the stream failures

`repro` is the third binary and needs neither half above: it runs two mixnet clients in one process,
one echoing a 64-byte payload and one dialling it, with no gRPC, no HTTP/2 and no proxies in the
path. A failure there belongs to the SDK or the network rather than to this rig, which is what makes
it the evidence the reports cite.

```
docker compose run --rm repro
TRIALS=400 BUDGETS=1,20,100,400 docker compose run --rm repro
```

Budgets are rotated one per trial rather than run in blocks, so a drifting network hits every value
equally. It exits when the trials are done and prints a per-budget table.

Two flags exist for the reply-block reserve, the number a receiver holds back before it will spend
any on a reply:

```
docker compose run --rm --entrypoint repro repro --trials 100 --budgets 1 --fresh-dialler --surb-threshold 0
```

`--surb-threshold` sets that reserve through `DebugConfig`. Left unset it takes the SDK's own path,
so the default arm of a comparison is the real default rather than a restatement of it.

`--fresh-dialler` builds a new dialling client for every trial. Reply blocks accumulate per sender
tag and a tag lives as long as the client, so a persistent client stops being short after a few
exchanges and the reserve stops binding. Rotating the dialler makes every trial a first exchange. It
costs one registration per trial, which is slow and occasionally fails, so those registrations are
retried and the count is reported at the end.

The reserve is client configuration, so two settings cannot be interleaved inside one process the way
budgets are. Comparing them means alternating blocks, which
[the measurement](https://github.com/jpgonzalezra/lwd-mixnet-proxy/blob/main/docs/measurements/2026-08-27-surb-reserve-costs-nothing.md)
explains.

## The establishment acknowledgement

This branch builds against an SDK revision rather than a release, because the rest of this section
depends on an API that no published version has: `Cargo.toml` pins the commit, and `main` keeps
`=1.21.5-rc.3`.

```
docker compose run --rm --entrypoint repro repro \
  --trials 500 --budgets default,10 --wait-established --dead-peer-trials 10 --timeout-secs 40
```

`--wait-established` waits for the peer's acknowledgement before writing the payload, and each trial
is then scored twice: whether the peer acknowledged and whether the echo came back. Waiting before
writing is the point. The acknowledgement is never retransmitted and inbound data also resolves the
wait, so a rig that wrote first would be measuring the echo and calling it the acknowledgement.

`--dead-peer-trials` dials an address whose client registered with a live gateway and then
disconnected. The gateway is routable, so nothing below the stream layer objects, and there is
nobody there. It is the case a dialler had no way to detect before the acknowledgement existed.

`default` is accepted in `--budgets` alongside numbers, and leaves the count to the SDK. Since the
counts now differ between the `Open` and the `Data` frames, that is the only way to exercise the
values the SDK actually ships.

Give `--timeout-secs` room above `--establish-timeout-secs`, which defaults to 15. A trial that
waits out the acknowledgement still has to run the echo afterwards, and a deadline that leaves no
time for it turns every unacknowledged stream into a failed one by construction.

## Notes

- `sp` keeps its identity in a Docker volume. That directory holds private keys: the Nym address is
  derived from them, so losing it changes identity and copying it allows impersonation.
- `dial` is deliberately ephemeral. A stable client identity is exactly what would let a server
  correlate a client's requests, and the transport never hands the listener a client address anyway:
  inbound opens without reply blocks are dropped by the SDK.
- Each connected client generates continuous cover traffic, so running both halves costs roughly
  2 Mbps sustained for as long as the rig is up.
