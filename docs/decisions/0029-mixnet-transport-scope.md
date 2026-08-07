# 0029. Keep a mixnet transport out of the crate, behind a sidecar

## Context

A light wallet reveals more to a lightwalletd operator than the protocol suggests. Every request
carries an IP address and a timestamp, and some carry their own content: `SendTransaction` links a
transaction to a network identity, and the transparent-address family hands over the client's
addresses directly. The leak is roughly inverse to the bandwidth. Bulk block download is the
heaviest call and the least revealing, because the client fetches everything and trial-decrypts
locally; the cheapest calls are the ones worth protecting.

That asymmetry makes a mixnet interesting. A mixnet routes each packet separately through several
relays that delay and reorder traffic, and adds cover traffic, so it resists the timing correlation
a low-latency overlay does not. We evaluated carrying `CompactTxStreamer` over
[Nym](https://nym.com), the deployed general-purpose mixnet with a Rust SDK.

**Carrying the service is nearly free in code, and that has to be established first, because it is
not why this ADR ends where it does.** `nym_sdk`'s `MixnetStream` implements
`AsyncRead + AsyncWrite`, and `tonic`'s `serve_with_incoming_shutdown` accepts an arbitrary stream of
connections, so the existing service layer is reused without modification. No wire protocol has to be
designed and no handler duplicated. The evaluation rig, two binaries piping bytes in each direction,
is about a hundred lines and compiled on the first attempt (`contrib/nym/`). The property that
motivates the exercise also comes for free: the stream protocol never conveys a client address to the
listener, and the SDK drops inbound opens carrying no reply blocks, so the server cannot learn a
stable client identifier even if its operator wanted to.

The costs sit elsewhere. Full method and numbers in
[`contrib/bench/results/mixnet-transport-2026-08.md`](../../contrib/bench/results/mixnet-transport-2026-08.md);
the four that decide this ADR:

1. **Streams fail silently, at a rate between 2% and 51% that is not stationary.** A stream opens,
   the far side accepts it, the sender's `write_all` and `flush` both return `Ok`, and the payload
   never arrives. Neither end receives an error and neither times out: both hang indefinitely. A
   minimal reproduction using nothing but the SDK (`contrib/nym/src/bin/repro.rs`, two clients in one
   process, no gRPC and no proxies) measured 2% over 200 trials one afternoon and 36.5% over 400
   trials the next day, with failures nearly tripling between the first and second halves of that
   same run. Raising the reply-block budget mitigates it only partially, from 51% at one block to 26%
   at four hundred, and costs 5.3x the latency to do so. No setting observed makes it reliable.
2. **Latency is seconds, not milliseconds.** `GetBlock` served from cache measured p50 1.39 s and p99
   3.42 s in one session, p50 2.4 s and p99 7.1 s in another, against p50 0.29 ms and p99 1.19 ms on
   the ordinary listener. Published per-hop figures suggest tens of milliseconds in total; the gap is
   roughly twentyfold.
3. **The dependency is larger than this project and cannot be trimmed.** `nym-sdk` resolves to 756
   dependencies against 580 for this crate in full, with one optional feature that is already off.
   SOCKS5, the VPN-mode IP router, a chain client and the credential machinery are all mandatory, and
   a Cargo feature gated off keeps none of them out of the lockfile or out of `--all-features`.
4. **Bulk sync does not close arithmetically.** At the measured throughput a wallet whose birthday
   predates the 2022 spam era would move roughly 43 GB, on the order of eleven days of continuous
   transfer, and no compression scheme changes that (see
   [`compact-block-wire-size-2026-08.md`](../../contrib/bench/results/compact-block-wire-size-2026-08.md)).
   Recent birthdays are fine: a day of catch-up is about thirty seconds.

The contrast with [0026](0026-grpc-web-support.md) is the point, not an inconsistency. That decision
added a transport to this crate behind a flag because the translation is a `tower` layer around the
router already present, so the cost is a few lines of wiring and the operator's choice of a CORS
policy. A mixnet transport is not a layer around the existing server: it is a second, larger network
stack running beside it, with its own gateway registration, credential storage, cover traffic and
identity on disk. Same category of feature, different order of cost, and the decisions differ for
that reason alone.

## Decision

**Do not add a mixnet listener to this crate, and do not add a Cargo feature for one.** If the
transport is pursued, it is a separate process accepting mixnet streams and piping each to the
ordinary gRPC listener over loopback, exactly as the evaluation rig does.

**The server imposes no transport-based restriction.** It serves the full `CompactTxStreamer` over
whatever connection reaches it. Which calls travel over a mixnet is a client routing decision, and
narrowing the server would be extra work rather than saved work.

For integrators, the evaluation supports this guidance, recorded here because it is the substance of
the decision rather than an aside:

- **Worth routing over a mixnet:** `SendTransaction`, `GetTaddressTransactions`, `GetAddressUtxos`,
  `GetTaddressBalance`, `GetTransaction`. High leak, few bytes, latency-insensitive. A shielded
  transaction fits in one or two packets and stays inside the default reply-block budget, so it never
  reaches the replenishment path at all, and seconds of latency are irrelevant against 75-second
  blocks.
- **Not worth routing over a mixnet:** bulk block download, unless the wallet's birthday is recent.
- **Retry is mandatory, not optional, and cannot be tuned away.** Given (1), a client must impose its
  own deadline on a stream that opens and never answers, then retry on a fresh one. Under degraded
  conditions loss was observed in both directions, so the deadline has to cover the whole exchange
  rather than just the connect. Raising the reply-block budget lowers the rate but never to zero, and
  buys that at a large latency cost, so it is a tuning knob and not a fix.
- **Do not sync and submit through the same server.** An operator that sees an IP synchronising and
  moments later receives an anonymous transaction can correlate the two by timing, and with few
  concurrent users the anonymity set is negligible. Submitting through a different instance than the
  one used for sync costs nothing and removes the correlation.

The per-connection limits of [0013](0013-resource-limits.md) carry over unchanged, since a mixnet
stream is a connection. Nothing here keys on a peer address, which is fortunate: over a mixnet there
is none, and rate limiting by IP is unavailable to a sidecar too.

## Consequences

- `Cargo.lock` is untouched and the 756-package tree stays out of every build, CI job and downstream
  consumer. Operators who want the transport run a second process; everyone else carries nothing.
- Nothing in this repository has to track a moving SDK. Over the evaluation window Nym deprecated its
  standalone TCP proxy, deprecated its own Zcash gRPC demo, and paused then resumed crate
  publication. A sidecar absorbs that churn; an in-tree feature would not.
- The cost is one loopback hop and a second process to supervise. Supervision is not a formality: the
  Nym client failed to register with a gateway within two minutes in 2 of 15 attempts, so a sidecar
  needs health checking and restart, not a bare restart policy.
- Documentation must not present a mixnet as a drop-in replacement for the ordinary listener. It is
  usable for the calls listed above and unusable for a full historical sync.
- The decision is cheap to reverse. If the establishment-failure rate and latency improve, adopting
  the transport in-tree means a newtype over `MixnetStream` implementing tonic's `Connected` with an
  empty `ConnectInfo` (nothing else can implement it: the trait is tonic's, `nym-sdk` does not depend
  on tonic, and orphan rules block both sides), then `serve_with_incoming_shutdown`. That is a small
  change, and this ADR records why it was not made yet rather than arguing it never should be.

Negative results are kept in the measurement report so they are not re-derived: mixnet epochs last
exactly one hour and produce no observable effect at the client, per-message compression of compact
blocks is worthless (1.06x) and stream-level compression modest (1.37x), and packet alignment is a
non-question because the transport exposes a byte stream rather than datagrams.
