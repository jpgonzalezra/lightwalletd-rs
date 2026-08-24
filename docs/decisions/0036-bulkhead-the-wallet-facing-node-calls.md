# 0036. Reserve node capacity for ingestion with a wallet-facing bulkhead

## Context

The process holds one backend node and hands the same `Arc<dyn NodeRpc>` to three consumers
(`src/lib.rs`): the ingestor, the mempool monitor, and the `Streamer`. The ingestor keeps the cache
at the chain tip, and the `Streamer` is every wallet-facing gRPC handler.

Two of them bound their own share. `ingestor::fetch_window` takes a permit from a semaphore sized by
`--ingest-concurrency` (default 8) before each block fetch. The mempool monitor is a single poller,
by [0005](0005-shared-mempool-monitor.md). The wallet-facing path bounded nothing: the only limits
in front of it, `concurrency_limit_per_connection` and `max_concurrent_streams` from
[0013](0013-resource-limits.md), are per-connection, and nothing caps connections.

That is backwards. The component the whole cache design depends on runs on 8 permits while an
anonymous client draws on the same finite node without limit, so the node saturates and the
ingestor is what yields.

The transparent-address RPCs make that cheap to do on purpose. `getaddressutxos` takes no height
range, so it is always a full walk of the named addresses' index entries, and `maxEntries` filters
in this process after the node has returned everything. `getaddresstxids` accepts any range inside
`MAX_TADDRESS_BLOCK_SPAN`, which was chosen never to bind on today's chain. Both accept up to
`MAX_STREAMED_ADDRESSES` (10,000) addresses per call. The cost is set by how much history the named
addresses have, which is public, free to look up, and nothing the caller pays for.

A deadline does not help. Zebra dispatches a read to its blocking pool when the request arrives
(`ReadStateService::call` ends in `spawn_blocking`), so a client resetting its stream, or a
proxy-side timeout firing, stops nothing at the node. The scan runs to completion either way. The
only lever that reserves node capacity is a cap on calls in flight.

The failure is quiet and it compounds. A starved ingestor logs at warn and sleeps 8 s, so nothing
alerts. Once the cache tip falls behind, `block_range_stream`'s miss branch fires for heights *above*
the cache tip as well, turning ordinary sync traffic into two node calls per block.

## Decision

Wrap the node handle the `Streamer` gets in `node::bulkhead::Bulkhead`, and only that one. The
ingestor and the mempool monitor keep the bare handle, which is what reserves their share.

The wrapper admits a call into one of two **disjoint** pools, chosen by whether the request bounds
its own cost at the node:

- **transparent-address** (`--client-scan-concurrency`, default 2): `get_address_balance`,
  `get_address_utxos`, `get_address_txids`.
- **node** (`--client-node-concurrency`, default 8): everything else, whose cost is bounded by the
  block, transaction, tree state, or subtree range the request names.

Disjoint rather than nested: one call takes one permit, so there is no acquisition order to reason
about and a full scan pool cannot delay a transaction lookup.

Two is small because a scan permit is not a unit of bounded work: one permit can carry 10,000
addresses of unbounded history. An honest scan is proportional to the requesting wallet's own
address history and costs milliseconds, so two permits still serve hundreds of them per second,
while the expensive case is attacker-chosen and gets serialized. Eight for the rest is not a claim
that those calls are cheap: a cache-miss block fetch pulls up to 2 MB, and a subtree request with no
limit is unbounded in entries. Eight concurrent bounded calls already carry far more wallet traffic
than an instance whose cache is healthy ever generates, and eight leaves the node room for the
ingestor's own eight.

A call that cannot be admitted within `PERMIT_WAIT` (250 ms) is refused with
`NodeError::Overloaded`, mapped to `RESOURCE_EXHAUSTED`. Refusing rather than queueing puts the
failure on the client that caused it instead of spreading latency over everyone, and
`RESOURCE_EXHAUSTED` says "come back" where `Unavailable` would tell a wallet the backend is down.
The wait exists so an ordinary burst rides out the queue: permits turn over in milliseconds when the
work is honest, and 250 ms is many turns. It is a constant, not a flag. It follows from how fast the
pools turn over, not from anything an operator knows about their deployment.

The permit is held by a task the caller cannot cancel. Dropping a client's future must not hand the
permit back while the node is still working, or the pool would bound admissions rather than node
work and a client could admit call after call by abandoning each one.

## Consequences

The ingestor's share of the node is reserved. Under a load that used to stall it, the ingestor keeps
its 8 permits and the cache keeps advancing, so the feedback loop through the cache-miss path does
not start.

Under that same load, wallet-facing calls that touch the node are shed. A transparent-address query
is refused once two are already running, and other node-backed calls once eight are. Reads served
from the cache, which is the bulk of sync traffic, are unaffected: they never enter a pool. Shedding
is visible per method in the existing metrics as `RESOURCE_EXHAUSTED` ([0035](0035-bounded-metric-labels.md)),
so no new series is needed to see it.

An operator who has sized their node for more than this can raise either flag. The defaults are
deliberately conservative: the cost of setting them too low is a refused request the client retries,
and the cost of setting them too high is the failure this decision exists to prevent.

Both backends are covered, since the wrapper sits above the backend choice. On `readstate` the
permit is held for as long as the in-process read takes, which is the honest accounting: that read
occupies the same machine as everything else.

Darkside mode is not wrapped. It runs no ingestor, serves staged fixtures, and its determinism is
the point.

The bulkhead bounds node calls, not the work around them. Block parsing after `get_block_raw`
returns, the response bytes held per call ([0034](0034-cap-the-node-response-body.md)), and the
number of connections that can be open at once are bounded elsewhere or not at all. None of them
changes here.
