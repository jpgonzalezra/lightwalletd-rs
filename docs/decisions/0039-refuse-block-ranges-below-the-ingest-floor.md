# 0039. Refuse block ranges below the ingest floor

## Context

`GetBlockRange` and `GetBlockRangeNullifiers` resolve a span of up to `MAX_BLOCK_RANGE` (10,000)
heights, block by block: from the cache when it holds one, and from the node when it does not
(`block_range_stream`, `src/service/blocks.rs`). `validate_block_range` checks that both bounds are
non-zero and that the span fits the cap. It never asks whether the heights are ones this instance
holds.

The two sources do not cost the same. A cached block is a `redb` read inside a transaction the chunk
already opened. A missing one is `fetch::compact_block`: `getblock <height> 1` for the hash and the
tree sizes, `getblock <hash> 0` for the bytes, then a full parse and a txid recomputation on the
blocking pool. Two node RPCs against zero, and which one a height costs is decided by the request.

Where the misses are is not arbitrary. `Cache::append` requires each batch to extend the tip by
exactly one, so the cache is contiguous by construction and a miss is only ever above the cached tip
or below the cache's base ([0027](0027-block-range-continuity.md) states the same thing from the
serving side). Those two are not alike:

- **Above the tip** the gap is the instance's own lag. The ingestor closes it at 37-498 blocks/s
  ([0024](0024-snapshot-bootstrap.md)) and, once caught up, keeps it to a block or two: `IDLE_POLL`
  is 2 s against a block interval of about 75 s. Nothing a client sends changes its size.
- **Below the base** every height is a permanent miss. The ingestor only extends its own tip, so it
  never backfills, and the node can still serve all of it. On a default mainnet instance that window
  is 1 to 419,199 (the floor is Sapling activation, [0033](0033-ingest-floor-from-network-parameters.md));
  on one bootstrapped from a snapshot it runs up to the snapshot's base height
  ([0024](0024-snapshot-bootstrap.md)).

So a client that picks a 10,000-height span below the base gets a request that is in spec, in budget,
and worth 20,000 node RPCs. The blocks are not written back on the way out, so the same request costs
the same again. [0036](0036-bulkhead-the-wallet-facing-node-calls.md) bounds how many of those calls
run at once and keeps the ingestor's share out of reach, which is what stops the cache from falling
behind. It does not stop one client from holding the wallet-facing pool indefinitely with work for
blocks this server does not have.

## Decision

Refuse a range whose lower bound is below the ingest floor, with `OutOfRange` naming that floor, and
make the check before the stream opens so a refused request costs the node nothing.

The floor is the cache's own base once it holds anything, and the configured ingest floor while it is
empty. Taking the base rather than the configured value matters when an operator restarts with a
`--start-height` below what the cache already covers: the ingestor will not backfill those heights, so
the base is the honest answer to what this instance serves.

Refuse the whole range rather than serve the part above the floor. The response carries no marker for
a short answer, so a truncated range reads as a complete one, and a wallet would take a gap in its
own scan for chain that holds nothing of interest.

`OutOfRange` over `NotFound`: the request named a span this server does not carry, and the client can
act on it by asking for one that starts at the floor. A single `GetBlock` below the floor is still
served from the node. It is two RPCs for one block, which is the bounded case this decision is not
about.

Darkside and `--nocache` get no floor and keep the old behavior. Both keep an empty cache on purpose
and run no ingestor, so the node is their source rather than their fallback, and a floor would refuse
everything. This follows what [0036](0036-bulkhead-the-wallet-facing-node-calls.md) already does for
darkside.

## Consequences

The node fan-out of one in-budget request drops from 20,000 RPCs to about 4 on a healthy instance:
everything from the floor to the cached tip is a hit, and what is left is the block or two the
ingestor has not reached. The permanent, client-selected miss window is gone, and with it the reason
one client could occupy the wallet-facing pool with work for blocks this server does not hold.

A wallet whose birthday is below the floor can no longer sync against that instance. On a default
deployment the refused window is pre-Sapling, where there is nothing for a shielded wallet to find.
On a snapshot-bootstrapped one it is real history, and the operator chose that when they picked a
snapshot base: [0024](0024-snapshot-bootstrap.md) already says a server can only serve the range it
holds. Serving it anyway, two node round trips per block, was not a service that instance could
provide.

The window above the cached tip is unchanged. An instance still catching up keeps answering from the
node, which is what lets it serve at all before the cache is filled. That window closes on its own,
and its size is an operational property rather than something a client picks. Bounding it too would
refuse the ranges a wallet legitimately asks for while the cache fills, since `GetLatestBlock`
reports the node's tip rather than the cache's.

Refusals show up per method as `OUT_OF_RANGE` in the existing metrics
([0035](0035-bounded-metric-labels.md)).
