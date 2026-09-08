# 0042. Resolve a subtree's completing block by hash lookup, under a deadline

## Context

`GetSubtreeRoots` answers with one root per note-commitment subtree, each tagged with the block that
completed it. The request is about twenty bytes and the reply is under a hundred bytes per subtree.

The node's answer already carries the completing height. Getting the hash for it read the whole
block: the handler called `block_at` once per subtree, which reads the compact block from the cache
and decodes it, or on a miss fetches it from the node with two round trips, parses it and recomputes
every txid. All of that to keep 32 bytes and throw the rest away.

Nothing bounded that in either direction. `MAX_SUBTREE_INDEX` ([0030](0030-subtree-index-range.md))
constrains the width of the index field, not the work, and there was no deadline anywhere in the
handler. Mainnet holds on the order of a thousand Sapling subtrees today and the count only grows,
so one small request read and decoded a thousand historical blocks and returned about a hundred
kilobytes. A good share of those blocks sit in the era where blocks are largest, since that is where
note commitments were densest and so where subtree boundaries fall closest together. On an instance
whose cache does not reach that far down (a cold sync, or one bootstrapped from a snapshot above
those heights) every one of them missed and went to the node instead, on the connection the ingestor
shares.

The absolute cost is not what stands out: `GetBlockRange` is allowed to decode ten times as many
blocks. What stands out is that the client pays for none of it. A range costs the client every block
it asks for. A subtree root costs it a hundred bytes. That ratio is what ADR
[0013](0013-resource-limits.md)'s invariant is about, and this handler was the one still outside it.

A count cap is not an answer here. Zero means unlimited in `maxEntries`, that is what the
light-client sync sends, and it takes the whole list at once rather than paging through it. A cap
below the real subtree count would hand a wallet a tree missing its newest shards, silently.

## Decision

Resolve the completing block by looking up its hash, and never read the block.

`Cache::hashes_at` reads the heights under one transaction and decodes each stored block down to its
`hash` field only. Prost skips fields a message does not declare, so it steps over each
transaction's length prefix instead of building the transaction. The stored bytes are still read and
the walk is
one step per transaction, but what a block costs stops being the sum of its parts: no `CompactTx`,
`CompactSaplingSpend` or `CompactOrchardAction` is allocated to reach 32 bytes of hash. One
transaction for the whole set also keeps the answer coherent if the ingestor truncates a reorg
mid-read, the argument of [0028](0028-mvcc-chunked-cache-reads.md).

`Cache::latest_hash` reads the same 32 bytes through a full decode and stays that way. It runs once
per ingest step, so the saving would be nothing, and the decode doubles as the check that catches a
corrupt cached tip within a tick rather than at the next startup.

Heights the cache does not hold go to `getblockhash`, batched. One request covers the whole subtree
set of any chain today, against the two-round-trips-and-a-parse per subtree it replaces.

A 30 s deadline covers the node work of the request, both the subtree query and the hash lookups, in
the shape [0025](0025-taddress-range-bounds.md) already uses for the transparent-address scan. It
does not cover the stream: a client reading slowly paces itself and holds nothing the server has to
keep working on.

What the node returns is truncated to the subtrees it can address from the requested start index,
`MAX_SUBTREE_INDEX - start_index + 1`. Indexes are `u16`, so entries past that are about subtrees
nobody could ask for again. Dropping them takes the size of the work out of the node's hands, and
never shortens a legitimate answer.

## Consequences

The reply is unchanged. No request returns fewer roots than before, which is what rules a count cap
out, and the light-client sync keeps getting the whole list from `maxEntries = 0`.

A cache-hit lookup no longer decodes the transactions of a historical block, and a miss costs a
batched height lookup rather than a block fetch and a parse. The bulkhead
([0036](0036-bulkhead-the-wallet-facing-node-calls.md)) sees one permit per batch instead of two per
subtree, so this RPC stops being a way to take the node capacity the ingestor needs.

A completing block the node cannot resolve is now reported as `Unavailable` rather than
`OutOfRange`.
The only way to reach it is a reorg between the two calls, and a retry is what fixes that.

The hash lookups are batched at 1,000 heights, larger than the 250 the snapshot check uses. That one
spreads its batches across a worker pool, where too few chunks starve the workers. Here the calls
are sequential and round trips are the whole cost.

`block_at` stays for `GetBlock` and `GetBlockNullifiers`, which need the block itself.
