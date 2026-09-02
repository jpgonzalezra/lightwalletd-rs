# 0041. Cap the pool-type filter and resolve it once per request

## Context

`BlockRange.poolTypes` names the value pools a wallet wants back from `GetBlockRange` and
`GetBlockRangeNullifiers`. A wallet sends between zero and four entries. Nothing bounded how many it
could send: `validate_pool_types` (`src/filter.rs`) rejected the `POOL_TYPE_INVALID` sentinel and
nothing else. That made it the one repeated field in the wallet-facing protocol without a cap of its
own. `AddressList.addresses`, `GetAddressUtxosArg.addresses` and `Exclude.excludeTxidSuffixes` all
have one. The effective ceiling was the 4 MiB gRPC decode limit, which this project does not lower:
about 4.19 million single-byte entries.

The list was re-read per block rather than per request. `filter_block_to_pools` derived `Pools` from
the raw slice on every call (four `slice::contains` scans), and `block_range_stream` calls the
transform once per yielded height, up to `MAX_BLOCK_RANGE` (10,000) of them. The nullifiers path
allocated a fresh `Vec<i32>` per block on top of that, to strip transparent from the filter. So
`MAX_BLOCK_RANGE` did not bound what the filter cost. It multiplied it.

That composition handed the client a lever. Values the enum does not define are not the invalid
sentinel, so they passed validation, and they matched none of the four `contains` calls, so each one
walked the whole list. They also resolved to no pool, which emptied every transaction, so each block
left as about a hundred bytes. The client no longer had to read anything, and HTTP/2 flow control,
the only thing pacing a client-driven stream, stopped pacing. One uploaded byte bought 40,000 `i32`
comparisons, and a 4 MiB request bought around 1.7 x 10^11 of them.

The list itself also outlived the handler. It moved into the stream's closure and stayed there as
long as the stream lived, which the client also chooses: a server-streaming call has no deadline of
its own.

`src/service/mempool.rs` already resolved `Pools` once outside its loop, for the structurally
identical `GetMempoolTxRequest.poolTypes`, so the two paths disagreed about the same field.

## Decision

Two changes, one per axis.

Cap the list at `MAX_POOL_TYPES` (16) in `validate_pool_types`. A longer one is refused with
`ResourceExhausted`, the code the other repeated fields already use when they overflow. Four pools
exist, so anything past four is redundant, but the cap is not four: `PoolType` is an open proto3
enum, a client built against a newer protocol may name a pool this build has never heard of, and
nothing forbids duplicates. Four times the current set covers both without anyone revisiting the
number when a pool is added, and against the amplification any small constant works the same. The
validator is shared, so `GetMempoolTx` picks up the cap too.

Resolve `Pools` once per request, and make that structural rather than conventional:
`filter_block_to_pools` and `filter_block_to_pools_nullifiers_only` take `Pools`, not `&[i32]`.
`Pools` is four bools and `Copy`, so the stream closure carries those and the client's list is
dropped when the handler returns. Stripping transparent for the nullifiers path becomes a second
constructor, `Pools::from_pool_types_dropping_transparent`, which reaches the same result the
per-block filtering did without allocating. That includes the fallback where a request naming
transparent alone leaves an empty filter, and therefore the legacy shielded default.

Either change on its own leaves something open. The cap alone keeps the per-block re-resolution, so
widening the cap later would reopen the cost. Resolving once alone keeps the decoded list resident
for the life of the stream.

## Consequences

A request carrying more than 16 pool types is refused before the stream opens. No wallet sends more
than four, so this is a new answer to a request nobody makes.

What the filter costs no longer scales with anything the client picks. `Pools` is resolved once from
a list of at most 16, and the per-block work is four bool tests. The nullifiers path no longer
allocates per block, and a stream holds four bools rather than whatever the client uploaded.

Refusals appear as `RESOURCE_EXHAUSTED` per method in the existing metrics
([0035](0035-bounded-metric-labels.md)), so an operator sees a client hitting the cap without a new
series.

`MAX_POOL_TYPES` is a module-local constant, like the other per-request caps in
[0013](0013-resource-limits.md).
