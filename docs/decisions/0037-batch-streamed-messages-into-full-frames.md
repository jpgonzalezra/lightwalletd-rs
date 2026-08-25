# 0037. Batch small streamed messages into full DATA frames

## Context

Tonic's encoder packs consecutive messages of a server-streaming response into one body chunk and
cuts the chunk when the source stream returns `Poll::Pending`, or earlier if it reaches its 32 KiB
yield threshold. So the wire shape of a stream is set by whether its handler awaits between
messages, not by anything the handler says.

`block_range_stream` reads the cache in chunks under one transaction ([0028](0028-mvcc-chunked-cache-reads.md))
and awaits nothing along that path, so a cached range leaves as full-sized frames. Its miss branch
calls `fetch::compact_block`, which makes two node calls and then parses on the blocking pool, so it
pends on every block and each block leaves as its own frame. A compact block with no shielded output
encodes to under 100 bytes.

`h2` 0.4.16 started charging a connection budget for DATA frames below 256 bytes: a frame costs
`256 - len`, a larger frame refunds `len - 256`, and a connection whose budget runs out is closed
with `GOAWAY ENHANCE_YOUR_CALM` and `too_many_data_frames`. The budget is 25,600, so roughly 150
undersized frames sitting unread in a client's buffer end the connection. Every tonic wallet
inherits the guard on its next lockfile refresh, and
[zcash/lightwalletd#593](https://github.com/zcash/lightwalletd/issues/593) reports it breaking sync
against the Go implementation, whose ranges have the same one-block-per-frame shape.

Measured against this server before this decision, over a proxy that reads the frame headers:

| Range | DATA frames | Median frame | Outcome |
| --- | --- | --- | --- |
| 10,000 blocks from the cache | 74 | 16,384 B | completed |
| 1,000 blocks from the node | 1,000 | 80 B | completed against a client that keeps up |
| 1,000 blocks from the node, client stalls 30 s | 146 | 80 B | `GOAWAY too_many_data_frames` |

`h2` 0.4.19 derives the budget from the configured connection window, which takes the break away at
default settings. That fixes clients as they upgrade. It does not fix the wire shape, and it does
not reach a client pinned between 0.4.16 and 0.4.18.

Two other methods have the same shape. `GetSubtreeRoots` fetches each root's completing block
whenever it falls outside the cached range, and a `SubtreeRoot` is under 100 bytes. Every root of a
pool is addressable in one request ([0030](0030-subtree-index-range.md)), and those blocks are
spread across the whole chain. `GetTaddressTransactions` makes one node call per txid and takes up
to 10,000 txids ([0025](0025-taddress-range-bounds.md)).

Prefetching the misses would also remove the pend, and would serve a cold range faster. It spends
the wrong resource. The wallet-facing node pools are 8 and 2 permits ([0036](0036-bulkhead-the-wallet-facing-node-calls.md)),
so one client reading ahead takes the pool from every other client and from nothing else.

## Decision

Wrap the stream a handler returns in `service::framing::coalesce`. It holds messages until they add
up to 4 KiB, then hands the batch over with nothing awaited in between, which is what lands it in
one chunk. Whatever is still held goes out when the source ends and ahead of an error, so delivery
is unchanged and only the grouping moves. Applied to both block-range methods, `GetSubtreeRoots`,
and `GetTaddressTransactions`.

An adapter rather than batching inside each handler: the handlers keep reading one message at a
time, which is what their reorg and deadline logic is written against, and the batching is written
and tested once.

4 KiB is sixteen times the threshold `h2` charges against, so a batched frame refunds budget rather
than spending it and the connection sits at its full budget however long the range runs. It is also
small next to what the messages cost to produce: at 100 bytes a block the batch holds around 40
blocks, which is 40 node round trips the client was going to wait through in any case. Nothing about
a deployment makes a different value right, so it is a constant rather than a flag.

Batching costs no node concurrency. It changes when a message reaches the encoder, not when it is
fetched, so it composes with the bulkhead instead of competing with it.

The mempool streams keep yielding one message at a time. `GetMempoolStream` stays open and exists to
hand a wallet a transaction the moment it appears, so holding one back until 4 KiB accumulated would
be wrong, and mempool volume never approaches the budget anyway.

## Consequences

A range now leaves as full frames whichever side its blocks came from: 300 node-served blocks that
used to be 300 frames of 79 bytes are 6 frames of about 4.4 KB. Cached ranges are unchanged:
nothing on that path pends, the encoder was already filling 32 KiB chunks, and the batch hands it
the same messages in groups.

A client sees the first message of a node-served stream once the batch fills rather than once the
first fetch returns. A stream that ends before the batch fills delivers everything at the end. What
gets delivered does not change: the same messages arrive in the same order, and a range that aborts
on a discontinuity ([0027](0027-block-range-continuity.md)) still hands over every block below the
seam before the `Aborted`.

What a stalled client makes the server do is untouched. The server keeps fetching into the stream's
flow-control window whether or not anyone is reading, and bounding that is a separate question from
the shape of the frames.
