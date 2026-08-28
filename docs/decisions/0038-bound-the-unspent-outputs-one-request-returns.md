# 0038. Bound the unspent outputs one address request returns

## Context

`GetAddressUtxos` and `GetAddressUtxosStream` share `collect_utxos` (`src/service/address.rs`). It
caps the address list at `MAX_STREAMED_ADDRESSES` and nothing else. The reply count has no
server-side bound. `getaddressutxos` takes no height range, so the backend always returns every
unspent output the named addresses hold, and the only thing that can stop the conversion loop is the
client's own `maxEntries`. The protocol reads a zero `maxEntries` as unlimited
(`proto/service.proto:208`), and that is what light-client SDKs send on every sync pass, so the
unbounded case is the ordinary one rather than an edge.

How large the result gets is set by how much the named addresses hold, which is public and free to
look up, and up to 10,000 addresses can be named in one request. How long it is held is set by the
client. A peer that stops sending `WINDOW_UPDATE` stops `hyper` polling the response body, so the
replies that have not been written stay resident: for the streaming method inside the `Vec` the
stream still owns, for the unary one inside the encoded buffer. Nothing releases either. The only
deadline in this module, `TADDRESS_SCAN_DEADLINE`, is applied in `taddress_transactions`, and
`server_builder` adds no timeout layer. HTTP/2 keepalive drops peers that stop answering, not peers
that answer and never read.

[0013](0013-resource-limits.md) lists `MAX_STREAMED_ADDRESSES` as one of the caps that bound
accumulation for this request. It bounds the input list. The output is what gets held.

[0036](0036-bulkhead-the-wallet-facing-node-calls.md) does not reach this either. Its permit is
released when the node answers, which is where the retention starts.

## Decision

Cap the replies one request may produce at `MAX_ADDRESS_UTXOS` (100,000), checked as the replies are
built, and refuse a larger result with `ResourceExhausted`. The message names `startHeight` and
`maxEntries` as the way to read the result in pages. `collect_utxos` is the one place both methods
go through, and it sits above the backend choice, so one check covers all four combinations.

The value is not free to pick. The only cursor the protocol offers is `startHeight`, which selects a
block rather than an output. A caller can page past a height once it has read that height in full,
and no further. So the cap has to clear the largest group of unspent outputs that can share one
height, or that height is unreadable and paging stalls on it forever.

Consensus bounds that group. A block is at most 2,000,000 bytes, and the smallest output the address
index can hold is 32 bytes: 8 for the value, 1 for the script length, 23 for a P2SH script. Under
62,500 outputs, once the header, the transaction overhead and the input every transaction needs
come out of the same budget. `MAX_INDEXED_OUTPUTS_PER_BLOCK` states that bound next to the cap, and
a `const` assertion holds the cap above it. Lowering one without the other fails the build rather
than reopening the stall quietly.

100,000 is the round number above that bound. That works out to roughly 25 MB per held response,
against a result that used to be as large as the named addresses happened to make it: 55 MB on the
`rpc` backend, where zebrad's own response cap is what stopped it, and unbounded on `readstate`. The
size is now a constant this server picks rather than a quantity the caller chooses.

Refuse rather than truncate. The reply carries no "there is more" field, so a truncated answer reads
as the complete set of unspent outputs. A wallet that believes it computes the wrong balance and may
fail to spend what it holds. Silently wrong is worse than an error the caller can act on, and a
caller that wants a partial answer already has `maxEntries`.

The check runs after the `maxEntries` break, so a request that states its own bound within the cap is
served whatever the addresses hold. The refusal is reachable only by asking for more than the cap.

The cap is a module-local constant, like the others in [0013](0013-resource-limits.md).

## Consequences

A request whose result would exceed 100,000 unspent outputs is now refused. The caller pages
instead: results come back ordered by height, which is the protocol's own stated reason for pairing
`startHeight` with `maxEntries` (`proto/service.proto:203-204`), and both backends produce that
order. A light wallet never reaches the cap. An account large enough to reach it, an exchange or a
payout account, has to page, and an SDK sending `maxEntries: 0` will see `ResourceExhausted` until
it does.

Paging costs the caller one overlapping height per page. A page of `maxEntries` replies can end
partway through a height, so the next request sets `startHeight` to the last height it saw rather
than one past it, and drops the outputs it already holds by transaction id and output index. A
height cannot fill a page, so that next page carries the whole of it and ends strictly further along
than the one before.

What one held response can pin is now bounded, for both methods and both backends. What is not
bounded here is how many responses can be held at once: that follows from the number of streams per
connection and the number of connections, and it is unchanged by this decision.

Refusals are visible per method as `RESOURCE_EXHAUSTED` in the existing metrics
([0035](0035-bounded-metric-labels.md)), so an operator can see a client hitting the cap without a
new series.
