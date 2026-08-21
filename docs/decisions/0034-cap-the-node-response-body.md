# 0034. Cap how much of a node response body is held in memory

## Context

Every call to the backend node goes through `raw_request` or `batch_request` in `src/node/mod.rs`,
and both read the reply with `reqwest::Response::json()`, which buffers the whole body and then
parses it. Nothing bounded that buffer. The client's two controls, a 30 s request timeout and a 5 s
connect timeout, bound seconds rather than bytes. Over the loopback link this hop is usually
deployed on that is no bound at all: tens of gigabytes arrive well inside 30 s.

`Cargo.toml` enables `reqwest`'s `gzip` and `zstd` features, which turns auto-decompression on for
every client in the process. So what gets buffered is the decompressed body: a few kilobytes of
`Content-Encoding: zstd` expand into gigabytes. On top of that, `RpcResponse.result` is a
`serde_json::Value`, so the raw buffer and the parsed tree are alive at the same time and the peak
cost is a multiple of the body.

The node link is plain HTTP by design ([0001](0001-backend-zebrad-over-zcashd.md)), and running
against a node the operator does not control is a supported setup. A node that answers with a body
larger than memory kills the process, and with it every wallet on the instance, instead of failing
the one call that asked. That is a worse failure than the one the same peer can already cause by not
answering at all.

The crate already solves this on the other HTTP path: the snapshot importer's `get_capped`
(`src/snapshot/import.rs`) reads through `bytes_stream()` with a running counter, capping the
decompressed stream for exactly this reason.

## Decision

Read node response bodies through `read_capped`, the same shape as the importer's: consume
`bytes_stream()`, count as it goes, and return `NodeError::ResponseTooLarge` the moment the total
would pass `MAX_RESPONSE_BYTES`. Then hand the buffer to `serde_json::from_slice`. Both call sites
use it, so every typed wrapper on `NodeRpc` inherits the bound.

`MAX_RESPONSE_BYTES` is 64 MiB. zebrad caps its own JSON-RPC responses at 50 MiB
(`max_response_body_size`), so 64 MiB refuses nothing an honest node can produce, with headroom at
the edge. What this client actually asks for sits orders of magnitude below that: a Zcash block tops
out at 2 MB, `getaddresstxids` at 10,000 txids, a `getblockhash` batch at 250 hashes. A tighter cap
would save little memory and could reject a legitimate answer from a node whose own limit was
raised. A much looser one would be decorative, since a 64 MiB spike is already the largest this
server has any reason to hold.

The cap is on the decompressed stream, which is what `bytes_stream()` yields. Counting wire bytes
instead would admit a few kilobytes and then hold the gigabytes they expand into.

The number is a constant rather than a flag. Nothing about a deployment makes a different value
right, and a knob here is one more thing to set wrong.

## Consequences

A node that answers with more than 64 MiB fails that one call with `Unavailable` and the process
keeps serving. The body is dropped as it arrives, so the peak held is the cap, not the response.

Malformed response bodies now surface as `NodeError::Decode` rather than `NodeError::Http`, because
the JSON parse moved from `reqwest` to us. Both map to `Unavailable`, so the code a wallet sees is
unchanged; only the server-side message differs.

The cap is the only bound this adds. Auto-decompression stays on process-wide and the client still
follows redirects. Both are their own change.
