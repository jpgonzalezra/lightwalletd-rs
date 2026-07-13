# 0005. Shared mempool monitor (live mode)

## Context

`GetMempoolTx` and `GetMempoolStream` both expose the node mempool. A naive implementation polls the
node once per request, so node load grows with the number of connected wallets.

## Decision

In live mode a single background task (`src/service/mempool_monitor.rs`) refreshes the mempool at most
once every ~2 s and fans a deduplicated, parsed-once snapshot out to all clients through a
`tokio::sync::watch`. `GetMempoolTx` borrows the current snapshot; `GetMempoolStream` subscribes to it.
A `watch` channel (a last-value snapshot) is used rather than a broadcast channel, since a late
subscriber wants the current mempool, not a replay of every past change.

## Consequences

- Node load is independent of the number of connected wallets; each mempool transaction is fetched and
  parsed once per refresh, and wallets see at most ~2 s of staleness while the node is healthy.
- The refresh tolerates partial node failures: a transaction that disappears between the
  `getrawmempool` listing and its `getrawtransaction` fetch is logged and skipped, and a failed
  listing retains the last good snapshot until the node recovers — but only up to a point: past a
  60 s cutoff the snapshot is no longer served as if current, and both RPCs return `Unavailable`
  instead. See [0021](0021-mempool-staleness-contract.md) for that bound and why "retains the last
  good snapshot" needed one.
- Darkside keeps the per-request path (`Streamer.mempool == None`), where a staged transaction must
  appear and drain synchronously.
