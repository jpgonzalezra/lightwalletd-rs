# 0033. Take the default ingest floor from the network parameters, not the node

## Context

With no `--start-height`, the server ingests from Sapling activation. That height used to come from
the `upgrades` table of the one `getblockchaininfo` issued at startup: the entry named `Sapling`,
and `0` if there was none.

The number is not advisory. It is where ingestion starts whenever the cache is empty, and it is the
floor a rollback may not cross: a reorg that reaches it empties the whole cache
(`ingestor::reorg_to_floor`, [0014](0014-cache-ingestor-resilience.md),
[0020](0020-windowed-ingest-batched-commits.md)). So a node that names a height above the chain
tip stalls ingestion for the life of the process, silently, because every poll then finds nothing
to do and looks exactly like being caught up. A height above the cached range instead leaves
ingestion working and turns the next ordinary one-block reorg into a full wipe, which also drops
the epoch digests and the snapshot base (`Cache::truncate_from(0)`).

The node link is plain HTTP by design ([0001](0001-zebrad-json-rpc-http.md)), and a third-party node
is a shape this server supports, so "the node said so" is not a reason to believe a number that
governs the cache.

Sapling activation is a consensus constant, and the crate already depends on `zcash_protocol`, which
carries the table. The readstate backend ([0023](0023-zebra-readstate-backend.md)) builds its own
upgrade list from network parameters, but too late to matter here: `chain_info` is read before the
backend is chosen.

## Decision

Resolve the default floor from the compiled-in parameters for the networks where the height is
fixed: `MAIN_NETWORK` and `TEST_NETWORK` via `zcash_protocol::consensus::Parameters`, keyed by the
chain name the node reports. A node that reports a different height gets a `warn` and is overruled.

Regtest sets its own activation heights, so there is no constant to prefer, and the chain name comes
from the same response: honouring the reported height for a name we do not recognize would hand the
floor straight back to whoever wrote the response. Regtest and unrecognized names ingest from
genesis instead. The resolved default is therefore always one of three heights this process picks:
419,200, 280,000, or 0.

Startup also warns when the resolved floor sits above the node's tip, which is the case
`--start-height` can still produce.

## Consequences

- The default floor no longer depends on anything the node says, on either backend. `--start-height`
  still overrides it, and an imported snapshot still raises it (`effective_start_height`).
- A disagreement gets logged, not obeyed, and it is worth reading: against an honest node the two
  numbers always match, so a mismatch means the node is not on the chain it claims, or the link is
  not carrying what the node sent.
- The proxy keeps serving through a disagreement instead of refusing to start. Failing closed here
  would hand anyone who can rewrite one field of one response a way to keep the server down.
- Activation heights are now pinned by the `zcash_protocol` version in `Cargo.toml`. They are
  consensus constants for the networks we read them for, so a bump cannot move them. A new network
  would need a line here.
- Genesis is a safe floor, which is why it is the fallback. `reorg_to_floor`'s crossing test cannot
  fire against 0, so on the networks whose activation height we cannot name, no rollback can reach
  the floor and empty the cache. The cost is ingesting the pre-Sapling range, which on a regtest
  chain is the genesis block.
- The floor-crossing wipe itself is untouched: it is still `--start-height` that a rollback may not
  cross, not the cache's own base height.
