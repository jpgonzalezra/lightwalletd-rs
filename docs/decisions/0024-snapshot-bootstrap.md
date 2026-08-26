# 0024. Bootstrap the cache from a peer's snapshot, anchored to the operator's own node

## Context

A fresh instance ingests from Sapling activation to the tip before it can serve a wallet the range it
asks for. On mainnet that is roughly 3.0M blocks. The measured ingest rate varies by an order of
magnitude across eras (ADR 0017's harness, against a real mainnet zebrad): about 498 blocks/s in the
modern pre-spam range, but about 37 blocks/s through the sandblasting stretch, where blocks carry
thousands of shielded outputs each. A cold start is therefore hours long, and the server has nothing
to serve for all of it. The operator levers that exist (`--redownload`, `--sync-from-height`,
`--nocache`) all reload from the same node at the same speed.

An instance that already holds those blocks could hand them to a new one far faster than the new
one's node can re-derive them. The obstacle is trust, and it is specific: a `CompactBlock` drops the
data needed to recompute a block hash, so `hash` and `prevHash` are fields the publisher asserts
rather than values a consumer can derive. A snapshot that is merely self-consistent proves nothing.

## Decision

Publish the cache contents as a portable, epoch-chunked artifact over HTTP, and verify every
imported block against the importer's own node.

**Not a `redb` file copy**, for four independent reasons: the file carries a storage-format version
that a future `redb` refuses to migrate; it carries free-page overhead; a foreign file bypasses every
`Cache` invariant on the way in; and a live file cannot be copied consistently while the ingestor
writes.

**Epochs** are a fixed span of 10,000 heights aligned to multiples of that size, matching
`MAX_BLOCK_RANGE`, so any two servers cut at the same boundaries and a body is byte-identical
wherever it came from. Each epoch carries two digests, both taken over the uncompressed body:

- `content_digest`, SHA-256 of the body, which proves the transfer arrived intact and nothing more:
  the manifest that declared it came from the same server as the body.
- `anchor`, SHA-256 over the epoch's `(height, block hash)` pairs, which the importer recomputes
  from its own node. It is deliberately independent of the framing, so a later format version can
  change the encoding without invalidating published anchors.

**Four verification layers** run per epoch, in cost order, all unconditional: content digest, chain
linkage (within the epoch and across the join onto what the cache already holds), note-commitment
tree-size deltas against the outputs and actions each block carries, and the anchor. The anchor is
dense rather than tip-only because a tip-only check is vacuous: a publisher would pin the real tip
hash in the last block, choose that block's `prevHash` freely, and leave everything below it
unconstrained while still passing the first three layers.

**Imports go through the cache's ordinary append**, one transaction per epoch, so the existing
invariants apply unchanged and a rejected epoch writes nothing. Resumption then needs no state of its
own: the importer asks the cache how far it got. A successful import into an empty cache records the
snapshot's base height in the same transaction, and startup floors the ingestor at it, so a deep
reorg cannot empty the cache and silently re-sync from Sapling.

**Compression is transport, never format.** It is negotiated per request and invisible to the
digests. Covering compressed bytes would make a digest depend on the compressor's version and level,
so two servers holding identical blocks would publish different digests and the manifest would stop
being portable.

Serving is off by default and requires a restart. Consuming is opt-in through `--snapshot-url`, and a
failure there is never fatal: the server starts and ingests from its node as before.

### Measurements behind the design

Against a real mainnet zebrad, 2026-07-26 and 2026-07-27, in two environments: co-located on the
node's own host, which is the shape a deployment has, and over a high-latency link, which is a
pessimistic floor. Sizes and compression ratios do not depend on the link.

**Block-hash lookups.** The import's cost is dominated by the anchor check, one lookup per height.
Co-located, at the default concurrency of 8, batched lookups run at 20.7k to 29.6k heights/s
depending on batch size, putting a full 3.0M-height range under two minutes. Even the slowest
configuration measured there (one worker, 100 per batch, 3.6k heights/s) covers that range in about
14 minutes. Over the high-latency link the same code ran at 17.7 heights/s unbatched and 1,118
batched. Verifying a full range therefore costs a small fraction of what ingesting it does in either
environment, by a wide margin, so the anchor check stays unconditional and there is no option to
sample it.

The two environments disagree about tuning, which is worth recording because it is not obvious.
Over a high-latency link round trips dominate, so larger batches win and concurrency adds little.
Co-located the node's own work dominates instead, so concurrency wins and batches past a few hundred
start to *lose*: 10,000 heights split into ten 1,000-height chunks spread badly across eight workers.
`LOOKUP_BATCH` is set to 250 on the co-located numbers, since that is the environment that matters,
and it stays adequate on a slow link.

zebra accepts a batch only when its elements declare `"jsonrpc": "2.0"`, though it accepts
`"1.0"` for a single call.

**Epoch sizes.** An epoch from the 2016 range is 49 MB (4.9 KB/block). An epoch from early
sandblasting is 1.21 GB (121 KB/block). Extrapolated across the eras, a full mainnet snapshot is on
the order of 50 GB, roughly three quarters of it in the sandblasting range.

**Compression, by era.** The 2016 epoch compresses 1.38x / 1.88x / 2.59x at `zstd -1` / `-3` / `-9`.
The sandblasting epoch compresses 1.02x / 1.05x / 1.06x at the same levels: it is dominated by
cryptographic material, which is incompressible by construction. Level 3 is the default because on
the data that dominates the artifact no level helps at all, so the right choice is the cheapest one
that still helps the light eras. A full snapshot goes from about 50 GB to about 41 GB.

## Consequences

- A new instance reaches the tip in a fraction of the time, having verified every block against its
  own node, and then serves identically to one that ingested from scratch.
- **A publisher can still substitute what a block contains.** The layers bind counts and chain
  position, not the values themselves: a block with the right number of outputs in the right place,
  whose `cmu`/`cmx` are fabricated, passes every check. The same holds for everything else a
  `CompactBlock` carries and no layer reaches, including `epk`, the note ciphertexts and the txids.
  A wallet's exposure differs by field, since a txid is one it can hand back to the node, but the
  boundary is what matters: block hashes are anchored, block contents are not. Recomputing subtree
  roots from the snapshot's commitments and comparing them against `z_getsubtreesbyindex` would
  close the commitment half, and is the natural upgrade path. Until then, the guarantee is that the
  snapshot describes the operator's own chain at every height, not that everything inside each block
  is authentic.
- **Consuming over plaintext hands that same substitution to anyone on the path.** `--snapshot-url`
  accepts `http://` for a peer on a trusted network and warns at startup, since the contents a
  snapshot carries are exactly the part no layer ties back to the operator's node.
- **An import needs about 1.5 GB of free memory.** An epoch body is held whole so its digest can be
  checked before anything is written; its blocks are decoded one at a time rather than materialized
  together, which halves the peak but cannot remove the body itself. Verifying in sub-batches would
  lower it further at the cost of per-epoch atomicity.
- Only completed epochs are published, so a manifest is a growing prefix of the publisher's range
  rather than all of it, and the epoch holding the tip is never served.
- A server can only serve the range it holds, and only to a consumer on the same chain. Full-history
  bootstrap needs a full-history peer.
- The `meta` table is created empty on first open, so an existing cache gains it with no migration
  and an instance that passes neither flag behaves exactly as before.
- Serving multi-gigabyte downloads is a real bandwidth commitment; it is off by default, capped by
  `--snapshot-max-concurrent-downloads`, and warns loudly on a non-loopback bind address.
