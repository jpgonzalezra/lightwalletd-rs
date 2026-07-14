# 0023. Hybrid Zebra ReadStateService backend behind the NodeRpc seam

## Context

All node access goes through JSON-RPC today: HTTP + JSON + hex for every block, tree state, subtree,
and address query. Co-located deployments pay that transport tax on every read. Zebra exposes the
same data in-process: `zebra_state::init_read_only` attaches a read-only secondary RocksDB instance
to a running zebrad's state, served through `ReadStateService` (a tower service whose `ReadRequest`
surface covers blocks, headers, transactions, all three note-commitment trees and subtree ranges,
and the transparent address index). Two hard constraints shape the design: zebra's finalized state
lags the best tip by up to `MAX_BLOCK_REORG_HEIGHT = 1000` blocks, so a bare secondary cannot serve
the tip; and tx submission plus the mempool only exist on the node side. Zebra also ships the
missing piece: `zebra_rpc::sync::TrustedChainSync` follows a primary's non-finalized chain over the
indexer gRPC and feeds it into a secondary `ReadStateService`. Zebra 6.0.0 uses the final
librustzcash cohort we had pinned as pre-releases (ADR 0019's planned re-bump), so one dependency
graph is possible. Full analysis: [docs/design/zebra-readstate-backend.md](../design/zebra-readstate-backend.md).

## Decision

Add a second `NodeRpc` implementation, `ZebraStateNode`, selected by `--backend readstate`, behind a
non-default cargo feature `readstate`:

- Reads (blocks, trees, subtrees, address index, mined transactions, tip/chain info) go to an
  in-process `ReadStateService` wired by `zebra_rpc::sync::init_read_state_with_syncer`, which pairs
  the read-only secondary with `TrustedChainSync` over the zebrad indexer gRPC for true-tip
  fidelity.
- Writes and node-only data (`sendrawtransaction`, mempool, `getinfo`) keep using the JSON-RPC
  client inside the same backend — a hybrid, by design.
- Raw block bytes are produced by `ZcashSerialize` from the state's `Block` and fed to the existing,
  golden-fixture-verified parser; the txid cross-check reads `TransactionIdsForBlock`. Wire output
  is byte-identical by construction and verified by parity tests.
- The mapping layer is generic over the tower service so it unit-tests against a scripted service;
  `rpc` remains the default backend and the only one for remote nodes.

## Consequences

- Co-located reads skip HTTP, JSON, and hex entirely; the ingest bottleneck moves to parse+commit.
- The `readstate` build couples to zebra's state format major (v28 ↔ zebra 6.x) and its crate
  cohort; a mismatch fails fast at startup with a pointer to `--backend rpc`. Operating it requires
  a same-host zebrad with `indexer_listen_addr` enabled.
- The librustzcash pre-release pins are replaced by finals, closing ADR 0019's follow-up and
  aligning with zebra's cohort.
- Heavy dependencies (RocksDB via zebra-state, zebra-rpc/zebra-chain) are accepted, but only behind
  the non-default feature; the default build and CI lane stay lean (ADR 0012 intact).
- The NodeRpc seam (ADR 0007) absorbs the whole change: service layer, ingestor (ADR 0020), cache,
  mempool monitor (ADR 0005/0021), and darkside are untouched.
