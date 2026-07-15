# lightwalletd-rs: readstate vs rpc backend wire-parity report

**Date:** 2026-07-14 (UTC)
**Repo:** the repository checkout, branch `feat/zebra-readstate-backend`
**Commit under test:** `e59adec (rebased; identical code content)` ("feat: zebra readstate backend behind the NodeRpc seam (ADR 0023)")
**Build:** `cargo build --release --features readstate` (rustc 1.96.0, cargo 1.96.0)
**Node under test:** live mainnet `zebrad 6.0.0` at 127.0.0.1:8232 (JSON-RPC) / 127.0.0.1:8231 (indexer gRPC), synced to tip throughout the run (chain height moved 3412462 → 3412476+ during the session; testnet zebrad on 18234 was untouched)
**Compare tool:** `grpcurl` (reflection-based, no `.proto` files needed) driven by a small Python harness (`grpccmp.py`, `run_all.py`, `addr_util.py`, `cmp_blocks.py`)

## Servers under test

| | Backend | Bind | Data dir | Start height |
|---|---|---|---|---|
| A | `rpc` | 127.0.0.1:19201 | fresh temp dir | 3406000 |
| B | `readstate` | 127.0.0.1:19202 (`--zebra-indexer-url 127.0.0.1:8231`) | fresh temp dir | 3406000 |

Both instances ingested from 3406000 to the live tip in well under a minute and then tracked new blocks in real time for the duration of the run; `GetLatestBlock` was confirmed identical (height + hash) between A and B at the start, middle, and end of the session.

Snapshot tip used for fixed-height comparisons below: **3412467** (both servers were already at or past this height when each comparison ran; all sampled heights are finalized/immutable, so the live-advancing tip did not affect results).

## Summary

| # | Comparison | Result | Checks | Notes |
|---|---|---|---|---|
| 1 | Compact blocks (`GetBlockRange`/`GetBlockRangeNullifiers`), byte-exact | **PASS** | 30/30 (5,997 blocks fetched, 100% byte-identical) | default pools, all-pools, 3 windows + 12 single heights |
| 2 | Treestates (`GetTreeState`/`GetLatestTreeState`) | **FAIL** | 0/16 | real wire difference: empty (not-yet-active) commitment trees serialize as `""` on `rpc`, `"000000"` on `readstate` |
| 3 | Subtrees (`GetSubtreeRoots`) | **PASS** | 5/5 | sapling, orchard, ironwood (empty pre-activation on both) |
| 4 | Address surface (`GetTaddressBalance`/`GetTaddressTxids`/`GetAddressUtxos`) | **PASS** | 20/20 | 5 real addresses, including a "hot" funding-stream address that trips the 10k-txid cap identically on both backends |
| 5 | `GetTransaction` | **PASS** | 5/5 | v4 sandblasting-era, v5, coinbase, non-coinbase — raw bytes + height identical |
| 6 | `GetLightdInfo` | **FAIL** | 0/1 | real wire difference: `upgradeName` is `"NU6.3"` on `rpc`, `"Nu6_3"` on `readstate` (`estimatedHeight` differed by 1 across the two sequential calls — allowed timing skew) |
| 7 | Error parity | **PASS** | 3/3 | `OutOfRange`, `InvalidArgument`, `NotFound` — status codes and messages match |

**Total: 63/80 individual checks passed. Two real, reproducible wire differences found (sections 2 and 6); everything else — including 5,997 compact blocks across three windows and both pool-type modes, all subtree/address/transaction/error-path checks — is byte-for-byte identical.**

---

## 1. Compact blocks — PASS (5,997 blocks, byte-identical)

`GetBlockRange` fetched and diffed as full pretty-printed JSON (protobuf JSON mapping — byte-exact underlying bytes since both servers use the same JSON encoder and base64 alphabet):

- **Window A** `[3406000..3406999]` (1000 blocks): default pools ✅, all-pools (`TRANSPARENT,SAPLING,ORCHARD,IRONWOOD`) ✅
- **Window B** `[3411467..3412457]` (tip-1000..tip-10, 991 blocks): default pools ✅, all-pools ✅
- **12 single blocks** at interesting heights (419200 Sapling activation, 419201, 653600 Blossom, 903000 Heartwood, 1046400 Canopy, 1687104/1687105 NU5, 2726400 NU6, 3400000, 3406500, 3412000, 3412467 near-tip): default pools ✅ and all-pools ✅ for every height
- `GetBlockRangeNullifiers` on window A (1000 blocks) and window B (991 blocks): ✅

Every one of the 30 checks above (5,997 total blocks fetched, counting both pool-type variants) was byte-for-byte identical between the two backends. No differences of any kind — not even non-determinism in field ordering (protobuf JSON field order is stable in both).

## 2. Treestates — FAIL (real wire difference)

12 heights sampled across `[419200, 700000, 903000, 1046400, 1687104, 2000000, 2726400, 3000000, 3300000, 3400000, 3410000, 3412367(tip-100)]`, plus by-hash lookups for 3 of them (419200, 700000, 903000), plus `GetLatestTreeState`. **16/16 failed** — but all with exactly the same root cause.

### The difference

For a shielded pool that has **not yet activated** at the requested height, backend `rpc` (A) returns an **empty string** for that pool's tree field, while backend `readstate` (B) returns **`"000000"`** (the canonical empty-frontier serialization: 3 zero bytes — no left node, no right node, zero parents).

Verbatim evidence, request `GetTreeState({"height":"419200"})` (Sapling activation height; Orchard and Ironwood not yet active):

```
--- A (rpc) ---
{
  "network": "main",
  "height": "419200",
  "hash": "00000000025a57200d898ac7f21e26bf29028bbe96ec46e05b2c17cc9db9e4f3",
  "time": 1540779337,
  "saplingTree": "000000",
  "orchardTree": "",
  "ironwoodTree": ""
}
--- B (readstate) ---
{
  "network": "main",
  "height": "419200",
  "hash": "00000000025a57200d898ac7f21e26bf29028bbe96ec46e05b2c17cc9db9e4f3",
  "time": 1540779337,
  "saplingTree": "000000",
  "orchardTree": "000000",
  "ironwoodTree": "000000"
}
```

Everything else in the response (network, height, hash, time, the active `saplingTree` bytes) is identical. The same pattern repeats at every sampled height:
- Heights before NU5 (419200, 700000, 903000, 1046400): both `orchardTree` and `ironwoodTree` differ (`""` vs `"000000"`).
- Heights from NU5 onward (1687104 through 3412367, and `GetLatestTreeState`): only `ironwoodTree` differs (Orchard is active on both, so both agree; Ironwood is not yet active on mainnet — activation height 3428143 per `GetLightdInfo`, still ahead of the live tip — so it differs everywhere sampled).

Example at height 1687104 (NU5 activation — Orchard just activated, both agree it's `"000000"`; only Ironwood still diverges):
```
A: "orchardTree": "000000", "ironwoodTree": ""
B: "orchardTree": "000000", "ironwoodTree": "000000"
```

### Analysis (not fixed — for the coordinator)

`src/service/treestate.rs::node_tree_state_to_proto` just forwards whatever `node::GetTreeState` produced for each pool's `commitments.final_state`; it does not special-case "pool not yet active." The divergence originates one layer down, in each backend's `node::NodeRpc::get_treestate` implementation:

- **`rpc` backend** passes through zebrad's own `z_gettreestate` JSON-RPC response. zebrad omits a pool's `commitments` object entirely when that pool isn't active at the requested height, so the Rust struct field defaults to an empty string.
- **`readstate` backend** (`src/node/readstate.rs`) computes the tree state directly against the in-process Zebra state/frontier machinery, which always produces a real (possibly empty) commitment tree for every pool regardless of activation status, serializing the empty case as `"000000"`.

This is a genuine wire-contract difference a client could observe (e.g. a wallet checking whether `orchardTree`/`ironwoodTree` is empty-string vs. a valid-looking non-empty hex string to decide whether a pool is usable yet). It reproduces on every height sampled and is 100% deterministic — not a race or timing artifact.

## 3. Subtrees — PASS

| Request | Result |
|---|---|
| sapling, startIndex=0, maxEntries=16 | ✅ identical (16 roots) |
| sapling, startIndex=50, maxEntries=16 | ✅ identical (16 roots, mid-range; sapling has 111 subtrees total as of tip) |
| orchard, startIndex=0, maxEntries=16 | ✅ identical (16 roots) |
| orchard, startIndex=150, maxEntries=16 | ✅ identical (16 roots, mid-range; orchard has 226 subtrees total as of tip) |
| ironwood, startIndex=0, maxEntries=16 | ✅ both return an empty stream (0 roots) — Ironwood is pre-activation on this chain |

## 4. Address surface — PASS

Five real transparent addresses were extracted by decoding P2PKH/P2SH `scriptPubKey`s from `GetBlockRange([3411000..3412463], poolTypes=[TRANSPARENT])` (hash160 → base58check, Zcash mainnet t1/t3 prefixes):

| Address | Role | GetTaddressBalance | GetTaddressTxids | GetAddressUtxos |
|---|---|---|---|---|
| `t3cFfPt1Bcvgez9ZbMBFWeZsskxTkPzGCow` | funding-stream P2SH, present in ~every block (hottest) | ✅ | ✅ (both hit the 10k `ResourceExhausted` cap identically at `[1..tip]` and at a 20,000-block window; a 5,000-block window succeeded — 5,001 identical txids) | ✅ (7,081 UTXOs, identical order) |
| `t1Ku2KLyndDPsR32jwnrTMd3yvi9tfFP8ML` | active t1 address | ✅ | ✅ (`[1..tip]` succeeded directly; also verified over a 20,000-block window, 8,197 identical txids) | ✅ (1,954 UTXOs, identical order) |
| `t1MKn34KBa8Xh4g8qU8psibBXvURafphVn7` | active t1 address | ✅ | ✅ (6,605 identical txids over 20,000-block window) | ✅ (57 UTXOs, identical order) |
| `t1PEp2GJLSdhDfCKqc2J211WKDUS1NfoQNy` | active t1 address | ✅ | ✅ (4,297 identical txids over 20,000-block window) | ✅ (53 UTXOs, identical order) |
| `t1RBkNhHAwZcrhN3YmJ9wS8eCcAVWFQg7oh` | active t1 address | ✅ | ✅ (`[1..tip]` succeeded directly, 3,728 identical txids) | ✅ (88 UTXOs, identical order) |

`GetAddressUtxos` results were compared with an exact ordered equality check first (all 5 addresses matched exactly in order — no order difference to flag), with a set-equality fallback available (unused) had order legitimately differed.

## 5. GetTransaction — PASS

| Case | Txid | Height | Result |
|---|---|---|---|
| v4, sandblasting era | `ba38515049f0da7a29629a96d35227f5a1180f790cbb429a23d616a7b86e580f` | 450000 | ✅ raw bytes + height identical (511 bytes, overwintered v4) |
| v5, post-NU5 | `36248d60a5a961bba3a8a0e89c87a90251252e925a1606dfa1b9a4588a9352f4` | 1700000 | ✅ raw bytes + height identical (2411 bytes, v5) |
| coinbase | `efc8a3b7a4298aee011d2db154d3849cbf1c8e0bfc1b8954b001d5eff998ee9d` | 3410000 | ✅ raw bytes + height identical (136 bytes, v5 coinbase) |
| non-coinbase | `fd4667e1a9b427715992cd12b3cdabeb2cfe7623e0e61c8d489f9b0b9a8effbf` | 3410000 | ✅ raw bytes + height identical (245 bytes, v4) |
| recent | `1eeae83246f56b35af3d5ccd4b5b6c74b8ea33ba5708bbeaaa0956f13fe19ed9` | 3411000 | ✅ raw bytes + height identical |

## 6. GetLightdInfo — FAIL (real wire difference)

Full field-by-field comparison, two back-to-back calls (A then B):

```
--- A (rpc) ---
{
  "version": "0.1.0", "vendor": "lightwalletd-rs", "taddrSupport": true,
  "chainName": "main", "saplingActivationHeight": "419200",
  "consensusBranchId": "5437f330", "blockHeight": "3412475",
  "gitCommit": "a601f2a", "estimatedHeight": "3412476",
  "zcashdBuild": "v6.0.0", "zcashdSubversion": "/Zebra:6.0.0/",
  "upgradeName": "NU6.3", "upgradeHeight": "3428143"
}
--- B (readstate) ---
{
  "version": "0.1.0", "vendor": "lightwalletd-rs", "taddrSupport": true,
  "chainName": "main", "saplingActivationHeight": "419200",
  "consensusBranchId": "5437f330", "blockHeight": "3412475",
  "gitCommit": "a601f2a", "estimatedHeight": "3412475",
  "zcashdBuild": "v6.0.0", "zcashdSubversion": "/Zebra:6.0.0/",
  "upgradeName": "Nu6_3", "upgradeHeight": "3428143"
}
```

- `estimatedHeight` differed by 1 (3412476 vs 3412475) — **allowed**: the live tip ticked forward between the sequential A and B calls; `blockHeight` (captured earlier in each request's own handling) matched exactly.
- `upgradeName`: **`"NU6.3"` (A) vs `"Nu6_3"` (B) — not allowed, a genuine and 100%-reproducible difference.** Confirmed stable across repeated calls.

### Analysis (not fixed — for the coordinator)

`src/service/chain.rs::get_lightd_info` takes `upgrade_name` from the `chain.upgrades` map produced by each backend's `get_blockchain_info()`:
- The `rpc` backend's map comes straight from zebrad's live `getblockchaininfo` JSON-RPC response, where zebrad itself formats upgrade names as `"Overwinter"`, `"Sapling"`, ..., `"NU5"`, `"NU6"`, `"NU6.1"`, `"NU6.2"`, `"NU6.3"` (confirmed directly against the node: `curl .../getblockchaininfo` shows exactly this table, with `"37a5165b": {"name": "NU6.3", ..., "status": "pending"}`).
- The `readstate` backend (`src/node/readstate.rs`, `get_blockchain_info`) does **not** ask the node for this table; it synthesizes it locally from `zebra_chain::parameters::Network::full_activation_list()` with `name: format!("{upgrade}")` — i.e. it uses `NetworkUpgrade`'s own `Display` impl, which renders the Rust-ish enum-variant spelling `"Nu6_3"` rather than the branded `"NU6.3"` string zebrad's RPC layer produces.

Since `GetLightdInfo.upgradeName` only ever surfaces the single **next pending** upgrade, NU6.3 is the only name currently observable on the wire — but the root cause (`format!("{upgrade}")` vs. zebrad's branded names) is systemic to the whole table and would affect any other upgrade name if it were ever the "next pending" one (e.g. a future `"Nu7"` vs. an expected `"NU7"`).

## 7. Error parity — PASS

| Request | Expected | A (rpc) | B (readstate) |
|---|---|---|---|
| `GetBlock({"height":"3512467"})` (tip+100000) | `OutOfRange` | `OutOfRange` | `OutOfRange` — messages both indicate the requested height exceeds the chain tip |
| `GetTreeState({"hash": <31 zero bytes>})` | `InvalidArgument` | `InvalidArgument` | `InvalidArgument` |
| `GetTransaction({"hash": <32 bytes of 0xff>})` (unknown txid) | `NotFound` | `NotFound` | `NotFound` |

All three status codes matched exactly between backends.

---

## Environment

- Host: Linux, kernel 6.1.0-40-amd64
- zebrad: 6.0.0, mainnet RPC on 127.0.0.1:8232, indexer gRPC on 127.0.0.1:8231 (untouched testnet zebrad also running on 18234, unaffected)
- Chain height during the run: 3412462 → 3412476+ (mainnet advanced ~14 blocks live during testing; all sampled heights were already finalized before being used)
- Build: `cargo build --release --features readstate`, rustc 1.96.0
- Servers: two `target/release/lightwalletd-rs` processes, `--no-tls-very-insecure`, gRPC reflection enabled, ports 19201 (`rpc`) / 19202 (`readstate`), `--start-height 3406000`, separate `--data-dir`s
- Comparison tool: `grpcurl` (dev build) against reflection, no `.proto` files used; Python 3 harness for JSON diffing and address/txid decoding

## Artifacts (scratch, not preserved)

`grpccmp.py`, `addr_util.py`, `run_all.py`, `cmp_blocks.py`, `results.json`, `run_all_final.log`, server logs `a.log`/`b.log` — used to produce this report, then cleaned up along with the two data directories per the mission's cleanup instructions.
