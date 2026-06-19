# Protocol references

The Zcash and Bitcoin specifications this implementation relies on, grouped by protocol layer. Each entry links a
spec to the place in this repository where it is implemented, so the code can be read against the rules it follows.
This is reference material for working on the code, not an explanation of the cryptography — each spec is the
authority on its own subject.

`Where:` pointers name a `module::function` or symbol when a line number would drift; stable structural anchors
(the block-header layout, the proto spec citations) are given as `file:line`.

## Light-client protocol

The reason this server exists: serve compact blocks so a shielded wallet can detect its payments without the full
chain.

- **[ZIP-307 — Light Client Protocol for Payment Detection](https://zips.z.cash/zip-0307)** — defines
  compact-block payment detection and trial decryption; it governs the shape of `CompactBlock`/`CompactTx` and the
  52-byte compact-note prefix kept from each output's ciphertext.
  *Where:* `proto/compact_formats.proto`, `src/compact.rs` (`COMPACT_CIPHERTEXT_LEN`, `to_compact_tx`).
- **[Light wallet gRPC interface (`CompactTxStreamer`)](https://github.com/zcash/lightwallet-protocol)** — the
  canonical light-client `.proto` set; the gRPC contract this server implements.
  *Where:* `proto/service.proto`, `proto/compact_formats.proto`, `src/proto.rs`.

## Transaction format & identifiers

- **[ZIP-225 — Version 5 Transaction Format](https://zips.z.cash/zip-0225)** — the v5 serialization that
  `librustzcash` parses to recover Sapling, Orchard, and transparent bundles.
  *Where:* `src/compact.rs` `to_compact_block` (`Transaction::read(.., BranchId::Nu5)`).
- **[ZIP-244 — Transaction Identifier Non-Malleability](https://zips.z.cash/zip-0244)** — the v5 TxId digest,
  computed locally from the raw block (the project's headline design decision); also renames the header's
  `hashLightClientRoot` to `hashBlockCommitments`.
  *Where:* `src/compact.rs` `to_compact_tx` (`tx.txid()`), `src/encoding.rs` (display ↔ wire byte order),
  `docs/ARCHITECTURE.md` "Local txid computation".
- **[Zcash Protocol Specification §7.1 — Transaction Encoding and Consensus / Transaction Identifiers](https://zips.z.cash/protocol/protocol.pdf#txnidentifiers)**
  — the authoritative transaction and txid definition the proto already cites.
  *Where:* `proto/compact_formats.proto:51`.

## Shielded protocols

- **[Sapling — Protocol Specification §4 / §7.3](https://zips.z.cash/protocol/protocol.pdf#spendencodingandconsensus)**
  ([output encoding](https://zips.z.cash/protocol/protocol.pdf#outputencodingandconsensus)) — spend nullifiers,
  the note commitment `cmu`, the ephemeral key, and the 52-byte ciphertext prefix. Sapling is specified in the
  protocol document, not a single ZIP.
  *Where:* `src/compact.rs` `to_compact_tx` (`sapling_bundle` → `CompactSaplingSpend` / `CompactSaplingOutput`).
- **[ZIP-224 — Orchard](https://zips.z.cash/zip-0224)** — the Orchard protocol over the Pallas curve (action
  nullifier, the `cmx` commitment), shipped in NU5; action encoding lives in
  [§ Action Encoding and Consensus](https://zips.z.cash/protocol/protocol.pdf#actionencodingandconsensus).
  *Where:* `src/compact.rs` `to_compact_tx` (`orchard_bundle` → `CompactOrchardAction`).

## Note commitment trees, subtree roots & tree state

- **[Note commitment trees — Protocol Specification (incremental Merkle tree / frontier)](https://zips.z.cash/protocol/protocol.pdf#merkletree)**
  — the `z_gettreestate` frontier (`finalState`) is served verbatim for wallet witness construction.
  *Where:* `src/service/treestate.rs`, `src/node` `get_treestate` (`z_gettreestate`).
- **[Subtree roots (2^16-leaf subtrees)](https://github.com/zcash/zcash/issues/6336)** — there is **no dedicated
  ZIP**; the canonical references are the `z_getsubtreesbyindex` RPC (zcash/zcash issue #6336, shipped in zcashd
  v5.6.0) and the spend-before-sync wallet sync algorithm.
  *Where:* `src/service/subtrees.rs` `get_subtree_roots`, `src/node` `get_subtrees` (`z_getsubtreesbyindex`).

## Chain history & block commitments

- **[ZIP-221 — FlyClient — Consensus-Layer Changes](https://zips.z.cash/zip-0221)** — defines the chain-history
  MMR (`hashChainHistoryRoot`) and the header `blockCommitments` field (renamed by ZIP-244).
  *Where:* `src/compact.rs:21-23` (block-header layout, `blockCommitments`).

## Network upgrades

- **[ZIP-200 — Network Upgrade Mechanism](https://zips.z.cash/zip-0200)** — consensus branch IDs and activation
  heights; why the parser pins a branch and why `--start-height` defaults to Sapling activation.
  *Where:* `BranchId::Nu5` in `src/compact.rs`, `docs/ARCHITECTURE.md` "Running" (`--start-height` default).
- **[ZIP-252 — Deployment of the NU5 Network Upgrade](https://zips.z.cash/zip-0252)** — NU5 = v5 transactions plus
  Orchard, the upgrade the parser targets. (Surrounding deployments: [ZIP-250 Heartwood](https://zips.z.cash/zip-0250),
  [ZIP-251 Canopy](https://zips.z.cash/zip-0251).)
  *Where:* `BranchId::Nu5` in `src/compact.rs`.
- **[Network Upgrade Guide (activation-height table)](https://zcash.readthedocs.io/en/latest/rtd_pages/nu_dev_guide.html)**
  — the activation heights per network.

## Bitcoin-inherited primitives

- **[BIP-34 — Block v2, Height in Coinbase](https://github.com/bitcoin/bips/blob/master/bip-0034.mediawiki)** —
  the block height read from the coinbase transaction's scriptSig.
  *Where:* `src/compact.rs` `coinbase_height`.
- **[Block hashing — double SHA-256](https://zips.z.cash/protocol/protocol.pdf#blockheader)** — internal
  little-endian, display big-endian, the Bitcoin convention.
  *Where:* `src/compact.rs` `sha256d`, `src/encoding.rs`.
- **[CompactSize (variable-length integer)](https://developer.bitcoin.org/reference/transactions.html#compactsize-unsigned-integers)**
  — the Bitcoin "CompactSize unsigned integer" used for transaction counts and the solution / script lengths.
  *Where:* `src/compact.rs` `write_compact_size`, `zcash_encoding::CompactSize`.
- **[Equihash (Proof of Work) — Protocol Specification §7.7.2](https://zips.z.cash/protocol/protocol.pdf#equihash)**
  — the variable-length equihash solution that closes the block header.
  *Where:* `src/compact.rs` `to_compact_block` (solution-length parse).

## Transparent / address layer

- **[Transparent transactions (P2PKH/P2SH, scriptPubKey, UTXO model)](https://zips.z.cash/protocol/protocol.pdf#transactions)**
  — Bitcoin-derived transparent inputs and outputs.
  *Where:* `src/compact.rs` `to_compact_tx` (`transparent_bundle` → `CompactTxIn` / `TxOut`).
- **[ZIP-209 — Prohibit Negative Shielded Value Pool](https://zips.z.cash/zip-0209)** — the `valueBalanceSapling`
  and `valueBalanceOrchard` quantities referenced by the fee formula.
  *Where:* `proto/compact_formats.proto:60-62`.
- **`addressindex` RPCs (`getaddressbalance`, `getaddressutxos`, `getaddresstxids`)** — the Bitcoin addressindex /
  Insight extension exposed by zebra for transparent-address queries.
  *Where:* `src/service/address.rs`, `src/node` (`get_address_balance`, `get_address_utxos`, `get_address_txids`).
