//! Convert a raw Zcash block into a [`CompactBlock`].
//!
//! A block is `[header][CompactSize tx_count][tx]*`. The header is parsed by hand (fixed layout); each
//! transaction is parsed with `librustzcash`, which also computes the correct txid (legacy and ZIP-244).
//! The compact form keeps only what a shielded wallet needs: Sapling spends/outputs, Orchard actions, and
//! transparent inputs/outputs.
//!
//! Every `Transaction::read` call passes a fixed `BranchId::Nu5`: the branch ID is only consulted for
//! pre-v5 transactions (where it does not affect the legacy double-SHA txid); v5/v6 read it from the wire.

use std::io::Cursor;

use sha2::{Digest, Sha256};
use zcash_encoding::CompactSize;
use zcash_primitives::transaction::Transaction;
use zcash_protocol::consensus::BranchId;

use crate::encoding;
use crate::proto::{
    ChainMetadata, CompactBlock, CompactOrchardAction, CompactSaplingOutput, CompactSaplingSpend,
    CompactTx, CompactTxIn, TxOut,
};

/// Header layout up to (but not including) the equihash solution: version(4) + prevHash(32) +
/// merkleRoot(32) + blockCommitments(32) + time(4) + nBits(4) + nonce(32).
const HEADER_PREFIX_LEN: usize = 140;

/// First 52 bytes of a note's `encCiphertext`: the compact note plaintext used for trial decryption.
const COMPACT_CIPHERTEXT_LEN: usize = 52;

/// Smallest possible serialized transaction, used to bound the tx-count pre-allocation against a
/// malformed block declaring a huge count with a short body (`CompactSize::read` only caps at the
/// Zcash max ~33.5M, large enough to OOM on `Vec::with_capacity`).
const MIN_TX_BYTES: usize = 4;

/// Errors produced while parsing a raw block.
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    /// The buffer is shorter than the structure being read.
    #[error("block data is truncated")]
    Truncated,
    /// A transaction (or a CompactSize length prefix) failed to parse.
    #[error("reading block: {0}")]
    Io(#[from] std::io::Error),
    /// The block height could not be read from the coinbase transaction.
    #[error("could not read height from coinbase")]
    NoHeight,
    /// Bytes remain after the last declared transaction (an overlong / malformed block).
    #[error("block has trailing data after the last transaction")]
    TrailingData,
}

/// Parse a raw block and build its [`CompactBlock`]. The `chainMetadata` tree sizes are left at zero;
/// the caller fills them in from the node (they are not part of the raw block).
pub fn to_compact_block(raw: &[u8]) -> Result<CompactBlock, ParseError> {
    if raw.len() < HEADER_PREFIX_LEN {
        return Err(ParseError::Truncated);
    }

    let prev_hash = raw[4..36].to_vec();
    let time = u32::from_le_bytes([raw[100], raw[101], raw[102], raw[103]]);

    // The solution length prefix sits right after the fixed header prefix; the header ends after it.
    let mut header_cursor = Cursor::new(&raw[HEADER_PREFIX_LEN..]);
    let solution_len = CompactSize::read(&mut header_cursor)? as usize;
    let header_end = HEADER_PREFIX_LEN + header_cursor.position() as usize + solution_len;
    if raw.len() < header_end {
        return Err(ParseError::Truncated);
    }
    let hash = sha256d(&raw[..header_end]);

    let mut tx_cursor = Cursor::new(&raw[header_end..]);
    let tx_count = CompactSize::read(&mut tx_cursor)? as usize;
    let capacity = tx_count.min(raw[header_end..].len() / MIN_TX_BYTES);
    let mut vtx = Vec::with_capacity(capacity);
    let mut height = None;
    for index in 0..tx_count {
        let tx = Transaction::read(&mut tx_cursor, BranchId::Nu5)?;
        if index == 0 {
            height = Some(coinbase_height(&tx)?);
        }
        vtx.push(to_compact_tx(index as u64, &tx, index == 0));
    }
    // The declared transactions must consume the block exactly; leftover bytes mean a malformed or
    // desynced block, rejected rather than silently accepted (and cached).
    if header_end + tx_cursor.position() as usize != raw.len() {
        return Err(ParseError::TrailingData);
    }

    Ok(CompactBlock {
        proto_version: 0,
        height: height.ok_or(ParseError::NoHeight)?,
        hash,
        prev_hash,
        time,
        header: Vec::new(),
        vtx,
        chain_metadata: Some(ChainMetadata::default()),
    })
}

/// Double SHA-256, in protocol (little-endian) byte order — the on-wire block hash.
fn sha256d(data: &[u8]) -> Vec<u8> {
    Sha256::digest(Sha256::digest(data)).to_vec()
}

/// Read the block height from the coinbase transaction's BIP34 scriptSig.
fn coinbase_height(tx: &Transaction) -> Result<u64, ParseError> {
    let bundle = tx.transparent_bundle().ok_or(ParseError::NoHeight)?;
    let script = &bundle
        .vin
        .first()
        .ok_or(ParseError::NoHeight)?
        .script_sig()
        .0
        .0;
    let n = *script.first().ok_or(ParseError::NoHeight)? as usize;
    if n == 0 || n > 8 || script.len() < 1 + n {
        return Err(ParseError::NoHeight);
    }
    let mut bytes = [0u8; 8];
    bytes[..n].copy_from_slice(&script[1..1 + n]);
    Ok(u64::from_le_bytes(bytes))
}

/// Build the compact form of a single transaction. A coinbase omits its (null) inputs.
fn to_compact_tx(index: u64, tx: &Transaction, is_coinbase: bool) -> CompactTx {
    let mut spends = Vec::new();
    let mut outputs = Vec::new();
    if let Some(sapling) = tx.sapling_bundle() {
        for spend in sapling.shielded_spends() {
            spends.push(CompactSaplingSpend {
                nf: spend.nullifier().0.to_vec(),
            });
        }
        for output in sapling.shielded_outputs() {
            outputs.push(CompactSaplingOutput {
                cmu: output.cmu().to_bytes().to_vec(),
                ephemeral_key: output.ephemeral_key().0.to_vec(),
                ciphertext: output.enc_ciphertext()[..COMPACT_CIPHERTEXT_LEN].to_vec(),
            });
        }
    }

    let mut actions = Vec::new();
    if let Some(orchard) = tx.orchard_bundle() {
        for action in orchard.actions().iter() {
            let note = action.encrypted_note();
            actions.push(CompactOrchardAction {
                nullifier: action.nullifier().to_bytes().to_vec(),
                cmx: action.cmx().to_bytes().to_vec(),
                ephemeral_key: note.epk_bytes.to_vec(),
                ciphertext: note.enc_ciphertext[..COMPACT_CIPHERTEXT_LEN].to_vec(),
            });
        }
    }

    let mut vin = Vec::new();
    let mut vout = Vec::new();
    if let Some(transparent) = tx.transparent_bundle() {
        if !is_coinbase {
            for input in &transparent.vin {
                vin.push(CompactTxIn {
                    prevout_txid: input.prevout().hash().to_vec(),
                    prevout_index: input.prevout().n(),
                });
            }
        }
        for output in &transparent.vout {
            vout.push(TxOut {
                value: output.value().into_u64(),
                script_pub_key: output.script_pubkey().0.0.clone(),
            });
        }
    }

    CompactTx {
        index,
        txid: tx.txid().as_ref().to_vec(),
        fee: 0,
        spends,
        outputs,
        actions,
        vin,
        vout,
    }
}

/// Parse a single raw transaction (as from `getrawtransaction <txid> 0`) into its compact form.
/// Used for mempool transactions, which are never coinbases.
pub fn compact_tx_from_raw(index: u64, raw: &[u8]) -> Result<CompactTx, ParseError> {
    let transaction = Transaction::read(&mut Cursor::new(raw), BranchId::Nu5)?;
    Ok(to_compact_tx(index, &transaction, false))
}

/// Split a raw block into its header bytes and the raw bytes of each transaction.
///
/// The inverse of `header + CompactSize(txs.len()) + txs.concat()`: the header runs up to (and
/// including) the equihash solution, and each transaction's slice is recovered from the cursor
/// positions before and after `Transaction::read`. Used by darkside to hold blocks as
/// `(header, Vec<tx_bytes>)` and rebuild them on demand.
pub fn split_block(raw: &[u8]) -> Result<(Vec<u8>, Vec<Vec<u8>>), ParseError> {
    if raw.len() < HEADER_PREFIX_LEN {
        return Err(ParseError::Truncated);
    }
    let mut header_cursor = Cursor::new(&raw[HEADER_PREFIX_LEN..]);
    let solution_len = CompactSize::read(&mut header_cursor)? as usize;
    let header_end = HEADER_PREFIX_LEN + header_cursor.position() as usize + solution_len;
    if raw.len() < header_end {
        return Err(ParseError::Truncated);
    }
    let header = raw[..header_end].to_vec();

    let mut tx_cursor = Cursor::new(&raw[header_end..]);
    let tx_count = CompactSize::read(&mut tx_cursor)? as usize;
    let capacity = tx_count.min(raw[header_end..].len() / MIN_TX_BYTES);
    let mut txs = Vec::with_capacity(capacity);
    for _ in 0..tx_count {
        let start = tx_cursor.position() as usize;
        Transaction::read(&mut tx_cursor, BranchId::Nu5)?;
        let end = tx_cursor.position() as usize;
        txs.push(raw[header_end + start..header_end + end].to_vec());
    }
    if header_end + tx_cursor.position() as usize != raw.len() {
        return Err(ParseError::TrailingData);
    }
    Ok((header, txs))
}

/// Append the canonical Bitcoin `CompactSize` encoding of `value` to `buf`. Infallible (writes to a
/// growable buffer), unlike `zcash_encoding::CompactSize::write`, which returns an `io::Result`.
pub fn write_compact_size(buf: &mut Vec<u8>, value: u64) {
    if value < 253 {
        buf.push(value as u8);
    } else if value <= u16::MAX as u64 {
        buf.push(253);
        buf.extend_from_slice(&(value as u16).to_le_bytes());
    } else if value <= u32::MAX as u64 {
        buf.push(254);
        buf.extend_from_slice(&(value as u32).to_le_bytes());
    } else {
        buf.push(255);
        buf.extend_from_slice(&value.to_le_bytes());
    }
}

/// Read the block height from a raw coinbase transaction's BIP34 scriptSig.
pub fn coinbase_height_from_raw(raw_tx: &[u8]) -> Result<u64, ParseError> {
    let transaction = Transaction::read(&mut Cursor::new(raw_tx), BranchId::Nu5)?;
    coinbase_height(&transaction)
}

/// Count the Sapling outputs and Orchard actions in a raw transaction, used to grow the
/// note-commitment tree sizes as darkside mines transactions into blocks.
pub fn shielded_counts(raw_tx: &[u8]) -> Result<(u32, u32), ParseError> {
    let transaction = Transaction::read(&mut Cursor::new(raw_tx), BranchId::Nu5)?;
    let sapling_outputs = transaction
        .sapling_bundle()
        .map(|bundle| bundle.shielded_outputs().len() as u32)
        .unwrap_or(0);
    let orchard_actions = transaction
        .orchard_bundle()
        .map(|bundle| bundle.actions().len() as u32)
        .unwrap_or(0);
    Ok((sapling_outputs, orchard_actions))
}

/// Compute the display-order (big-endian) hex txid of a raw transaction, matching what a wallet
/// derives from `CompactTx.txid`.
pub fn txid_display(raw_tx: &[u8]) -> Result<String, ParseError> {
    let transaction = Transaction::read(&mut Cursor::new(raw_tx), BranchId::Nu5)?;
    Ok(encoding::wire_to_display_hex(transaction.txid().as_ref()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{shielded_v5_txs, testdata_blocks, v6_coinbase_txs};
    use prost::Message;
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct Fixture {
        block: u64,
        full: String,
        compact: String,
    }

    // The reference fixtures were generated without transaction IDs (the txids are 32 zero bytes),
    // so we normalize ours to zero before comparing the rest of the structure byte-for-byte, while
    // separately asserting that we do compute a real txid for every transaction.
    #[test]
    fn compact_block_structure_matches_golden_fixtures() {
        let json = std::fs::read_to_string("testdata/compact_blocks.json").unwrap();
        let fixtures: Vec<Fixture> = serde_json::from_str(&json).unwrap();
        assert!(!fixtures.is_empty());
        for fixture in &fixtures {
            let raw = hex::decode(&fixture.full).unwrap();
            let mut compact = to_compact_block(&raw).unwrap();
            for tx in &mut compact.vtx {
                assert_eq!(tx.txid.len(), 32, "txid length at height {}", fixture.block);
                assert_ne!(
                    tx.txid,
                    vec![0u8; 32],
                    "missing txid at height {}",
                    fixture.block
                );
                tx.txid = vec![0u8; 32];
            }
            assert_eq!(
                hex::encode(compact.encode_to_vec()),
                fixture.compact,
                "compact block structure mismatch at height {}",
                fixture.block
            );
        }
    }

    #[test]
    fn shielded_counts_matches_v5_vectors() {
        let vectors = shielded_v5_txs();
        assert!(!vectors.is_empty());
        for (raw, sapling_outputs, orchard_actions) in vectors {
            assert_eq!(
                shielded_counts(&raw).unwrap(),
                (sapling_outputs, orchard_actions)
            );
        }
    }

    #[test]
    fn v6_coinbase_txids_match_the_node() {
        for (raw, _, expected_txid) in v6_coinbase_txs() {
            assert_eq!(txid_display(&raw).unwrap(), expected_txid);
        }
    }

    #[test]
    fn v6_coinbase_heights_read_from_coinbase_script() {
        for (raw, height, _) in v6_coinbase_txs() {
            assert_eq!(coinbase_height_from_raw(&raw).unwrap(), height);
        }
    }

    #[test]
    fn v6_coinbase_shielded_counts_are_zero() {
        for (raw, _, _) in v6_coinbase_txs() {
            assert_eq!(shielded_counts(&raw).unwrap(), (0, 0));
        }
    }

    #[test]
    fn empty_input_is_truncated() {
        assert!(matches!(to_compact_block(&[]), Err(ParseError::Truncated)));
    }

    #[test]
    fn input_shorter_than_header_prefix_is_truncated() {
        let raw = testdata_blocks().into_iter().next().unwrap();
        let short = &raw[..HEADER_PREFIX_LEN - 1];
        assert!(matches!(
            to_compact_block(short),
            Err(ParseError::Truncated)
        ));
    }

    #[test]
    fn solution_length_overrun_is_truncated() {
        // Keep the header prefix and the solution-length prefix but cut the buffer before the
        // equihash solution ends, so `header_end > raw.len()`.
        let raw = testdata_blocks().into_iter().next().unwrap();
        let mut header_cursor = Cursor::new(&raw[HEADER_PREFIX_LEN..]);
        let solution_len = CompactSize::read(&mut header_cursor).unwrap() as usize;
        let header_end = HEADER_PREFIX_LEN + header_cursor.position() as usize + solution_len;
        let truncated = &raw[..header_end - 1];
        assert!(matches!(
            to_compact_block(truncated),
            Err(ParseError::Truncated)
        ));
    }

    #[test]
    fn truncated_transaction_body_errors() {
        // A valid header followed by a transaction region cut short: the per-tx `Transaction::read`
        // fails. Drop the final 10 bytes, which fall inside the last transaction.
        let raw = testdata_blocks().into_iter().next().unwrap();
        let truncated = &raw[..raw.len() - 10];
        assert!(matches!(
            to_compact_block(truncated),
            Err(ParseError::Io(_))
        ));
    }

    #[test]
    fn out_of_range_coinbase_height_push_is_no_height() {
        // Corrupt the coinbase BIP34 height-push length byte to an out-of-range value (0). The byte
        // is located by its content: the push-length byte followed by the minimal little-endian
        // height bytes forms a sequence that occurs exactly once in the coinbase transaction.
        let raw = testdata_blocks().into_iter().next().unwrap();
        let (header, mut txs) = split_block(&raw).unwrap();
        let coinbase = &mut txs[0];
        let height = coinbase_height_from_raw(coinbase).unwrap();

        let mut height_le = Vec::new();
        let mut value = height;
        while value > 0 {
            height_le.push((value & 0xff) as u8);
            value >>= 8;
        }
        let mut needle = vec![height_le.len() as u8];
        needle.extend_from_slice(&height_le);
        let positions: Vec<usize> = coinbase
            .windows(needle.len())
            .enumerate()
            .filter(|(_, window)| *window == needle.as_slice())
            .map(|(index, _)| index)
            .collect();
        assert_eq!(
            positions.len(),
            1,
            "coinbase height push must be uniquely locatable"
        );
        coinbase[positions[0]] = 0;

        let mut corrupted = header;
        write_compact_size(&mut corrupted, txs.len() as u64);
        for tx in &txs {
            corrupted.extend_from_slice(tx);
        }
        assert!(matches!(
            to_compact_block(&corrupted),
            Err(ParseError::NoHeight)
        ));
    }

    #[test]
    fn oversized_tx_count_with_short_body_errors_without_panic() {
        // A valid header followed by a huge declared `tx_count` but only one real transaction: the
        // capacity hint must stay bounded (no multi-GB allocation) and the per-tx read loop must
        // fail cleanly once the body is exhausted.
        let raw = testdata_blocks().into_iter().next().unwrap();
        let (header, txs) = split_block(&raw).unwrap();

        let mut malformed = header;
        write_compact_size(&mut malformed, 33_554_432);
        malformed.extend_from_slice(&txs[0]);

        assert!(matches!(
            to_compact_block(&malformed),
            Err(ParseError::Io(_))
        ));
    }

    #[test]
    fn split_block_round_trips_header_and_transactions() {
        let blocks = testdata_blocks();
        assert!(!blocks.is_empty());
        for raw in blocks {
            let (header, txs) = split_block(&raw).unwrap();

            let mut rebuilt = header;
            write_compact_size(&mut rebuilt, txs.len() as u64);
            for tx in &txs {
                rebuilt.extend_from_slice(tx);
            }
            assert_eq!(rebuilt, raw, "split_block round-trip mismatch");
        }
    }

    #[test]
    fn trailing_data_after_last_transaction_is_rejected() {
        let raw = testdata_blocks().into_iter().next().unwrap();
        let mut overlong = raw.clone();
        overlong.extend_from_slice(&[0xde, 0xad, 0xbe, 0xef]);

        assert!(matches!(
            to_compact_block(&overlong),
            Err(ParseError::TrailingData)
        ));
        assert!(matches!(
            split_block(&overlong),
            Err(ParseError::TrailingData)
        ));
    }
}

#[cfg(test)]
mod property_tests {
    use super::*;
    use crate::testutil::testdata_blocks;
    use proptest::prelude::*;

    /// A real block with random mutations: byte flips, a random truncation point, and trailing junk.
    /// Flips run before truncation (indices are valid against the full block); the result may or may
    /// not still parse, which is the point — it drives the fuzzer past the header into the tx loop.
    fn mutated_block() -> impl Strategy<Value = Vec<u8>> {
        let base = testdata_blocks()
            .into_iter()
            .next()
            .expect("a testdata block");
        let len = base.len();
        (
            prop::collection::vec((0..len, any::<u8>()), 0..16), // (index, new_byte) flips
            0..=len,                                             // truncate-to length
            prop::collection::vec(any::<u8>(), 0..64),           // trailing junk
        )
            .prop_map(move |(flips, truncate_to, trailing)| {
                let mut bytes = base.clone();
                for (index, value) in flips {
                    bytes[index] = value;
                }
                bytes.truncate(truncate_to);
                bytes.extend_from_slice(&trailing);
                bytes
            })
    }

    proptest! {
        // (a) arbitrary bytes — must return Ok or Err, never panic.
        #[test]
        fn to_compact_block_never_panics_on_arbitrary_bytes(
            raw in prop::collection::vec(any::<u8>(), 0..2048),
        ) {
            let _ = to_compact_block(&raw);
        }

        #[test]
        fn split_block_never_panics_on_arbitrary_bytes(
            raw in prop::collection::vec(any::<u8>(), 0..2048),
        ) {
            let _ = split_block(&raw);
        }

        // (b) mutated valid block — exercises the header_end math and the tx-slice arithmetic.
        #[test]
        fn to_compact_block_never_panics_on_mutated_block(raw in mutated_block()) {
            let _ = to_compact_block(&raw);
        }

        #[test]
        fn split_block_never_panics_on_mutated_block(raw in mutated_block()) {
            let _ = split_block(&raw);
        }

        // When split_block accepts the input, the pieces must reassemble into it exactly.
        #[test]
        fn split_block_round_trips_when_ok(raw in mutated_block()) {
            if let Ok((header, txs)) = split_block(&raw) {
                let mut rebuilt = header;
                write_compact_size(&mut rebuilt, txs.len() as u64);
                for tx in &txs {
                    rebuilt.extend_from_slice(tx);
                }
                prop_assert_eq!(rebuilt, raw);
            }
        }
    }
}
