//! Raw-block helpers and the [`ActiveBlock`] the mock chain holds (split so it can be re-serialized
//! after mutation).

use sha2::{Digest, Sha256};

use crate::compact;
use crate::encoding;

use super::error::DarksideError;

/// Byte length of a synthetic block header: the 140-byte fixed prefix, the 3-byte CompactSize prefix
/// for the 1344-byte equihash solution, and the solution itself.
const SYNTHETIC_HEADER_LEN: usize = 1487;

/// One block presented by the mock chain, held split so it can be re-serialized after mutation.
pub(super) struct ActiveBlock {
    pub(super) header: Vec<u8>,
    pub(super) txs: Vec<Vec<u8>>,
    pub(super) sapling_size: u32,
    pub(super) orchard_size: u32,
    pub(super) ironwood_size: u32,
}

impl ActiveBlock {
    /// Re-serialize to the raw block format `header + CompactSize(tx_count) + txs`.
    pub(super) fn to_raw(&self) -> Vec<u8> {
        let mut raw = self.header.clone();
        compact::write_compact_size(&mut raw, self.txs.len() as u64);
        for tx in &self.txs {
            raw.extend_from_slice(tx);
        }
        raw
    }

    /// The on-wire (protocol order) block hash.
    pub(super) fn hash(&self) -> [u8; 32] {
        sha256d(&self.header)
    }

    /// The display-order (big-endian hex) block hash, as zebrad reports it.
    pub(super) fn display_hash(&self) -> String {
        encoding::wire_to_display_hex(&self.hash())
    }
}

/// Double SHA-256 in protocol (little-endian) byte order.
fn sha256d(data: &[u8]) -> [u8; 32] {
    Sha256::digest(Sha256::digest(data)).into()
}

/// Rewrite each active block's `prevHash` (header bytes `4..36`) so the chain links together; the
/// first block's prev hash is left as staged.
pub(super) fn set_prevhash(active: &mut [ActiveBlock]) {
    let mut prev_hash: Option<[u8; 32]> = None;
    for block in active.iter_mut() {
        if let Some(hash) = prev_hash {
            block.header[4..36].copy_from_slice(&hash);
        }
        prev_hash = Some(sha256d(&block.header));
    }
}

/// Read the BIP34 height from a raw block's coinbase.
pub(super) fn raw_block_height(raw: &[u8]) -> Result<u64, DarksideError> {
    let (_, txs) = compact::split_block(raw)?;
    let coinbase = txs
        .first()
        .ok_or_else(|| DarksideError::Invalid("block has no transactions".to_string()))?;
    Ok(compact::coinbase_height_from_raw(coinbase)?)
}

/// Build a synthetic empty block (a single fake coinbase carrying `height`) for `StageBlocksCreate`.
pub(super) fn synthetic_block(height: i32, nonce: i32) -> Result<Vec<u8>, DarksideError> {
    // A real coinbase from block 797905 (little-endian height 0xD12C0C00); the height bytes are
    // patched to the requested height below.
    const FAKE_COINBASE: &str = concat!(
        "0400008085202f890100000000000000000000000000000000000000000000000000",
        "00000000000000ffffffff2a03d12c0c00043855975e464b8896790758f824ceac97836",
        "22c17ed38f1669b8a45ce1da857dbbe7950e2ffffffff02a0ebce1d000000001976a914",
        "7ed15946ec14ae0cd8fa8991eb6084452eb3f77c88ac405973070000000017a914e445cf",
        "a944b6f2bdacefbda904a81d5fdd26d77f8700000000000000000000000000000000000000",
    );

    let merkle_root = Sha256::digest(format!("{nonce}#{height}").as_bytes());
    let mut header = Vec::with_capacity(SYNTHETIC_HEADER_LEN);
    header.extend_from_slice(&4u32.to_le_bytes()); // version
    header.extend_from_slice(&[0u8; 32]); // prevHash
    header.extend_from_slice(&merkle_root); // merkleRoot
    header.extend_from_slice(&[0u8; 32]); // hashFinalSaplingRoot
    header.extend_from_slice(&1u32.to_le_bytes()); // time
    header.extend_from_slice(&[0u8; 4]); // nBits
    header.extend_from_slice(&[0u8; 32]); // nonce
    compact::write_compact_size(&mut header, 1344); // solution length
    header.extend_from_slice(&[0u8; 1344]); // solution

    let height_le = hex::encode((height as u32).to_le_bytes());
    let coinbase = hex::decode(FAKE_COINBASE.replace("d12c0c00", &height_le))?;

    let mut block = header;
    block.push(1); // transaction count
    block.extend_from_slice(&coinbase);
    Ok(block)
}
