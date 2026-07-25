//! The snapshot wire format: the manifest, the epoch body framing, and the two digests.
//!
//! An epoch is a fixed span of [`EPOCH_SIZE`] heights aligned to a multiple of that size, so any two
//! servers cut epochs at the same boundaries and the body for a given `(chain, start, count)` is
//! byte-identical wherever it came from.
//!
//! Each epoch carries two digests. [`content_digest`] covers the body bytes and proves only that the
//! transfer arrived intact. The anchor ([`AnchorHasher`]) covers `(height, block hash)` pairs alone,
//! so an importer recomputes it from its own node: it is what ties a snapshot to the real chain
//! rather than to the goodwill of whoever served it. It is deliberately independent of the framing,
//! so a later format version can change the body encoding without invalidating published anchors.

use std::io::Write;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::SnapshotError;

/// Heights covered by one epoch. Matches the maximum span a `GetBlockRange` request may ask for, so
/// an epoch is the largest range this codebase already treats as one unit of work.
pub const EPOCH_SIZE: u64 = 10_000;

/// Version of both the manifest and the epoch framing.
pub const FORMAT_VERSION: u8 = 1;

/// First bytes of every epoch body. The last byte is [`FORMAT_VERSION`].
pub const MAGIC: [u8; 8] = *b"LWDSNAP\x01";

/// The epoch `height` belongs to.
pub fn epoch_index(height: u64) -> u64 {
    height / EPOCH_SIZE
}

/// The first height `index` covers, ignoring where a cache actually starts.
pub fn epoch_first_height(index: u64) -> u64 {
    index.saturating_mul(EPOCH_SIZE)
}

/// The last height `index` covers.
pub fn epoch_last_height(index: u64) -> u64 {
    epoch_first_height(index).saturating_add(EPOCH_SIZE - 1)
}

/// What a server publishes about the range it can serve.
///
/// `base_height` and `tip_height` describe the publisher's cache and are advisory; every
/// [`EpochEntry`] states its own bounds, so a consumer never has to infer them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// Format version of this manifest and of the epoch bodies it describes.
    pub format_version: u8,
    /// Chain name as the publisher's node reports it (`main`, `test`, ...).
    pub chain: String,
    /// Heights per epoch, always [`EPOCH_SIZE`] for [`FORMAT_VERSION`] 1.
    pub epoch_size: u64,
    /// Lowest height the publisher holds.
    pub base_height: u64,
    /// Highest height the publisher holds, which may sit above the last published epoch.
    pub tip_height: u64,
    /// The epochs available for download, ascending. Only completed epochs are published, so this
    /// is a growing prefix of the publisher's range rather than all of it.
    pub epochs: Vec<EpochEntry>,
}

/// One downloadable epoch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpochEntry {
    /// Epoch number, i.e. `start / epoch_size`.
    pub index: u64,
    /// First height in the body. Above `index * epoch_size` when the publisher's cache starts
    /// mid-epoch.
    pub start: u64,
    /// Last height in the body.
    pub end: u64,
    /// Length of the uncompressed body in bytes.
    pub bytes: u64,
    /// SHA-256 of the uncompressed body, hex-encoded.
    pub content_digest: String,
    /// SHA-256 over the epoch's `(height, block hash)` pairs, hex-encoded.
    pub anchor: String,
}

/// A completed epoch's digests as persisted in the cache's `meta` table, and everything the manifest
/// needs about it. Fixed-width so a stored row decodes without a parser.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochDigest {
    /// First height covered.
    pub start: u64,
    /// Last height covered.
    pub end: u64,
    /// Length of the uncompressed body in bytes.
    pub bytes: u64,
    /// SHA-256 of the uncompressed body.
    pub content: [u8; 32],
    /// SHA-256 over the epoch's `(height, block hash)` pairs.
    pub anchor: [u8; 32],
}

impl EpochDigest {
    /// Length of the encoded form: three `u64` fields followed by the two digests.
    pub const ENCODED_LEN: usize = 8 * 3 + 32 * 2;

    /// Encode for storage.
    pub fn encode(&self) -> [u8; Self::ENCODED_LEN] {
        let mut out = [0u8; Self::ENCODED_LEN];
        out[0..8].copy_from_slice(&self.start.to_le_bytes());
        out[8..16].copy_from_slice(&self.end.to_le_bytes());
        out[16..24].copy_from_slice(&self.bytes.to_le_bytes());
        out[24..56].copy_from_slice(&self.content);
        out[56..88].copy_from_slice(&self.anchor);
        out
    }

    /// Decode a stored row, or `None` if it is not exactly [`Self::ENCODED_LEN`] bytes.
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let bytes: &[u8; Self::ENCODED_LEN] = bytes.try_into().ok()?;
        Some(Self {
            start: u64::from_le_bytes(bytes[0..8].try_into().ok()?),
            end: u64::from_le_bytes(bytes[8..16].try_into().ok()?),
            bytes: u64::from_le_bytes(bytes[16..24].try_into().ok()?),
            content: bytes[24..56].try_into().ok()?,
            anchor: bytes[56..88].try_into().ok()?,
        })
    }

    /// The manifest entry for this epoch.
    pub fn entry(&self, index: u64) -> EpochEntry {
        EpochEntry {
            index,
            start: self.start,
            end: self.end,
            bytes: self.bytes,
            content_digest: hex::encode(self.content),
            anchor: hex::encode(self.anchor),
        }
    }
}

/// Write an epoch body header: magic, chain, first height, and how many records follow.
pub fn write_header(
    out: &mut impl Write,
    chain: &str,
    start: u64,
    count: u32,
) -> Result<(), SnapshotError> {
    if chain.is_empty() || !chain.is_ascii() || chain.len() > u8::MAX as usize {
        return Err(SnapshotError::InvalidChain(chain.to_string()));
    }
    out.write_all(&MAGIC)?;
    out.write_all(&[chain.len() as u8])?;
    out.write_all(chain.as_bytes())?;
    out.write_all(&start.to_le_bytes())?;
    out.write_all(&count.to_le_bytes())?;
    Ok(())
}

/// Write one length-prefixed record. Heights are implicit and consecutive from the header's `start`.
pub fn write_record(out: &mut impl Write, block: &[u8]) -> Result<(), SnapshotError> {
    let length = u32::try_from(block.len())
        .map_err(|_| SnapshotError::Malformed(format!("block of {} bytes", block.len())))?;
    out.write_all(&length.to_le_bytes())?;
    out.write_all(block)?;
    Ok(())
}

/// The decoded header of an epoch body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EpochHeader {
    /// Chain the publisher's blocks belong to.
    pub chain: String,
    /// Height of the first record.
    pub start: u64,
    /// Number of records that follow.
    pub count: u32,
}

/// Split an epoch body into its header and its record payloads, in order.
///
/// Borrows from `body` rather than copying: a caller that only needs to verify digests and decode
/// blocks never pays a second allocation per block.
pub fn parse_epoch(body: &[u8]) -> Result<(EpochHeader, Vec<&[u8]>), SnapshotError> {
    let mut cursor = Cursor { body, offset: 0 };
    let magic = cursor.take(MAGIC.len())?;
    if magic != MAGIC {
        return Err(SnapshotError::Malformed(format!(
            "bad magic {:?}, expected a version {FORMAT_VERSION} epoch body",
            hex::encode(magic)
        )));
    }
    let chain_length = cursor.take(1)?[0] as usize;
    let chain = std::str::from_utf8(cursor.take(chain_length)?)
        .map_err(|error| SnapshotError::Malformed(format!("chain name is not UTF-8: {error}")))?
        .to_string();
    let start = u64::from_le_bytes(
        cursor
            .take(8)?
            .try_into()
            .map_err(|_| SnapshotError::Malformed("truncated start height".to_string()))?,
    );
    let count = u32::from_le_bytes(
        cursor
            .take(4)?
            .try_into()
            .map_err(|_| SnapshotError::Malformed("truncated record count".to_string()))?,
    );

    let mut records = Vec::new();
    for _ in 0..count {
        let length = u32::from_le_bytes(
            cursor
                .take(4)?
                .try_into()
                .map_err(|_| SnapshotError::Malformed("truncated record length".to_string()))?,
        );
        records.push(cursor.take(length as usize)?);
    }
    if cursor.offset != body.len() {
        return Err(SnapshotError::Malformed(format!(
            "{} trailing bytes after {count} records",
            body.len() - cursor.offset
        )));
    }
    Ok((
        EpochHeader {
            chain,
            start,
            count,
        },
        records,
    ))
}

/// Bounds-checked forward reader over an epoch body.
struct Cursor<'a> {
    body: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8], SnapshotError> {
        let end = self
            .offset
            .checked_add(length)
            .filter(|e| *e <= self.body.len());
        let Some(end) = end else {
            return Err(SnapshotError::Malformed(format!(
                "body ends after {} bytes, need {length} more at offset {}",
                self.body.len(),
                self.offset
            )));
        };
        let taken = &self.body[self.offset..end];
        self.offset = end;
        Ok(taken)
    }
}

/// SHA-256 of an uncompressed epoch body.
pub fn content_digest(body: &[u8]) -> [u8; 32] {
    Sha256::digest(body).into()
}

/// A [`Write`] sink that hashes and counts instead of storing, so an epoch's `content_digest` is
/// always taken over exactly the bytes an export would send.
#[derive(Default)]
pub struct DigestWriter {
    hasher: Sha256,
    bytes: u64,
}

impl DigestWriter {
    /// The digest of everything written, and how many bytes that was.
    pub fn finish(self) -> ([u8; 32], u64) {
        (self.hasher.finalize().into(), self.bytes)
    }
}

impl Write for DigestWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.hasher.update(buf);
        self.bytes += buf.len() as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Incremental anchor computation: `SHA-256( for each height ascending: height_be || block_hash )`.
///
/// Shared by the export (hashes read out of cached blocks) and by import verification (hashes read
/// from the node), so the two can never drift apart.
#[derive(Default)]
pub struct AnchorHasher {
    hasher: Sha256,
}

impl AnchorHasher {
    /// Fold in one height and its block hash, in wire (not display) byte order.
    pub fn update(&mut self, height: u64, block_hash: &[u8]) {
        self.hasher.update(height.to_be_bytes());
        self.hasher.update(block_hash);
    }

    /// The anchor over everything folded in so far.
    pub fn finish(self) -> [u8; 32] {
        self.hasher.finalize().into()
    }
}

/// The anchor over `entries`, which must be ascending by height.
pub fn anchor_digest<'a>(entries: impl IntoIterator<Item = (u64, &'a [u8])>) -> [u8; 32] {
    let mut hasher = AnchorHasher::default();
    for (height, block_hash) in entries {
        hasher.update(height, block_hash);
    }
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(chain: &str, start: u64, blocks: &[&[u8]]) -> Vec<u8> {
        let mut out = Vec::new();
        write_header(&mut out, chain, start, blocks.len() as u32).unwrap();
        for block in blocks {
            write_record(&mut out, block).unwrap();
        }
        out
    }

    #[test]
    fn magic_carries_the_format_version_in_its_last_byte() {
        assert_eq!(MAGIC[7], FORMAT_VERSION);
    }

    #[test]
    fn epoch_bounds_follow_the_fixed_alignment() {
        assert_eq!(epoch_index(419_200), 41);
        assert_eq!(epoch_first_height(41), 410_000);
        assert_eq!(epoch_last_height(41), 419_999);
    }

    #[test]
    fn epoch_bounds_saturate_instead_of_overflowing() {
        assert_eq!(epoch_last_height(u64::MAX), u64::MAX);
    }

    #[test]
    fn parse_epoch_roundtrips_a_written_body() {
        let blocks: Vec<&[u8]> = vec![b"first", b"", b"third"];
        let encoded = body("main", 410_000, &blocks);

        let (header, records) = parse_epoch(&encoded).unwrap();

        assert_eq!(
            (header, records),
            (
                EpochHeader {
                    chain: "main".to_string(),
                    start: 410_000,
                    count: 3
                },
                blocks
            )
        );
    }

    #[test]
    fn parse_epoch_rejects_a_foreign_magic() {
        let mut encoded = body("main", 0, &[b"block"]);
        encoded[0] = b'X';
        assert!(matches!(
            parse_epoch(&encoded),
            Err(SnapshotError::Malformed(_))
        ));
    }

    #[test]
    fn parse_epoch_rejects_a_truncated_body() {
        let encoded = body("main", 0, &[b"block"]);
        assert!(matches!(
            parse_epoch(&encoded[..encoded.len() - 1]),
            Err(SnapshotError::Malformed(_))
        ));
    }

    #[test]
    fn parse_epoch_rejects_trailing_bytes() {
        let mut encoded = body("main", 0, &[b"block"]);
        encoded.push(0);
        assert!(matches!(
            parse_epoch(&encoded),
            Err(SnapshotError::Malformed(_))
        ));
    }

    #[test]
    fn parse_epoch_rejects_a_record_length_past_the_end() {
        let mut encoded = body("main", 0, &[b"block"]);
        let length_offset = encoded.len() - b"block".len() - 4;
        encoded[length_offset..length_offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            parse_epoch(&encoded),
            Err(SnapshotError::Malformed(_))
        ));
    }

    #[test]
    fn write_header_rejects_a_non_ascii_chain_name() {
        let mut out = Vec::new();
        assert!(matches!(
            write_header(&mut out, "maiñ", 0, 0),
            Err(SnapshotError::InvalidChain(_))
        ));
    }

    #[test]
    fn digest_writer_matches_hashing_the_same_bytes() {
        let encoded = body("main", 7, &[b"a", b"bb"]);
        let mut writer = DigestWriter::default();
        writer.write_all(&encoded).unwrap();

        assert_eq!(
            writer.finish(),
            (content_digest(&encoded), encoded.len() as u64)
        );
    }

    #[test]
    fn anchor_digest_matches_a_hand_computed_value() {
        // sha256( 0000000000000001 || 01*32 || 0000000000000002 || 02*32 ), computed independently.
        let hashes = [[1u8; 32], [2u8; 32]];
        assert_eq!(
            hex::encode(anchor_digest([
                (1u64, hashes[0].as_slice()),
                (2u64, hashes[1].as_slice())
            ])),
            "ec797f480d0ab3fcfe4465c0ac944bd9da7d84523aa1a59aad1836f75c5a1abe"
        );
    }

    #[test]
    fn anchor_digest_depends_on_the_height_a_hash_sits_at() {
        let hash = [7u8; 32];
        assert_ne!(
            anchor_digest([(1u64, hash.as_slice())]),
            anchor_digest([(2u64, hash.as_slice())])
        );
    }

    #[test]
    fn epoch_digest_roundtrips_through_its_stored_encoding() {
        let digest = EpochDigest {
            start: 410_000,
            end: 419_999,
            bytes: 1234,
            content: [0xaa; 32],
            anchor: [0xbb; 32],
        };
        assert_eq!(EpochDigest::decode(&digest.encode()), Some(digest));
    }

    #[test]
    fn epoch_digest_rejects_a_row_of_the_wrong_length() {
        assert_eq!(EpochDigest::decode(&[0u8; 87]), None);
    }

    #[test]
    fn manifest_serializes_with_the_documented_field_names() {
        let manifest = Manifest {
            format_version: FORMAT_VERSION,
            chain: "main".to_string(),
            epoch_size: EPOCH_SIZE,
            base_height: 419_200,
            tip_height: 3_424_773,
            epochs: vec![
                EpochDigest {
                    start: 419_200,
                    end: 419_999,
                    bytes: 1234,
                    content: [0x11; 32],
                    anchor: [0x22; 32],
                }
                .entry(41),
            ],
        };

        let json = serde_json::to_value(&manifest).unwrap();

        assert_eq!(json["epochs"][0]["content_digest"], "11".repeat(32));
        assert_eq!(serde_json::from_value::<Manifest>(json).unwrap(), manifest);
    }
}
