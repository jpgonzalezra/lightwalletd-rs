//! On-disk store of compact blocks, keyed by height, backed by `redb`.
//!
//! Each block is stored as its protobuf encoding under its height. The store is ordered, so the lowest
//! and highest cached heights are cheap to read, and a reorg is just "drop everything above height N".

use std::path::Path;

use prost::Message;
use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};

use crate::proto::CompactBlock;

/// Height → protobuf-encoded `CompactBlock`.
const BLOCKS: TableDefinition<u64, &[u8]> = TableDefinition::new("compact_blocks");

/// Errors from the block cache.
#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    /// Opening or creating the database failed.
    #[error(transparent)]
    Database(#[from] redb::DatabaseError),
    /// Beginning a transaction failed.
    #[error(transparent)]
    Transaction(#[from] redb::TransactionError),
    /// Opening a table failed.
    #[error(transparent)]
    Table(#[from] redb::TableError),
    /// A read or write within a transaction failed.
    #[error(transparent)]
    Storage(#[from] redb::StorageError),
    /// Committing a transaction failed.
    #[error(transparent)]
    Commit(#[from] redb::CommitError),
    /// A stored block could not be decoded.
    #[error("decoding cached block: {0}")]
    Decode(#[from] prost::DecodeError),
    /// The cache holds a logically inconsistent entry (wrong key, non-monotonic, or a gap).
    #[error("cache corruption at height {height}: {detail}")]
    Corruption {
        /// The height at or around which the inconsistency was detected.
        height: u64,
        /// Human-readable description of the inconsistency.
        detail: String,
    },
}

/// A `redb`-backed store of compact blocks.
pub struct Cache {
    db: Database,
}

impl Cache {
    /// Open (creating if needed) the cache at `path`.
    pub fn open(path: &Path) -> Result<Self, CacheError> {
        let db = Database::create(path)?;
        // Materialize the table so reads against an otherwise-empty cache succeed.
        let txn = db.begin_write()?;
        txn.open_table(BLOCKS)?;
        txn.commit()?;
        Ok(Self { db })
    }

    /// Store the compact block at `height`, appending onto the cache tip.
    ///
    /// Rejects logically inconsistent writes (the block's own height not matching the key, or a
    /// non-monotonic append) with [`CacheError::Corruption`] rather than silently storing them.
    pub fn add(&self, height: u64, block: &CompactBlock) -> Result<(), CacheError> {
        if block.height != height {
            return Err(CacheError::Corruption {
                height,
                detail: format!("block.height {} does not match key {height}", block.height),
            });
        }
        let bytes = block.encode_to_vec();
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(BLOCKS)?;
            if let Some((tip, _)) = table.last()? {
                let tip = tip.value();
                if height != tip + 1 {
                    return Err(CacheError::Corruption {
                        height,
                        detail: format!("non-monotonic append: tip is {tip}, got {height}"),
                    });
                }
            }
            table.insert(height, bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Return the compact block at `height`, if cached.
    pub fn get(&self, height: u64) -> Result<Option<CompactBlock>, CacheError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(BLOCKS)?;
        match table.get(height)? {
            Some(guard) => Ok(Some(CompactBlock::decode(guard.value())?)),
            None => Ok(None),
        }
    }

    /// The highest cached height, or `None` if the cache is empty.
    pub fn latest_height(&self) -> Result<Option<u64>, CacheError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(BLOCKS)?;
        Ok(table.last()?.map(|(height, _)| height.value()))
    }

    /// The hash of the highest cached block, used by the ingestor to detect reorgs.
    pub fn latest_hash(&self) -> Result<Option<Vec<u8>>, CacheError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(BLOCKS)?;
        match table.last()? {
            Some((_, value)) => Ok(Some(CompactBlock::decode(value.value())?.hash)),
            None => Ok(None),
        }
    }

    /// Drop every block above `height` (keeping `height` itself). Used to roll back a reorg.
    pub fn reorg(&self, height: u64) -> Result<(), CacheError> {
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(BLOCKS)?;
            table.retain(|cached, _| cached <= height)?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Drop every block at or above `height`, so re-ingestion refills from `height`. Backs the
    /// `--sync-from-height`/`--redownload` operator levers; `truncate_from(0)` empties the cache.
    pub fn truncate_from(&self, height: u64) -> Result<(), CacheError> {
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(BLOCKS)?;
            table.retain(|cached, _| cached < height)?;
        }
        txn.commit()?;
        Ok(())
    }

    /// A cheap open-time consistency check. On a non-empty cache it decodes the tip and verifies the
    /// height range has no gaps. O(log n) — it touches only the first and last entries, so the happy
    /// path stays scan-free. A detected symptom is localized and truncated by [`Self::reorg`].
    pub fn validate_light(&self) -> Result<(), CacheError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(BLOCKS)?;
        let len = table.len()?;
        if len == 0 {
            return Ok(());
        }

        let first = table
            .first()?
            .map(|(height, _)| height.value())
            .ok_or_else(|| CacheError::Corruption {
                height: 0,
                detail: "non-empty cache has no first entry".to_string(),
            })?;
        let (last_height, last_value) = table.last()?.ok_or_else(|| CacheError::Corruption {
            height: 0,
            detail: "non-empty cache has no last entry".to_string(),
        })?;
        let last = last_height.value();

        // The tip must decode and its own height must match its key.
        let tip =
            CompactBlock::decode(last_value.value()).map_err(|error| CacheError::Corruption {
                height: last,
                detail: format!("tip block failed to decode: {error}"),
            })?;
        if tip.height != last {
            return Err(CacheError::Corruption {
                height: last,
                detail: format!("tip block.height {} does not match key {last}", tip.height),
            });
        }

        // A contiguous range [first, last] holds exactly `last - first + 1` entries.
        let expected = last - first + 1;
        if len != expected {
            return Err(CacheError::Corruption {
                height: last,
                detail: format!("gap detected: {len} entries span [{first}, {last}]"),
            });
        }
        Ok(())
    }

    /// Locate the lowest corrupt height, to be called only after [`Self::validate_light`] (or a read)
    /// reports a symptom. Returns `None` if the cache is in fact consistent. The caller truncates with
    /// `reorg(corrupt.saturating_sub(1))`, dropping the corruption so re-ingestion refills it.
    ///
    /// Realistic corruption in this transactional, strict-append store is a contiguous suffix (an
    /// interrupted final write) or a schema-wide decode failure visible at the tip — not an isolated
    /// mid-cache block. Localization matches that: a gap is binary-searched (the valid prefix is
    /// contiguous), a decode/height symptom is found by walking down from the tip. An isolated
    /// mid-cache corruption (which redb's page checksums and transactionality make practically
    /// impossible) is out of scope.
    pub fn lowest_corrupt_height(&self) -> Result<Option<u64>, CacheError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(BLOCKS)?;
        let len = table.len()?;
        if len == 0 {
            return Ok(None);
        }
        let (Some((first_height, _)), Some((last_height, _))) = (table.first()?, table.last()?)
        else {
            return Ok(None);
        };
        let first = first_height.value();
        let last = last_height.value();

        // Gap symptom: the valid prefix is contiguous, so binary-search the lowest missing height.
        if len != last - first + 1 {
            // Invariant: `present` is a present height, a gap exists in `(present, missing]`.
            let mut present = first;
            let mut missing = last;
            while present + 1 < missing {
                let mid = present + (missing - present) / 2;
                if table.get(mid)?.is_some() {
                    present = mid;
                } else {
                    missing = mid;
                }
            }
            return Ok(Some(missing));
        }

        // Decode/height symptom: walk down from the tip until a block decodes with a matching height;
        // the corrupt suffix is everything above it.
        let mut height = last;
        loop {
            let good = match table.get(height)? {
                Some(value) => {
                    CompactBlock::decode(value.value()).is_ok_and(|block| block.height == height)
                }
                None => false,
            };
            if good {
                return Ok((height < last).then_some(height + 1));
            }
            if height == first {
                return Ok(Some(first)); // even the lowest block is corrupt
            }
            height -= 1;
        }
    }

    /// Insert a raw value at `height`, bypassing the [`Self::add`] guards. Test-only: builds the
    /// corrupt or gapped fixtures the guards would otherwise reject.
    #[cfg(test)]
    pub(crate) fn insert_raw(&self, height: u64, bytes: &[u8]) -> Result<(), CacheError> {
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(BLOCKS)?;
            table.insert(height, bytes)?;
        }
        txn.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::temp_cache;

    fn block(height: u64, hash_byte: u8) -> CompactBlock {
        CompactBlock {
            height,
            hash: vec![hash_byte; 32],
            ..Default::default()
        }
    }

    #[test]
    fn add_then_get_roundtrips_the_block() {
        let (_dir, cache) = temp_cache();
        cache.add(100, &block(100, 0xaa)).unwrap();
        assert_eq!(cache.get(100).unwrap(), Some(block(100, 0xaa)));
    }

    #[test]
    fn get_returns_none_for_absent_height() {
        let (_dir, cache) = temp_cache();
        assert_eq!(cache.get(42).unwrap(), None);
    }

    #[test]
    fn latest_height_tracks_the_highest_block() {
        let (_dir, cache) = temp_cache();
        cache.add(100, &block(100, 1)).unwrap();
        cache.add(101, &block(101, 2)).unwrap();
        assert_eq!(cache.latest_height().unwrap(), Some(101));
    }

    #[test]
    fn reorg_drops_blocks_above_the_given_height() {
        let (_dir, cache) = temp_cache();
        for height in 100..=105 {
            cache.add(height, &block(height, height as u8)).unwrap();
        }
        cache.reorg(102).unwrap();
        assert_eq!(cache.latest_height().unwrap(), Some(102));
        assert_eq!(cache.get(103).unwrap(), None);
    }

    #[test]
    fn truncate_from_drops_blocks_at_or_above_the_given_height() {
        let (_dir, cache) = temp_cache();
        for height in 100..=105 {
            cache.add(height, &block(height, height as u8)).unwrap();
        }
        cache.truncate_from(103).unwrap();
        assert_eq!(cache.latest_height().unwrap(), Some(102));
        assert_eq!(cache.get(103).unwrap(), None);
    }

    #[test]
    fn truncate_from_zero_empties_the_cache() {
        let (_dir, cache) = temp_cache();
        for height in 100..=105 {
            cache.add(height, &block(height, height as u8)).unwrap();
        }
        cache.truncate_from(0).unwrap();
        assert_eq!(cache.latest_height().unwrap(), None);
    }

    #[test]
    fn add_rejects_a_block_whose_height_field_does_not_match_the_key() {
        let (_dir, cache) = temp_cache();
        let result = cache.add(100, &block(101, 0xaa));
        assert!(matches!(
            result,
            Err(CacheError::Corruption { height: 100, .. })
        ));
    }

    #[test]
    fn add_rejects_a_non_monotonic_height() {
        let (_dir, cache) = temp_cache();
        cache.add(100, &block(100, 1)).unwrap();
        let result = cache.add(102, &block(102, 2));
        assert!(matches!(
            result,
            Err(CacheError::Corruption { height: 102, .. })
        ));
    }

    #[test]
    fn validate_light_accepts_an_empty_cache() {
        let (_dir, cache) = temp_cache();
        assert!(cache.validate_light().is_ok());
    }

    #[test]
    fn validate_light_accepts_a_contiguous_cache() {
        let (_dir, cache) = temp_cache();
        for height in 100..=105 {
            cache.add(height, &block(height, height as u8)).unwrap();
        }
        assert!(cache.validate_light().is_ok());
    }

    #[test]
    fn validate_light_detects_a_gap() {
        let (_dir, cache) = temp_cache();
        cache
            .insert_raw(100, &block(100, 1).encode_to_vec())
            .unwrap();
        cache
            .insert_raw(102, &block(102, 3).encode_to_vec())
            .unwrap();
        assert!(matches!(
            cache.validate_light(),
            Err(CacheError::Corruption { .. })
        ));
    }

    #[test]
    fn lowest_corrupt_height_locates_a_corrupt_suffix_by_descending() {
        let (_dir, cache) = temp_cache();
        for height in 100..=102 {
            cache.add(height, &block(height, height as u8)).unwrap();
        }
        // An undecodable suffix of length 2 above the last good block (102).
        cache.insert_raw(103, &[0x08, 0xff]).unwrap();
        cache.insert_raw(104, &[0x08, 0xff]).unwrap();

        assert_eq!(cache.lowest_corrupt_height().unwrap(), Some(103));

        // Truncating from the located height leaves a consistent cache re-ingestion can extend.
        cache.reorg(103u64.saturating_sub(1)).unwrap();
        assert_eq!(cache.latest_height().unwrap(), Some(102));
        assert!(cache.validate_light().is_ok());
    }

    #[test]
    fn lowest_corrupt_height_locates_a_gap_by_binary_search() {
        let (_dir, cache) = temp_cache();
        cache
            .insert_raw(100, &block(100, 1).encode_to_vec())
            .unwrap();
        cache
            .insert_raw(102, &block(102, 3).encode_to_vec())
            .unwrap();

        assert_eq!(cache.lowest_corrupt_height().unwrap(), Some(101));
    }

    #[test]
    fn lowest_corrupt_height_returns_none_for_a_consistent_cache() {
        let (_dir, cache) = temp_cache();
        for height in 100..=105 {
            cache.add(height, &block(height, height as u8)).unwrap();
        }
        assert_eq!(cache.lowest_corrupt_height().unwrap(), None);
    }
}
