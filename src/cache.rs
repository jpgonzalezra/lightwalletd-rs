//! On-disk store of compact blocks, keyed by height, backed by `redb`.
//!
//! Each block is stored as its protobuf encoding under its height. The store is ordered, so the lowest
//! and highest cached heights are cheap to read, and a reorg is just "drop everything above height N".

use std::ops::RangeInclusive;
use std::path::Path;

use prost::Message;
use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};

use crate::proto::CompactBlock;
use crate::snapshot::format::epoch_index;

/// Height → protobuf-encoded `CompactBlock`.
const BLOCKS: TableDefinition<u64, &[u8]> = TableDefinition::new("compact_blocks");

/// Key → opaque value, for the small amount of state that describes the blocks rather than being
/// one. Created empty on first open, so a cache written by an earlier version needs no migration.
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");

/// Prefix of the [`META`] rows holding a snapshot epoch's digests. The index is zero-padded to the
/// width of `u64::MAX` so lexicographic key order matches numeric epoch order.
const EPOCH_DIGEST_PREFIX: &str = "epoch_digest/";

/// The [`META`] key holding epoch `index`'s digests.
fn epoch_digest_key(index: u64) -> String {
    format!("{EPOCH_DIGEST_PREFIX}{index:020}")
}

/// The epoch a [`META`] key describes, or `None` if the key is not an epoch digest row.
fn epoch_digest_index(key: &str) -> Option<u64> {
    key.strip_prefix(EPOCH_DIGEST_PREFIX)?.parse().ok()
}

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
        // Materialize the tables so reads against an otherwise-empty cache succeed.
        let txn = db.begin_write()?;
        txn.open_table(BLOCKS)?;
        txn.open_table(META)?;
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
        self.add_batch(std::slice::from_ref(block))
    }

    /// Store a run of consecutive compact blocks in a single transaction, appending onto the cache
    /// tip. One commit — and thus one fsync — covers the whole batch, which is what makes windowed
    /// catch-up cheap. The [`Self::add`] guards apply to the batch as a whole: the first block must
    /// extend the current tip by exactly one, and the heights must be consecutive. An empty batch
    /// is a no-op.
    pub fn add_batch(&self, blocks: &[CompactBlock]) -> Result<(), CacheError> {
        let Some(first) = blocks.first() else {
            return Ok(());
        };
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(BLOCKS)?;
            if let Some((tip, _)) = table.last()? {
                let tip = tip.value();
                if first.height != tip + 1 {
                    return Err(CacheError::Corruption {
                        height: first.height,
                        detail: format!("non-monotonic append: tip is {tip}, got {}", first.height),
                    });
                }
            }
            for (expected, block) in (first.height..).zip(blocks) {
                if block.height != expected {
                    return Err(CacheError::Corruption {
                        height: expected,
                        detail: format!(
                            "batch is not consecutive: expected {expected}, got {}",
                            block.height
                        ),
                    });
                }
                table.insert(block.height, block.encode_to_vec().as_slice())?;
            }
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

    /// Visit the raw stored value for each cached height in `range`, ascending, under a single read
    /// transaction. `redb` reads are MVCC, so the view stays coherent for the whole walk even while
    /// the ingestor appends.
    ///
    /// Hands out the stored bytes rather than a decoded block, so an export that only moves blocks
    /// around pays no decode/encode round trip. The visitor's error type is free (as long as it can
    /// carry a [`CacheError`]) so a caller need not launder its own errors through this one.
    pub fn for_each_raw<E>(
        &self,
        range: RangeInclusive<u64>,
        mut visit: impl FnMut(u64, &[u8]) -> Result<(), E>,
    ) -> Result<(), E>
    where
        E: From<CacheError>,
    {
        let txn = self.db.begin_read().map_err(CacheError::from)?;
        let table = txn.open_table(BLOCKS).map_err(CacheError::from)?;
        for entry in table.range(range).map_err(CacheError::from)? {
            let (height, value) = entry.map_err(CacheError::from)?;
            visit(height.value(), value.value())?;
        }
        Ok(())
    }

    /// The lowest and highest cached heights, read together, or `None` if the cache is empty.
    pub fn range(&self) -> Result<Option<(u64, u64)>, CacheError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(BLOCKS)?;
        match (table.first()?, table.last()?) {
            (Some((first, _)), Some((last, _))) => Ok(Some((first.value(), last.value()))),
            _ => Ok(None),
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
        self.remove_from(height.saturating_add(1))
    }

    /// Drop every block at or above `height`, so re-ingestion refills from `height`. Backs the
    /// `--sync-from-height`/`--redownload` operator levers; `truncate_from(0)` empties the cache.
    pub fn truncate_from(&self, height: u64) -> Result<(), CacheError> {
        self.remove_from(height)
    }

    /// Drop every block at or above `height`, together with the snapshot epoch digests describing
    /// any epoch that reaches into the dropped range, in one transaction. Metadata must never
    /// outlive the blocks it describes: a digest surviving its epoch would keep advertising a range
    /// the cache no longer holds.
    fn remove_from(&self, height: u64) -> Result<(), CacheError> {
        let dropped_epoch = epoch_index(height);
        let txn = self.db.begin_write()?;
        {
            let mut blocks = txn.open_table(BLOCKS)?;
            blocks.retain(|cached, _| cached < height)?;
            let mut meta = txn.open_table(META)?;
            meta.retain(|key, _| {
                epoch_digest_index(key).is_none_or(|index| index < dropped_epoch)
            })?;
        }
        txn.commit()?;
        Ok(())
    }

    /// The stored digest row for snapshot epoch `index`, if it has one. The value is opaque here:
    /// the snapshot module owns its encoding.
    pub fn epoch_digest(&self, index: u64) -> Result<Option<Vec<u8>>, CacheError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(META)?;
        Ok(table
            .get(epoch_digest_key(index).as_str())?
            .map(|value| value.value().to_vec()))
    }

    /// Store the digest row for snapshot epoch `index`.
    pub fn set_epoch_digest(&self, index: u64, value: &[u8]) -> Result<(), CacheError> {
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(META)?;
            table.insert(epoch_digest_key(index).as_str(), value)?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Every stored epoch digest row, ascending by epoch index, read in one transaction.
    pub fn epoch_digests(&self) -> Result<Vec<(u64, Vec<u8>)>, CacheError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(META)?;
        let mut rows = Vec::new();
        for entry in table.iter()? {
            let (key, value) = entry?;
            if let Some(index) = epoch_digest_index(key.value()) {
                rows.push((index, value.value().to_vec()));
            }
        }
        Ok(rows)
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
    /// mid-cache block. Localization matches that: a gap is found by scanning up from the lowest
    /// cached height, a decode/height symptom by walking down from the tip. An isolated mid-cache
    /// corruption (which redb's page checksums and transactionality make practically impossible) is
    /// out of scope.
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

        // Gap symptom: present blocks can sit above the hole (the last entry always does), so a
        // present→missing boundary search would land on a present height. Scan up from `first` for
        // the lowest missing height instead — O(n) is fine on this rare, cold recovery path.
        if len != last - first + 1 {
            for height in first..=last {
                if table.get(height)?.is_none() {
                    return Ok(Some(height));
                }
            }
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

    /// A stand-in digest row. The cache treats the value as opaque, so its shape does not matter
    /// here; only that it survives or is dropped with the blocks it describes.
    fn digest_row(marker: u8) -> Vec<u8> {
        vec![marker; 88]
    }

    /// The epochs that currently have a stored digest.
    fn stored_epochs(cache: &Cache) -> Vec<u64> {
        cache
            .epoch_digests()
            .unwrap()
            .into_iter()
            .map(|(index, _)| index)
            .collect()
    }

    #[test]
    fn for_each_raw_visits_the_requested_range_ascending() {
        let (_dir, cache) = temp_cache();
        for height in 100..=105 {
            cache.add(height, &block(height, height as u8)).unwrap();
        }

        let mut visited = Vec::new();
        cache
            .for_each_raw(102..=104, |height, raw| -> Result<(), CacheError> {
                visited.push((height, CompactBlock::decode(raw)?));
                Ok(())
            })
            .unwrap();

        assert_eq!(
            visited,
            (102..=104)
                .map(|height| (height, block(height, height as u8)))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn for_each_raw_propagates_the_visitors_error() {
        let (_dir, cache) = temp_cache();
        cache.add(100, &block(100, 1)).unwrap();

        let result = cache.for_each_raw(100..=100, |_, _| {
            Err(CacheError::Corruption {
                height: 100,
                detail: "from the visitor".to_string(),
            })
        });

        assert!(matches!(result, Err(CacheError::Corruption { .. })));
    }

    #[test]
    fn range_reports_the_cached_bounds() {
        let (_dir, cache) = temp_cache();
        assert_eq!(cache.range().unwrap(), None);
        for height in 100..=105 {
            cache.add(height, &block(height, height as u8)).unwrap();
        }
        assert_eq!(cache.range().unwrap(), Some((100, 105)));
    }

    #[test]
    fn epoch_digests_are_ordered_numerically_not_lexicographically() {
        let (_dir, cache) = temp_cache();
        for index in [10u64, 2, 9] {
            cache
                .set_epoch_digest(index, &digest_row(index as u8))
                .unwrap();
        }

        assert_eq!(stored_epochs(&cache), vec![2, 9, 10]);
    }

    #[test]
    fn epoch_digest_roundtrips_a_stored_row() {
        let (_dir, cache) = temp_cache();
        cache.set_epoch_digest(41, &digest_row(0xaa)).unwrap();

        assert_eq!(cache.epoch_digest(41).unwrap(), Some(digest_row(0xaa)));
        assert_eq!(cache.epoch_digest(42).unwrap(), None);
    }

    #[test]
    fn reorg_drops_the_digests_of_every_epoch_it_reaches_into() {
        let (_dir, cache) = temp_cache();
        for index in 0..=2 {
            cache.set_epoch_digest(index, &digest_row(1)).unwrap();
        }

        // Blocks above 15,000 go, so epoch 1 (10,000..19,999) loses part of its range.
        cache.reorg(15_000).unwrap();

        assert_eq!(stored_epochs(&cache), vec![0]);
    }

    #[test]
    fn truncate_from_keeps_the_digests_of_epochs_it_leaves_intact() {
        let (_dir, cache) = temp_cache();
        for index in 0..=2 {
            cache.set_epoch_digest(index, &digest_row(1)).unwrap();
        }

        // Epoch 1 ends at 19,999, so truncating from 20,000 leaves it whole.
        cache.truncate_from(20_000).unwrap();

        assert_eq!(stored_epochs(&cache), vec![0, 1]);
    }

    #[test]
    fn truncate_from_zero_drops_every_digest_with_the_blocks() {
        let (_dir, cache) = temp_cache();
        cache.add(100, &block(100, 1)).unwrap();
        for index in 0..=2 {
            cache.set_epoch_digest(index, &digest_row(1)).unwrap();
        }

        cache.truncate_from(0).unwrap();

        assert_eq!(cache.latest_height().unwrap(), None);
        assert_eq!(stored_epochs(&cache), Vec::<u64>::new());
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
    fn add_batch_appends_consecutive_blocks_in_one_transaction() {
        let (_dir, cache) = temp_cache();
        cache.add(99, &block(99, 0)).unwrap();
        let batch: Vec<CompactBlock> = (100..=105).map(|h| block(h, h as u8)).collect();

        cache.add_batch(&batch).unwrap();

        assert_eq!(cache.latest_height().unwrap(), Some(105));
        assert_eq!(cache.get(103).unwrap(), Some(block(103, 103)));
        assert!(cache.validate_light().is_ok());
    }

    #[test]
    fn add_batch_of_empty_slice_is_a_no_op() {
        let (_dir, cache) = temp_cache();
        cache.add_batch(&[]).unwrap();
        assert_eq!(cache.latest_height().unwrap(), None);
    }

    #[test]
    fn add_batch_rejects_a_batch_that_does_not_extend_the_tip() {
        let (_dir, cache) = temp_cache();
        cache.add(100, &block(100, 1)).unwrap();
        let batch = vec![block(102, 2), block(103, 3)];

        let result = cache.add_batch(&batch);

        assert!(matches!(
            result,
            Err(CacheError::Corruption { height: 102, .. })
        ));
        assert_eq!(cache.latest_height().unwrap(), Some(100)); // aborted, nothing written
    }

    #[test]
    fn add_batch_rejects_a_non_consecutive_batch_without_partial_writes() {
        let (_dir, cache) = temp_cache();
        cache.add(100, &block(100, 1)).unwrap();
        let batch = vec![block(101, 2), block(103, 3)];

        let result = cache.add_batch(&batch);

        assert!(matches!(result, Err(CacheError::Corruption { .. })));
        // The aborted transaction must not have committed the valid prefix.
        assert_eq!(cache.latest_height().unwrap(), Some(100));
        assert!(cache.validate_light().is_ok());
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
    fn lowest_corrupt_height_locates_a_gap_adjacent_to_the_tip() {
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
    fn lowest_corrupt_height_locates_a_gap_with_present_blocks_above_it() {
        let (_dir, cache) = temp_cache();
        // A detected gap always has present blocks above the hole (the last entry is present by
        // definition), so localization must not assume a single present→missing boundary.
        cache
            .insert_raw(100, &block(100, 1).encode_to_vec())
            .unwrap();
        for height in 102..=104 {
            cache
                .insert_raw(height, &block(height, height as u8).encode_to_vec())
                .unwrap();
        }

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
