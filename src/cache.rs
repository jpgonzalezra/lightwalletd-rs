//! On-disk store of compact blocks, keyed by height, backed by `redb`.
//!
//! Each block is stored as its protobuf encoding under its height. The store is ordered, so the lowest
//! and highest cached heights are cheap to read, and a reorg is just "drop everything above height N".

use std::path::Path;

use prost::Message;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

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

    /// Store the compact block at `height`, overwriting any existing entry.
    pub fn add(&self, height: u64, block: &CompactBlock) -> Result<(), CacheError> {
        let bytes = block.encode_to_vec();
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(BLOCKS)?;
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

    /// The lowest cached height, or `None` if the cache is empty.
    pub fn first_height(&self) -> Result<Option<u64>, CacheError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(BLOCKS)?;
        Ok(table.first()?.map(|(height, _)| height.value()))
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(height: u64, hash_byte: u8) -> CompactBlock {
        CompactBlock {
            height,
            hash: vec![hash_byte; 32],
            ..Default::default()
        }
    }

    fn temp_cache() -> (tempfile::TempDir, Cache) {
        let dir = tempfile::tempdir().unwrap();
        let cache = Cache::open(&dir.path().join("blocks.redb")).unwrap();
        (dir, cache)
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
}
