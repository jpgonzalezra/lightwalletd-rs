//! Reading a snapshot out of a live cache: the manifest, the epoch bodies, and the maintenance of
//! the digests both are built from.
//!
//! Computing an epoch's digests means reading every block in it and decoding each one's hash, so
//! rebuilding the whole manifest on demand would be a full pass over a multi-gigabyte cache on every
//! request. Instead an epoch's digests are computed once, when the epoch becomes immutable, and
//! stored next to the blocks; the manifest is then a cheap read of those rows. An operator enabling
//! this on a cache ingested by an earlier version has no stored digests at all, so the same
//! maintenance walk backfills them from the cache base upward, throttled against the ingestor and
//! resumable. The manifest lists only the epochs that already have digests, which makes it a growing
//! prefix of the served range rather than an error or a startup stall.

use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use prost::Message;

use crate::cache::{Cache, CacheError};
use crate::proto::CompactBlock;

use super::SnapshotError;
use super::format::{
    AnchorHasher, DigestWriter, EPOCH_SIZE, EpochDigest, FORMAT_VERSION, Manifest,
    epoch_first_height, epoch_index, epoch_last_height, write_header, write_record,
};

/// Build the manifest for everything this cache can currently serve.
///
/// `base_height` and `tip_height` describe the cache and are advisory: they are read separately from
/// the epoch rows, and a consumer relies on each entry's own bounds instead.
pub fn manifest(cache: &Cache, chain: &str) -> Result<Manifest, SnapshotError> {
    let (base_height, tip_height) = cache.range()?.unwrap_or((0, 0));
    let mut epochs = Vec::new();
    for (index, row) in cache.epoch_digests()? {
        let digest = EpochDigest::decode(&row).ok_or(SnapshotError::MalformedDigest { index })?;
        epochs.push(digest.entry(index));
    }
    Ok(Manifest {
        format_version: FORMAT_VERSION,
        chain: chain.to_string(),
        epoch_size: EPOCH_SIZE,
        base_height,
        tip_height,
        epochs,
    })
}

/// Stream one published epoch's body into `out`.
///
/// Reads the stored protobuf bytes straight out of the table without decoding them, so the cost is
/// close to a file copy. Only epochs that already have stored digests can be served, which is what
/// makes the served bytes and the advertised digests describe the same thing.
pub fn write_epoch(
    cache: &Cache,
    chain: &str,
    index: u64,
    out: &mut impl Write,
) -> Result<(), SnapshotError> {
    let row = cache
        .epoch_digest(index)?
        .ok_or(SnapshotError::UnknownEpoch { index })?;
    let digest = EpochDigest::decode(&row).ok_or(SnapshotError::MalformedDigest { index })?;
    write_body(cache, chain, digest.start, digest.end, out, |_, _| Ok(()))
}

/// Compute and store the digests for the lowest complete epoch that lacks them, returning which
/// epoch that was, or `None` when there is nothing left to do.
///
/// One call is one unit of maintenance work, which is what makes the walk throttleable and
/// resumable: an interrupted backfill simply continues from the lowest epoch still missing.
pub fn store_next_epoch_digest(cache: &Cache, chain: &str) -> Result<Option<u64>, SnapshotError> {
    let Some(index) = next_pending_epoch(cache)? else {
        return Ok(None);
    };
    let range_before = cache.range()?;
    let Some(digest) = compute_epoch_digest(cache, chain, index)? else {
        return Ok(None);
    };
    // A truncation between the read and the write would leave the row describing blocks the cache no
    // longer holds. The cache drops digest rows for the epochs a truncation touches, but this write
    // comes after that, so it could resurrect one. Only append is safe to race with.
    if cache.range()?.is_some_and(|(base, tip)| {
        range_before
            .is_some_and(|(before_base, before_tip)| base != before_base || tip < before_tip)
    }) {
        tracing::warn!(
            epoch = index,
            "cache was truncated while computing epoch digests; discarding them"
        );
        return Ok(None);
    }
    cache.set_epoch_digest(index, &digest.encode())?;
    Ok(Some(index))
}

/// Tuning for [`maintain_digests`].
#[derive(Debug, Clone, Copy)]
pub struct DigestMaintenance {
    /// Pause between epochs, so a backfill over a large cache never competes with the ingestor for
    /// disk IO at full speed.
    pub epoch_interval: Duration,
    /// Pause taken once every complete epoch has its digests, i.e. until the tip crosses the next
    /// epoch boundary.
    pub idle_interval: Duration,
}

impl Default for DigestMaintenance {
    fn default() -> Self {
        Self {
            epoch_interval: Duration::from_millis(250),
            idle_interval: Duration::from_secs(30),
        }
    }
}

/// Keep the stored epoch digests current, forever: backfill whatever is missing, then compute one
/// epoch each time the tip crosses a boundary. Both are the same walk, so a boundary crossing during
/// a backfill can never make it skip the epochs it has not reached yet.
pub async fn maintain_digests(cache: Arc<Cache>, chain: String, config: DigestMaintenance) {
    tracing::info!("snapshot epoch digest maintenance started");
    loop {
        let cache = Arc::clone(&cache);
        let chain = chain.clone();
        // Computing an epoch decodes 10,000 blocks off disk; keep it off the async runtime's
        // worker threads.
        let computed =
            tokio::task::spawn_blocking(move || store_next_epoch_digest(&cache, &chain)).await;
        let interval = match computed {
            Ok(Ok(Some(index))) => {
                tracing::info!(
                    epoch = index,
                    first_height = epoch_first_height(index),
                    "stored snapshot epoch digests"
                );
                config.epoch_interval
            }
            Ok(Ok(None)) => config.idle_interval,
            Ok(Err(error)) => {
                tracing::warn!(%error, "computing snapshot epoch digests failed; retrying");
                config.idle_interval
            }
            Err(join_error) => {
                tracing::error!(%join_error, "snapshot epoch digest task panicked");
                config.idle_interval
            }
        };
        tokio::time::sleep(interval).await;
    }
}

/// Write an epoch header and the records for `[start, end]`, calling `on_block` with each block's
/// stored bytes on the way.
///
/// The one place an epoch body is produced: the digests are computed by pointing this at a hashing
/// sink, so what is advertised is by construction what is served.
fn write_body(
    cache: &Cache,
    chain: &str,
    start: u64,
    end: u64,
    out: &mut impl Write,
    mut on_block: impl FnMut(u64, &[u8]) -> Result<(), SnapshotError>,
) -> Result<(), SnapshotError> {
    let count = end
        .checked_sub(start)
        .and_then(|span| span.checked_add(1))
        .and_then(|count| u32::try_from(count).ok())
        .ok_or_else(|| SnapshotError::Malformed(format!("epoch bounds [{start}, {end}]")))?;

    write_header(out, chain, start, count)?;
    let mut written = 0u64;
    cache.for_each_raw(start..=end, |height, raw| -> Result<(), SnapshotError> {
        write_record(out, raw)?;
        on_block(height, raw)?;
        written += 1;
        Ok(())
    })?;
    if written != u64::from(count) {
        return Err(SnapshotError::Malformed(format!(
            "epoch [{start}, {end}] holds {written} of {count} blocks"
        )));
    }
    Ok(())
}

/// Compute an epoch's digests from the cache, or `None` if it is not one this cache can publish.
fn compute_epoch_digest(
    cache: &Cache,
    chain: &str,
    index: u64,
) -> Result<Option<EpochDigest>, SnapshotError> {
    let Some((start, end)) = complete_epoch_bounds(cache, index)? else {
        return Ok(None);
    };
    let mut content = DigestWriter::default();
    let mut anchor = AnchorHasher::default();
    write_body(cache, chain, start, end, &mut content, |height, raw| {
        let block = CompactBlock::decode(raw).map_err(CacheError::from)?;
        if block.height != height {
            return Err(CacheError::Corruption {
                height,
                detail: format!("block.height {} does not match key {height}", block.height),
            }
            .into());
        }
        anchor.update(height, &block.hash);
        Ok(())
    })?;
    let (content_digest, bytes) = content.finish();
    Ok(Some(EpochDigest {
        start,
        end,
        bytes,
        content: content_digest,
        anchor: anchor.finish(),
    }))
}

/// The heights `index` covers in this cache, or `None` if the epoch is not both fully cached and
/// immutable.
///
/// Immutable means the tip has moved past the epoch's last height: while the tip sits inside an
/// epoch, a tip reorg can still replace one of its blocks. Anything deeper would have to roll back
/// more than a whole epoch, which crosses the ingestor's floor and empties the cache instead.
fn complete_epoch_bounds(cache: &Cache, index: u64) -> Result<Option<(u64, u64)>, SnapshotError> {
    let Some((base, tip)) = cache.range()? else {
        return Ok(None);
    };
    let start = epoch_first_height(index).max(base);
    let end = epoch_last_height(index);
    if start > end || end >= tip {
        return Ok(None);
    }
    Ok(Some((start, end)))
}

/// The lowest complete epoch with no stored digest.
///
/// Deliberately scans up from the cache base rather than resuming above the highest stored epoch: a
/// tip crossing a boundary during a backfill stores a high epoch, and resuming from there would skip
/// every epoch the backfill had not reached. The scan is one point lookup per epoch over a few
/// hundred rows.
fn next_pending_epoch(cache: &Cache) -> Result<Option<u64>, SnapshotError> {
    let Some((base, tip)) = cache.range()? else {
        return Ok(None);
    };
    let mut index = epoch_index(base);
    while epoch_last_height(index) < tip {
        if cache.epoch_digest(index)?.is_none() {
            return Ok(Some(index));
        }
        index += 1;
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use std::ops::RangeInclusive;

    use super::*;
    use crate::snapshot::format::{EpochHeader, anchor_digest, content_digest, parse_epoch};
    use crate::testutil::temp_cache;

    /// A block whose hash is derived from its height, so every height is distinguishable and the
    /// chain links up.
    fn synthetic_block(height: u64) -> CompactBlock {
        CompactBlock {
            height,
            hash: block_hash(height),
            prev_hash: block_hash(height.saturating_sub(1)),
            ..Default::default()
        }
    }

    fn block_hash(height: u64) -> Vec<u8> {
        let mut hash = vec![0xab; 32];
        hash[..8].copy_from_slice(&height.to_le_bytes());
        hash
    }

    /// A cache holding a chained run of synthetic blocks over `heights`.
    fn cache_over(heights: RangeInclusive<u64>) -> (tempfile::TempDir, Cache) {
        let (dir, cache) = temp_cache();
        let blocks: Vec<CompactBlock> = heights.map(synthetic_block).collect();
        cache.add_batch(&blocks).unwrap();
        (dir, cache)
    }

    /// Run the maintenance walk to completion, returning the epochs it stored in order.
    fn store_all_digests(cache: &Cache) -> Vec<u64> {
        let mut stored = Vec::new();
        while let Some(index) = store_next_epoch_digest(cache, "main").unwrap() {
            stored.push(index);
        }
        stored
    }

    fn export(cache: &Cache, index: u64) -> Vec<u8> {
        let mut body = Vec::new();
        write_epoch(cache, "main", index, &mut body).unwrap();
        body
    }

    /// A cache over epoch 0 whose heights 100..=105 carry the real mainnet blocks from
    /// `testdata/compact_blocks.json`, re-heighted so the cache's append guards accept them. Real
    /// payloads exercise the record framing at realistic sizes instead of the minimal synthetic ones.
    fn cache_with_real_payloads() -> (tempfile::TempDir, Cache) {
        let json = std::fs::read_to_string("testdata/compact_blocks.json").unwrap();
        let fixtures: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap();
        let real: Vec<CompactBlock> = fixtures
            .iter()
            .map(|fixture| {
                let raw = hex::decode(fixture["full"].as_str().unwrap()).unwrap();
                crate::compact::to_compact_block(&raw).unwrap()
            })
            .collect();

        let (dir, cache) = temp_cache();
        let blocks: Vec<CompactBlock> = (0..=10_000u64)
            .map(|height| {
                match height
                    .checked_sub(100)
                    .and_then(|offset| real.get(offset as usize))
                {
                    Some(block) => CompactBlock {
                        height,
                        ..block.clone()
                    },
                    None => synthetic_block(height),
                }
            })
            .collect();
        cache.add_batch(&blocks).unwrap();
        (dir, cache)
    }

    #[test]
    fn exported_epoch_roundtrips_to_the_cached_blocks() {
        let (_dir, cache) = cache_with_real_payloads();
        store_all_digests(&cache);

        let body = export(&cache, 0);
        let (header, records) = parse_epoch(&body).unwrap();

        assert_eq!(
            header,
            EpochHeader {
                chain: "main".to_string(),
                start: 0,
                count: 10_000,
            }
        );
        for (offset, record) in records.iter().enumerate() {
            let height = offset as u64;
            assert_eq!(
                CompactBlock::decode(*record).unwrap(),
                cache.get(height).unwrap().unwrap()
            );
        }
    }

    #[test]
    fn exporting_the_same_epoch_twice_is_byte_identical() {
        let (_dir, cache) = cache_over(0..=10_000);
        store_all_digests(&cache);

        assert_eq!(export(&cache, 0), export(&cache, 0));
    }

    #[test]
    fn the_manifest_entry_matches_the_exported_body() {
        let (_dir, cache) = cache_over(0..=10_000);
        store_all_digests(&cache);
        let body = export(&cache, 0);

        let manifest = manifest(&cache, "main").unwrap();

        let hashes: Vec<(u64, Vec<u8>)> = (0..10_000).map(|h| (h, block_hash(h))).collect();
        let entry = &manifest.epochs[0];
        assert_eq!(
            (
                entry.bytes,
                entry.content_digest.clone(),
                entry.anchor.clone()
            ),
            (
                body.len() as u64,
                hex::encode(content_digest(&body)),
                hex::encode(anchor_digest(
                    hashes
                        .iter()
                        .map(|(height, hash)| (*height, hash.as_slice()))
                ))
            )
        );
    }

    #[test]
    fn a_cache_starting_mid_epoch_publishes_a_partial_first_epoch() {
        let (_dir, cache) = cache_over(9_995..=10_005);
        store_all_digests(&cache);

        let manifest = manifest(&cache, "main").unwrap();

        assert_eq!(
            manifest
                .epochs
                .iter()
                .map(|epoch| (epoch.index, epoch.start, epoch.end))
                .collect::<Vec<_>>(),
            vec![(0, 9_995, 9_999)]
        );
    }

    #[test]
    fn the_epoch_holding_the_tip_is_not_published() {
        // Epoch 0 is complete only once the tip has moved past 9,999; here the tip sits inside it.
        let (_dir, cache) = cache_over(0..=9_999);
        store_all_digests(&cache);

        assert!(manifest(&cache, "main").unwrap().epochs.is_empty());
    }

    #[test]
    fn an_empty_cache_publishes_no_epochs() {
        let (_dir, cache) = temp_cache();

        let manifest = manifest(&cache, "main").unwrap();

        assert_eq!(
            (manifest.base_height, manifest.tip_height, manifest.epochs),
            (0, 0, vec![])
        );
    }

    #[test]
    fn write_epoch_refuses_an_epoch_that_is_not_published() {
        let (_dir, cache) = cache_over(0..=10_000);
        store_all_digests(&cache);

        let mut body = Vec::new();
        assert!(matches!(
            write_epoch(&cache, "main", 1, &mut body),
            Err(SnapshotError::UnknownEpoch { index: 1 })
        ));
    }

    #[test]
    fn the_backfill_stores_every_complete_epoch_and_grows_the_manifest() {
        // A cache written by a version without digest maintenance: blocks, no digest rows.
        let (_dir, cache) = cache_over(0..=20_000);
        assert!(manifest(&cache, "main").unwrap().epochs.is_empty());

        let mut published = Vec::new();
        while store_next_epoch_digest(&cache, "main").unwrap().is_some() {
            published.push(manifest(&cache, "main").unwrap().epochs.len());
        }

        assert_eq!(published, vec![1, 2]);
    }

    #[test]
    fn an_interrupted_backfill_resumes_where_it_stopped() {
        let (_dir, interrupted) = cache_over(0..=20_000);
        let (_uninterrupted_dir, uninterrupted) = cache_over(0..=20_000);
        store_all_digests(&uninterrupted);

        assert_eq!(
            store_next_epoch_digest(&interrupted, "main").unwrap(),
            Some(0)
        );
        // Whatever ran before, the walk continues at the lowest epoch still missing.
        assert_eq!(store_all_digests(&interrupted), vec![1]);
        assert_eq!(
            interrupted.epoch_digests().unwrap(),
            uninterrupted.epoch_digests().unwrap()
        );
    }

    #[test]
    fn digests_are_not_recomputed_once_stored() {
        let (_dir, cache) = cache_over(0..=10_000);

        assert_eq!(store_all_digests(&cache), vec![0]);
        assert_eq!(store_next_epoch_digest(&cache, "main").unwrap(), None);
    }

    #[test]
    fn exporting_is_unaffected_by_concurrent_appends() {
        let (_dir, cache) = cache_over(0..=10_000);
        store_all_digests(&cache);
        let expected = export(&cache, 0);

        std::thread::scope(|scope| {
            let writer = scope.spawn(|| {
                for height in 10_001..=10_050 {
                    cache.add(height, &synthetic_block(height)).unwrap();
                }
            });
            for _ in 0..20 {
                assert_eq!(export(&cache, 0), expected);
            }
            writer.join().unwrap();
        });
    }
}
