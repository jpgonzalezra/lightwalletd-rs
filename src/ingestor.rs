//! Background task that polls the node and fills the block cache.
//!
//! Catch-up runs in windows: up to `window` consecutive blocks are fetched concurrently (at most
//! `concurrency` in-flight node requests) and committed to the cache in a single transaction, so a
//! long initial sync is bounded by node round-trips, not by per-block commits and fsyncs. At the
//! tip the window degrades to one block per step. Every appended block is verified to chain onto
//! the previous one (`prevHash` matches); a mismatch rolls back one block as a reorg.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::cache::{Cache, CacheError};
use crate::config::IngestConfig;
use crate::encoding;
use crate::fetch::{self, FetchError};
use crate::node::{NodeError, NodeRpc};
use crate::proto::CompactBlock;

/// After this many consecutive corruption recoveries without a successful normal step, fall back to
/// the backoff sleep so recovery can never spin at full CPU even if the `fetch` height guard is
/// somehow defeated.
const MAX_CONSECUTIVE_RECOVERIES: u32 = 5;

/// How long to wait when the cache is already at the node's tip before polling again.
const IDLE_POLL: Duration = Duration::from_secs(2);

/// Backoff after a node/transport error (and after corruption past the recovery bound).
const ERROR_BACKOFF: Duration = Duration::from_secs(8);

/// Poll the node forever, appending new blocks to the cache and rolling back reorgs.
pub async fn run(
    node: Arc<dyn NodeRpc>,
    cache: Arc<Cache>,
    start_height: u64,
    config: IngestConfig,
) {
    tracing::info!(
        start_height,
        window = config.window,
        concurrency = config.concurrency,
        "ingestor started"
    );
    let mut consecutive_recoveries = 0u32;
    loop {
        match step(&node, &cache, start_height, &config).await {
            // Advanced (or handled a reorg): go for the next window immediately.
            Ok(Progress::Advanced) => consecutive_recoveries = 0,
            // Cache is at the node's tip: wait before polling again.
            Ok(Progress::Idle) => {
                consecutive_recoveries = 0;
                tokio::time::sleep(IDLE_POLL).await;
            }
            // Cache corruption: truncate from the corrupt point and retry immediately, so re-ingestion
            // refills it rather than the loop stalling on the backoff sleep.
            Err(error) if should_recover(&error, consecutive_recoveries) => {
                consecutive_recoveries += 1;
                tracing::warn!(%error, consecutive_recoveries, "cache corruption during ingest; recovering");
                if let Err(recover_error) = recover(&cache) {
                    tracing::error!(%recover_error, "cache recovery failed; backing off");
                    tokio::time::sleep(ERROR_BACKOFF).await;
                }
            }
            // Node/transport errors — and corruption past the recovery bound — back off.
            Err(error) => {
                if consecutive_recoveries >= MAX_CONSECUTIVE_RECOVERIES {
                    tracing::error!(%error, "corruption recovery limit reached; backing off");
                } else {
                    tracing::warn!(%error, "ingestor step failed; retrying");
                }
                consecutive_recoveries = 0;
                tokio::time::sleep(ERROR_BACKOFF).await;
            }
        }
    }
}

/// Outcome of a successful ingestor step.
#[derive(Debug, PartialEq, Eq)]
enum Progress {
    /// Blocks were appended or a reorg was rolled back; step again immediately.
    Advanced,
    /// The cache is at the node's tip (or the node is behind); poll again after a pause.
    Idle,
}

/// Whether a failed step should truncate-and-recover (then retry immediately) rather than back off: a
/// cache corruption/decode error, but only while consecutive recoveries stay under
/// [`MAX_CONSECUTIVE_RECOVERIES`] — the backstop against a sleepless truncate→re-add→detect loop.
fn should_recover(error: &StepError, consecutive_recoveries: u32) -> bool {
    error.is_corruption() && consecutive_recoveries < MAX_CONSECUTIVE_RECOVERIES
}

/// Locate the lowest corrupt height and truncate from it; re-ingestion refills what was dropped.
fn recover(cache: &Cache) -> Result<(), CacheError> {
    if let Some(corrupt) = cache.lowest_corrupt_height()? {
        tracing::warn!(corrupt, "truncating cache from corrupt height");
        cache.reorg(corrupt.saturating_sub(1))?;
    }
    Ok(())
}

/// Try to ingest the next window of blocks. Returns [`Progress::Advanced`] if blocks were added or a
/// reorg was rolled back, [`Progress::Idle`] if the cache is already at the node's tip.
///
/// The tip height **and** hash come from a single `getblockchaininfo`, so a reorg that replaces the
/// tip block without advancing the height is caught by comparing the hash, not just the height.
async fn step(
    node: &Arc<dyn NodeRpc>,
    cache: &Cache,
    start_height: u64,
    config: &IngestConfig,
) -> Result<Progress, StepError> {
    let info = node.get_blockchain_info().await?;
    let tip_height = info.blocks;

    let next = match cache.latest_height()? {
        None => start_height,
        Some(latest_height) if latest_height == tip_height => {
            // Same height: a tip hash mismatch means a reorg replaced the tip block in place.
            let tip_hash = encoding::display_hex_to_wire(&info.bestblockhash)?;
            if cache
                .latest_hash()?
                .is_some_and(|latest| latest == tip_hash)
            {
                return Ok(Progress::Idle);
            }
            tracing::warn!(
                height = latest_height,
                "tip reorg detected; rolling back one block"
            );
            reorg_to_floor(cache, latest_height.saturating_sub(1), start_height)?;
            return Ok(Progress::Advanced);
        }
        Some(latest_height) if latest_height > tip_height => {
            // Node is behind our cache. Only treat it as a reorg if the node's tip block actually
            // disagrees with what we cached at that height; a node that is merely re-syncing or was
            // restarted from an older snapshot must not drain hours of ingested blocks.
            let tip_hash = encoding::display_hex_to_wire(&info.bestblockhash)?;
            match cache.get(tip_height)? {
                // The node is on our chain, just behind: keep serving the cache and wait.
                Some(cached) if cached.hash == tip_hash => return Ok(Progress::Idle),
                // The node's (shorter) chain disagrees at its tip: a real reorg.
                Some(_) => {
                    tracing::warn!(
                        latest_height,
                        tip_height,
                        "node behind cache on a different chain; rolling back one block"
                    );
                    reorg_to_floor(cache, latest_height.saturating_sub(1), start_height)?;
                    return Ok(Progress::Advanced);
                }
                // The node's tip is below our cached range: nothing to compare against; wait.
                None => return Ok(Progress::Idle),
            }
        }
        Some(latest_height) => latest_height + 1,
    };
    if next > tip_height {
        return Ok(Progress::Idle);
    }

    // Fetch the window concurrently, then keep the longest prefix that chains onto the cached tip.
    let last = tip_height.min(next.saturating_add(config.window.saturating_sub(1) as u64));
    let started = Instant::now();
    let (results, panicked) = fetch_window(node, next..=last, config.concurrency).await;

    let mut prev_hash = cache.latest_hash()?;
    let mut batch: Vec<CompactBlock> = Vec::with_capacity(results.len());
    let mut failure: Option<StepError> = None;
    for (height, result) in results {
        let block = match result {
            Ok(block) => block,
            Err(error) => {
                failure = Some(error.into());
                break;
            }
        };
        if prev_hash
            .as_ref()
            .is_some_and(|prev| *prev != block.prev_hash)
        {
            if batch.is_empty() {
                // The first fetched block does not chain onto the cached tip: a reorg replaced it.
                tracing::warn!(
                    height = next.saturating_sub(1),
                    "reorg detected; rolling back one block"
                );
                reorg_to_floor(cache, next.saturating_sub(2), start_height)?;
                return Ok(Progress::Advanced);
            }
            // The node reorged while the window was in flight: keep the chained prefix; the next
            // step re-checks from the new tip.
            tracing::warn!(
                height,
                "mid-window chain mismatch; keeping the chained prefix"
            );
            break;
        }
        prev_hash = Some(block.hash.clone());
        batch.push(block);
    }

    if let (Some(first), Some(last_block)) = (batch.first(), batch.last()) {
        let (from, to) = (first.height, last_block.height);
        cache.add_batch(&batch)?;
        let seconds = started.elapsed().as_secs_f64().max(f64::EPSILON);
        tracing::info!(
            from,
            to,
            tip = tip_height,
            rate = format_args!("{:.1} blocks/s", batch.len() as f64 / seconds),
            "ingested"
        );
    }

    // A panicked fetch task counts as a window failure too (unless a per-height error already
    // ended the window): it must never be quietly absorbed as a shorter prefix.
    let failure = failure.or_else(|| panicked.map(StepError::FetchTask));
    match failure {
        // Nothing usable arrived: surface the error so the run loop backs off.
        Some(error) if batch.is_empty() => Err(error),
        // Part of the window landed: report progress; the failed remainder is refetched next step.
        Some(error) => {
            tracing::warn!(%error, "partial window ingested; the remainder will be retried");
            Ok(Progress::Advanced)
        }
        None => Ok(Progress::Advanced),
    }
}

/// Fetch `heights` from the node with at most `concurrency` requests in flight, returning each
/// height's result in ascending order plus, separately, the first fetch task that panicked (its
/// height is lost with the panic, so it cannot be a per-height entry). Failures are per-height;
/// the caller decides how much of the window to keep.
async fn fetch_window(
    node: &Arc<dyn NodeRpc>,
    heights: std::ops::RangeInclusive<u64>,
    concurrency: usize,
) -> (
    BTreeMap<u64, Result<CompactBlock, FetchError>>,
    Option<tokio::task::JoinError>,
) {
    let semaphore = Arc::new(Semaphore::new(concurrency.max(1)));
    let mut tasks = JoinSet::new();
    for height in heights {
        let node = Arc::clone(node);
        let semaphore = Arc::clone(&semaphore);
        tasks.spawn(async move {
            // `acquire_owned` only fails if the semaphore were closed, which nothing here does;
            // treat it as this height's fetch failing rather than panicking the task.
            match semaphore.acquire_owned().await {
                Ok(_permit) => (height, fetch::compact_block(node.as_ref(), height).await),
                Err(closed) => (height, Err(closed.into())),
            }
        });
    }
    let mut results = BTreeMap::new();
    let mut panicked = None;
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok((height, result)) => {
                results.insert(height, result);
            }
            // A panicked fetch is a bug, but the ingestor task's handle is detached (lib.rs), so
            // resuming the unwind would kill ingestion silently while the server keeps serving a
            // frozen cache. Surface it loudly instead and let the step fail so the run loop backs
            // off — a deterministic panic then shows up as a repeating error, not a hang.
            Err(join_error) if join_error.is_panic() => {
                tracing::error!(%join_error, "ingest fetch task panicked");
                panicked.get_or_insert(join_error);
            }
            // Cancelled (runtime shutdown mid-window): the missing height simply ends the chained
            // prefix, and the next step — if any — refetches from there.
            Err(join_error) => {
                tracing::warn!(%join_error, "ingest fetch task cancelled; skipping");
            }
        }
    }
    (results, panicked)
}

/// Roll the cache back to `target` (keeping `target` itself). A rollback that would cross the
/// `start_height` floor — or that could not lower the tip (the genesis-saturation case) — empties
/// the cache instead of wedging against the floor: an empty cache chains onto anything, so the next
/// step resumes ingesting the node's chain from `start_height`.
fn reorg_to_floor(cache: &Cache, target: u64, start_height: u64) -> Result<(), CacheError> {
    let would_not_lower_tip = match cache.latest_height()? {
        Some(tip) => target >= tip,
        None => true,
    };
    if target < start_height || would_not_lower_tip {
        tracing::warn!(
            target,
            start_height,
            "reorg reaches the cache floor; emptying the cache to resume from the node's chain"
        );
        cache.truncate_from(0)?;
        return Ok(());
    }
    cache.reorg(target)?;
    Ok(())
}

/// Errors from a single ingestor step.
#[derive(Debug, thiserror::Error)]
enum StepError {
    #[error(transparent)]
    Node(#[from] NodeError),
    #[error(transparent)]
    Fetch(#[from] FetchError),
    #[error(transparent)]
    Cache(#[from] CacheError),
    #[error("decoding tip hash: {0}")]
    Encoding(#[from] hex::FromHexError),
    #[error("ingest fetch task panicked: {0}")]
    FetchTask(tokio::task::JoinError),
}

impl StepError {
    /// Whether this error is a cache corruption recoverable by truncation, as opposed to a
    /// node/transport failure (which only warrants a backoff retry).
    fn is_corruption(&self) -> bool {
        matches!(
            self,
            StepError::Cache(CacheError::Corruption { .. } | CacheError::Decode(_))
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    use crate::node::{GetBlockVerbose, GetBlockchainInfo};
    use crate::proto::CompactBlock;
    use crate::testutil::{FakeNode, temp_cache, testdata_blocks};

    fn tip_block(height: u64, hash: Vec<u8>) -> CompactBlock {
        CompactBlock {
            height,
            hash,
            ..Default::default()
        }
    }

    /// The raw block and its parsed form for fixture `index` (heights 289460..=289465). The parsed
    /// height is used so the cache guards and the fetch height check hold.
    fn fixture(index: usize) -> (Vec<u8>, CompactBlock) {
        let json = std::fs::read_to_string("testdata/compact_blocks.json").unwrap();
        let fixtures: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap();
        let raw = hex::decode(fixtures[index]["full"].as_str().unwrap()).unwrap();
        let parsed = crate::compact::to_compact_block(&raw).unwrap();
        (raw, parsed)
    }

    fn blockchain_info(blocks: u64, bestblockhash: &str) -> GetBlockchainInfo {
        serde_json::from_value(json!({
            "chain": "main",
            "blocks": blocks,
            "bestblockhash": bestblockhash,
            "consensus": { "chaintip": "00000000" },
        }))
        .unwrap()
    }

    fn verbose_for(raw: &[u8]) -> (String, GetBlockVerbose) {
        // The verbose hash must match the raw block's real hash, which the fetch path verifies.
        let hash = crate::encoding::wire_to_display_hex(
            &crate::compact::to_compact_block(raw).unwrap().hash,
        );
        let verbose = serde_json::from_value(json!({
            "hash": hash,
            "trees": { "sapling": { "size": 0 }, "orchard": { "size": 0 } },
        }))
        .unwrap();
        (hash, verbose)
    }

    fn fake_serving(raw: Vec<u8>, tip: u64) -> FakeNode {
        let (_, verbose) = verbose_for(&raw);
        FakeNode {
            blockchain_info: Some(blockchain_info(tip, "00")),
            block_verbose: Some(verbose),
            block_raw: Some(raw),
            ..Default::default()
        }
    }

    /// A fake serving a whole chain of raw blocks, each keyed by its real height and hash.
    fn fake_serving_chain(raws: Vec<Vec<u8>>, tip: u64) -> FakeNode {
        let mut verbose_by_height = HashMap::new();
        let mut raw_by_hash = HashMap::new();
        for raw in raws {
            let height = crate::compact::to_compact_block(&raw).unwrap().height;
            let (hash, verbose) = verbose_for(&raw);
            verbose_by_height.insert(height, verbose);
            raw_by_hash.insert(hash, raw);
        }
        FakeNode {
            blockchain_info: Some(blockchain_info(tip, "00")),
            verbose_by_height,
            raw_by_hash,
            ..Default::default()
        }
    }

    fn config() -> IngestConfig {
        IngestConfig {
            window: 64,
            concurrency: 8,
        }
    }

    async fn run_step(fake: FakeNode, cache: &Cache, start: u64) -> Result<Progress, StepError> {
        run_step_with(fake, cache, start, config()).await
    }

    async fn run_step_with(
        fake: FakeNode,
        cache: &Cache,
        start: u64,
        config: IngestConfig,
    ) -> Result<Progress, StepError> {
        let node: Arc<dyn NodeRpc> = Arc::new(fake);
        step(&node, cache, start, &config).await
    }

    #[tokio::test]
    async fn step_appends_block_that_chains_onto_the_cached_tip() {
        let (raw, parsed) = fixture(0);
        let height = parsed.height;
        let (_dir, cache) = temp_cache();
        cache
            .add(height - 1, &tip_block(height - 1, parsed.prev_hash.clone()))
            .unwrap();

        let progress = run_step(fake_serving(raw, height), &cache, height - 1)
            .await
            .unwrap();

        assert_eq!(progress, Progress::Advanced);
        assert_eq!(cache.latest_height().unwrap(), Some(height));
    }

    #[tokio::test]
    async fn step_ingests_a_whole_window_in_one_step() {
        // Four consecutive real blocks (380640..=380643): an empty cache and a node tip at the last
        // height must land in a single step, committed as one batch.
        let raws = testdata_blocks();
        let first = crate::compact::to_compact_block(&raws[0]).unwrap().height;
        let last = first + raws.len() as u64 - 1;
        let (_dir, cache) = temp_cache();

        let progress = run_step(fake_serving_chain(raws, last), &cache, first)
            .await
            .unwrap();

        assert_eq!(progress, Progress::Advanced);
        assert_eq!(cache.latest_height().unwrap(), Some(last));
        assert!(cache.validate_light().is_ok());
    }

    #[tokio::test]
    async fn step_window_is_clamped_by_the_configured_size() {
        let raws = testdata_blocks();
        let first = crate::compact::to_compact_block(&raws[0]).unwrap().height;
        let last = first + raws.len() as u64 - 1;
        let (_dir, cache) = temp_cache();

        let config = IngestConfig {
            window: 2,
            concurrency: 8,
        };
        let progress = run_step_with(fake_serving_chain(raws, last), &cache, first, config)
            .await
            .unwrap();

        assert_eq!(progress, Progress::Advanced);
        assert_eq!(cache.latest_height().unwrap(), Some(first + 1));
    }

    #[tokio::test]
    async fn step_commits_the_available_prefix_when_a_fetch_fails_mid_window() {
        // The node serves only the first two blocks of the window; the step must commit those two
        // and report progress, leaving the remainder for the next step.
        let raws = testdata_blocks();
        let first = crate::compact::to_compact_block(&raws[0]).unwrap().height;
        let tip = first + raws.len() as u64 - 1;
        let (_dir, cache) = temp_cache();

        let progress = run_step(
            fake_serving_chain(raws.into_iter().take(2).collect(), tip),
            &cache,
            first,
        )
        .await
        .unwrap();

        assert_eq!(progress, Progress::Advanced);
        assert_eq!(cache.latest_height().unwrap(), Some(first + 1));
    }

    #[tokio::test]
    async fn step_keeps_the_chained_prefix_on_a_mid_window_chain_mismatch() {
        // Corrupt the third block's prevHash so it no longer chains onto the second. Its own hash
        // (recomputed by the fake from the mutated bytes) stays self-consistent, so only the chain
        // check can reject it — the step must commit the first two blocks and stop there.
        let mut raws = testdata_blocks();
        raws[2][4..36].fill(0xee);
        let first = crate::compact::to_compact_block(&raws[0]).unwrap().height;
        let tip = first + raws.len() as u64 - 1;
        let (_dir, cache) = temp_cache();

        let progress = run_step(fake_serving_chain(raws, tip), &cache, first)
            .await
            .unwrap();

        assert_eq!(progress, Progress::Advanced);
        assert_eq!(cache.latest_height().unwrap(), Some(first + 1));
    }

    #[tokio::test]
    async fn step_is_idle_when_the_tip_hash_matches_at_the_same_height() {
        let (_dir, cache) = temp_cache();
        cache.add(100, &tip_block(100, vec![0u8; 32])).unwrap();

        // The cached tip hash is 32 zero bytes; its display-order hex is 64 zeros.
        let fake = FakeNode {
            blockchain_info: Some(blockchain_info(100, &"00".repeat(32))),
            ..Default::default()
        };
        let progress = run_step(fake, &cache, 100).await.unwrap();

        assert_eq!(progress, Progress::Idle);
    }

    #[tokio::test]
    async fn step_rolls_back_one_block_when_the_tip_hash_differs_at_the_same_height() {
        let (_dir, cache) = temp_cache();
        cache.add(100, &tip_block(100, vec![0xaa; 32])).unwrap();
        cache.add(101, &tip_block(101, vec![0xbb; 32])).unwrap();

        // Same height 101, but the node reports a different tip hash → an in-place tip reorg.
        let fake = FakeNode {
            blockchain_info: Some(blockchain_info(101, &"cc".repeat(32))),
            ..Default::default()
        };
        let progress = run_step(fake, &cache, 100).await.unwrap();

        assert_eq!(progress, Progress::Advanced);
        assert_eq!(cache.latest_height().unwrap(), Some(100));
    }

    #[tokio::test]
    async fn step_idles_when_the_node_is_behind_but_on_the_same_chain() {
        // Cache [100..=102]; the node reports tip 101 with exactly the hash we cached at 101 — a
        // node that is merely behind (restart, re-sync). The cache must be left intact.
        let (_dir, cache) = temp_cache();
        cache.add(100, &tip_block(100, vec![0xaa; 32])).unwrap();
        cache.add(101, &tip_block(101, vec![0xbb; 32])).unwrap();
        cache.add(102, &tip_block(102, vec![0xcc; 32])).unwrap();

        let fake = FakeNode {
            blockchain_info: Some(blockchain_info(101, &"bb".repeat(32))),
            ..Default::default()
        };
        let progress = run_step(fake, &cache, 100).await.unwrap();

        assert_eq!(progress, Progress::Idle);
        assert_eq!(cache.latest_height().unwrap(), Some(102)); // nothing drained
    }

    #[tokio::test]
    async fn step_idles_when_the_node_tip_is_below_the_cached_range() {
        // The node re-synced from scratch and is still below our cache floor: nothing to compare,
        // so wait rather than drain.
        let (_dir, cache) = temp_cache();
        cache.add(100, &tip_block(100, vec![0xaa; 32])).unwrap();

        let fake = FakeNode {
            blockchain_info: Some(blockchain_info(50, &"dd".repeat(32))),
            ..Default::default()
        };
        let progress = run_step(fake, &cache, 100).await.unwrap();

        assert_eq!(progress, Progress::Idle);
        assert_eq!(cache.latest_height().unwrap(), Some(100));
    }

    #[tokio::test]
    async fn step_rolls_back_when_the_node_is_behind_on_a_different_chain() {
        // Cache [100..=102]; the node reports tip 101 with a hash that disagrees with our cached
        // block 101 — a genuine reorg onto a shorter chain. Roll back one block per detection.
        let (_dir, cache) = temp_cache();
        cache.add(100, &tip_block(100, vec![0xaa; 32])).unwrap();
        cache.add(101, &tip_block(101, vec![0xbb; 32])).unwrap();
        cache.add(102, &tip_block(102, vec![0xcc; 32])).unwrap();

        let fake = FakeNode {
            blockchain_info: Some(blockchain_info(101, &"ee".repeat(32))),
            ..Default::default()
        };
        let progress = run_step(fake, &cache, 100).await.unwrap();

        assert_eq!(progress, Progress::Advanced);
        assert_eq!(cache.latest_height().unwrap(), Some(101));
    }

    #[tokio::test]
    async fn step_rolls_back_one_block_when_prev_hash_does_not_chain() {
        let (raw, parsed) = fixture(0);
        let height = parsed.height;
        let (_dir, cache) = temp_cache();
        // Cache [height-2, height-1], floor = start_height = height-2. The fetched block `height`
        // does not chain, so the reorg target is height-2 (== floor): allowed, keeping [height-2].
        cache
            .add(height - 2, &tip_block(height - 2, vec![0xee; 32]))
            .unwrap();
        cache
            .add(height - 1, &tip_block(height - 1, vec![0xff; 32]))
            .unwrap();

        let progress = run_step(fake_serving(raw, height), &cache, height - 2)
            .await
            .unwrap();

        assert_eq!(progress, Progress::Advanced);
        assert_eq!(cache.latest_height().unwrap(), Some(height - 2));
    }

    #[tokio::test]
    async fn step_reorg_below_start_height_floor_empties_the_cache_and_resumes() {
        let (_dir, cache) = temp_cache();
        // Cache [100], floor = start_height = 100.
        cache.add(100, &tip_block(100, vec![0xaa; 32])).unwrap();

        // Node reports the same height with a different tip hash → same-height reorg branch, whose
        // target 99 falls below the floor. Instead of wedging (error-looping while serving a stale
        // tip), the cache empties; the next step re-ingests block 100 from the node's chain.
        let fake = FakeNode {
            blockchain_info: Some(blockchain_info(100, &"cc".repeat(32))),
            ..Default::default()
        };
        let progress = run_step(fake, &cache, 100).await.unwrap();

        assert_eq!(progress, Progress::Advanced);
        assert_eq!(cache.latest_height().unwrap(), None); // emptied, ready to re-chain
    }

    #[tokio::test]
    async fn step_genesis_floor_reorg_empties_the_cache_instead_of_no_op_progress() {
        let (_dir, cache) = temp_cache();
        // Cache [0], floor = start_height = 0 (empty upgrade list, e.g. regtest).
        cache.add(0, &tip_block(0, vec![0xaa; 32])).unwrap();

        // Node reports the same height 0 with a different tip hash → in-place tip reorg branch,
        // whose target saturates to 0 — a rollback that cannot lower the tip. The cache must empty
        // (making real progress possible next step) rather than run a no-op reorg in a hot loop.
        let fake = FakeNode {
            blockchain_info: Some(blockchain_info(0, &"cc".repeat(32))),
            ..Default::default()
        };
        let progress = run_step(fake, &cache, 0).await.unwrap();

        assert_eq!(progress, Progress::Advanced);
        assert_eq!(cache.latest_height().unwrap(), None);
    }

    #[tokio::test]
    async fn step_surfaces_corruption_when_the_cached_tip_is_undecodable() {
        let (_dir, cache) = temp_cache();
        cache.insert_raw(100, &[0x08, 0xff]).unwrap();

        // Same height as the tip → `step` reads the (undecodable) tip hash and surfaces the corruption.
        let fake = FakeNode {
            blockchain_info: Some(blockchain_info(100, &"00".repeat(32))),
            ..Default::default()
        };
        let error = run_step(fake, &cache, 100).await.unwrap_err();

        assert!(error.is_corruption());
    }

    #[tokio::test]
    async fn step_wrong_height_block_is_not_classified_as_corruption() {
        let (raw, parsed) = fixture(0);
        let (_dir, cache) = temp_cache();

        // Empty cache, node well ahead: `step` fetches from `start_height`, but the node serves a
        // block at a different height → a `FetchError`, kept on the node backoff (never a cache
        // corruption).
        let fake = fake_serving(raw, parsed.height + 10);
        let error = run_step(fake, &cache, parsed.height - 5).await.unwrap_err();

        assert!(!error.is_corruption());
        assert!(matches!(
            error,
            StepError::Fetch(FetchError::UnexpectedHeight { .. })
        ));
    }

    #[tokio::test]
    async fn step_surfaces_a_panicked_fetch_task_as_a_step_error() {
        let (_dir, cache) = temp_cache();

        // Only `get_blockchain_info` is configured, so the window's fetch task panics inside the
        // fake ("get_block_verbose not configured"). The panic must come back as a step failure —
        // driving the run loop's backoff — rather than unwinding into (and silently killing) the
        // detached ingestor task, and rather than being absorbed as an empty window.
        let fake = FakeNode {
            blockchain_info: Some(blockchain_info(100, &"00".repeat(32))),
            ..Default::default()
        };
        let error = run_step(fake, &cache, 100).await.unwrap_err();

        assert!(matches!(error, StepError::FetchTask(_)));
        assert!(!error.is_corruption());
    }

    #[test]
    fn recover_truncates_a_corrupt_suffix_to_the_last_good_block() {
        let (_dir, cache) = temp_cache();
        for height in 100..=102 {
            cache
                .add(height, &tip_block(height, vec![height as u8; 32]))
                .unwrap();
        }
        cache.insert_raw(103, &[0x08, 0xff]).unwrap();

        recover(&cache).unwrap();

        assert_eq!(cache.latest_height().unwrap(), Some(102));
        assert!(cache.validate_light().is_ok());
    }

    #[test]
    fn should_recover_only_for_corruption_within_the_recovery_bound() {
        let corruption = StepError::Cache(CacheError::Corruption {
            height: 1,
            detail: String::new(),
        });
        assert!(should_recover(&corruption, 0));
        assert!(should_recover(&corruption, MAX_CONSECUTIVE_RECOVERIES - 1));
        // At the cap, fall back to backoff instead of recovering again (no sleepless loop).
        assert!(!should_recover(&corruption, MAX_CONSECUTIVE_RECOVERIES));

        let transport = StepError::Fetch(FetchError::UnexpectedHeight {
            requested: 5,
            got: 9,
        });
        assert!(!should_recover(&transport, 0));
    }
}
