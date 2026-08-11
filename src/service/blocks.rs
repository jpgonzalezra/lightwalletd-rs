//! Block-serving methods: `GetBlock`, `GetBlockNullifiers`, `GetBlockRange`, and
//! `GetBlockRangeNullifiers`.

use std::ops::RangeInclusive;
use std::sync::Arc;

use async_stream::try_stream;
use tonic::{Request, Response, Status};

use crate::cache::Cache;
use crate::node::NodeRpc;
use crate::proto::{BlockId, BlockRange, BoxStream, CompactBlock};
use crate::repair::RepairSignal;
use crate::{fetch, filter};

use super::{Streamer, block_at, errors};

/// Maximum number of blocks a single `GetBlockRange(Nullifiers)` request may span.
/// A wallet syncs in bounded windows; an unbounded span is a denial-of-service lever.
const MAX_BLOCK_RANGE: u64 = 10_000;

/// Validate an extracted block range: both bounds must be specified (non-zero), and
/// the span must not exceed [`MAX_BLOCK_RANGE`].
fn validate_block_range(start: u64, end: u64) -> Result<(), Status> {
    if start == 0 || end == 0 {
        return Err(Status::invalid_argument(
            "get_block_range: start and end heights must be specified (non-zero)",
        ));
    }
    let span = start.abs_diff(end) + 1;
    if span > MAX_BLOCK_RANGE {
        return Err(Status::invalid_argument(format!(
            "get_block_range: requested {span} blocks exceeds the maximum of {MAX_BLOCK_RANGE}"
        )));
    }
    Ok(())
}

pub(super) async fn get_block(
    streamer: &Streamer,
    request: Request<BlockId>,
) -> Result<Response<CompactBlock>, Status> {
    let block_id = request.into_inner();
    if block_id.height == 0 && block_id.hash.is_empty() {
        return Err(Status::invalid_argument(
            "get_block: request for unspecified identifier",
        ));
    }
    if !block_id.hash.is_empty() {
        return Err(Status::unimplemented(
            "get_block by hash is not yet supported",
        ));
    }
    let block = block_at(&streamer.cache, streamer.node.as_ref(), block_id.height).await?;
    Ok(Response::new(block))
}

pub(super) async fn get_block_nullifiers(
    streamer: &Streamer,
    request: Request<BlockId>,
) -> Result<Response<CompactBlock>, Status> {
    let block_id = request.into_inner();
    if block_id.height == 0 && block_id.hash.is_empty() {
        return Err(Status::invalid_argument(
            "get_block_nullifiers: request for unspecified identifier",
        ));
    }
    if !block_id.hash.is_empty() {
        return Err(Status::unimplemented(
            "get_block_nullifiers by hash is not yet supported",
        ));
    }
    let block = block_at(&streamer.cache, streamer.node.as_ref(), block_id.height).await?;
    Ok(Response::new(filter::nullifiers_only(block)))
}

pub(super) async fn get_block_range(
    streamer: &Streamer,
    request: Request<BlockRange>,
) -> Result<Response<BoxStream<CompactBlock>>, Status> {
    let range = request.into_inner();
    let pool_types = range.pool_types;
    filter::validate_pool_types(&pool_types)?;
    let (Some(start), Some(end)) = (range.start, range.end) else {
        return Err(Status::invalid_argument(
            "get_block_range: must specify start and end heights",
        ));
    };
    let (start, end) = (start.height, end.height);
    validate_block_range(start, end)?;
    let stream = block_range_stream(
        streamer.cache.clone(),
        streamer.node.clone(),
        streamer.repair.clone(),
        start,
        end,
        move |block| filter::filter_block_to_pools(block, &pool_types),
    );
    Ok(Response::new(stream))
}

pub(super) async fn get_block_range_nullifiers(
    streamer: &Streamer,
    request: Request<BlockRange>,
) -> Result<Response<BoxStream<CompactBlock>>, Status> {
    let range = request.into_inner();
    // An invalid pool type is rejected up front, for parity with `get_block_range`. The requested
    // pools are otherwise honored (transparent is always dropped; see
    // `filter::filter_block_to_pools_nullifiers_only`): this is not the legacy "ignore pool_types
    // entirely" behavior.
    filter::validate_pool_types(&range.pool_types)?;
    let pool_types = range.pool_types;
    let (Some(start), Some(end)) = (range.start, range.end) else {
        return Err(Status::invalid_argument(
            "get_block_range_nullifiers: must specify start and end heights",
        ));
    };
    let (start, end) = (start.height, end.height);
    validate_block_range(start, end)?;
    let stream = block_range_stream(
        streamer.cache.clone(),
        streamer.node.clone(),
        streamer.repair.clone(),
        start,
        end,
        move |block| filter::filter_block_to_pools_nullifiers_only(block, &pool_types),
    );
    Ok(Response::new(stream))
}

/// Number of consecutive heights read from the cache under one read transaction. Bounds both how
/// much of a range is held in memory at once and how long a transaction stays open: an open read
/// transaction keeps `redb` from reclaiming superseded pages, and a stream advances at the client's
/// pace, so spanning a whole 10,000-block range in one transaction would let a slow wallet pin the
/// cache file's growth.
const CACHE_READ_CHUNK: u64 = 64;

/// Where a block in the stream came from. A discontinuity between two node-served blocks is the node
/// reorging between two fetches, which the cache had no part in and cannot repair.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum BlockSource {
    Cache,
    Node,
}

/// What the next block in the stream must hash to, and the height that established it.
struct ChainLink {
    height: u64,
    /// Ascending, the previous block's `hash`, matched against the next block's `prev_hash`;
    /// descending, the previous block's `prev_hash`, matched against the next block's `hash`.
    expected_hash: Vec<u8>,
    source: BlockSource,
}

impl ChainLink {
    /// The link `block` establishes for whatever follows it in this direction.
    fn after(height: u64, block: &CompactBlock, descending: bool, source: BlockSource) -> Self {
        let expected_hash = if descending {
            block.prev_hash.clone()
        } else {
            block.hash.clone()
        };
        Self {
            height,
            expected_hash,
            source,
        }
    }

    /// Check that `block` connects to this link, reporting the suspect height for repair if it does
    /// not and the cache is implicated.
    ///
    /// A range is resolved height by height from the cache or the node, and during a reorg repair
    /// those two disagree: the cache can still hold the abandoned fork below the point the ingestor
    /// has rolled back to while the node already serves the new chain. Splicing them would describe a
    /// chain that never existed, so the stream fails instead.
    ///
    /// Only a seam with a cache-served block on at least one side is reported: two node-served blocks
    /// that do not connect are the node reorging between two fetches, and truncating the cache for
    /// that repairs nothing (the seam is above the cached tip, where nothing is cached to drop).
    fn check(
        &self,
        block: &CompactBlock,
        height: u64,
        descending: bool,
        source: BlockSource,
        repair: Option<&RepairSignal>,
    ) -> Result<(), Status> {
        let actual_hash = if descending {
            &block.hash
        } else {
            &block.prev_hash
        };
        if *actual_hash == self.expected_hash {
            return Ok(());
        }
        // Either side of the seam may be the stale one, so the lower height is what has to go: dropping
        // it drops everything above it too.
        let suspect_height = height.min(self.height);
        let cache_implicated = self.source == BlockSource::Cache || source == BlockSource::Cache;
        tracing::warn!(
            height,
            previous_height = self.height,
            suspect_height,
            cache_implicated,
            "chain discontinuity while serving a block range"
        );
        if cache_implicated && let Some(repair) = repair {
            repair.report(suspect_height);
        }
        Err(Status::aborted(format!(
            "get_block_range: chain discontinuity at height {height}; the chain reorged while \
             serving, retry"
        )))
    }
}

/// The sub-ranges of `low..=high` to read from the cache, in the order they are served.
fn cache_chunks(low: u64, high: u64, descending: bool) -> Vec<RangeInclusive<u64>> {
    let mut chunks = Vec::new();
    let mut chunk_low = low;
    loop {
        let chunk_high = chunk_low.saturating_add(CACHE_READ_CHUNK - 1).min(high);
        chunks.push(chunk_low..=chunk_high);
        if chunk_high >= high {
            break;
        }
        chunk_low = chunk_high + 1;
    }
    if descending {
        chunks.reverse();
    }
    chunks
}

/// Stream the blocks in the range (ascending if `start <= end`, otherwise descending), reading them
/// from the cache in chunks and falling back to the node per missing height, and applying `transform`
/// before yielding each. Shared by `GetBlockRange` and `GetBlockRangeNullifiers`, which differ only in
/// that final transform.
///
/// Consecutive blocks are checked to connect, whichever source they came from, so a range served
/// across a reorg repair aborts rather than splicing two chains together.
fn block_range_stream(
    cache: Arc<Cache>,
    node: Arc<dyn NodeRpc>,
    repair: Option<RepairSignal>,
    start: u64,
    end: u64,
    transform: impl Fn(CompactBlock) -> CompactBlock + Send + 'static,
) -> BoxStream<CompactBlock> {
    let descending = start > end;
    let (low, high) = if descending {
        (end, start)
    } else {
        (start, end)
    };
    Box::pin(try_stream! {
        let mut link: Option<ChainLink> = None;
        for chunk in cache_chunks(low, high, descending) {
            // One transaction per chunk, released before the node is awaited: the fetch below is the
            // slow part, and holding a read transaction across it would pin the cache for no gain.
            let mut cached = cache.get_range(chunk.clone())?;
            let heights: Vec<u64> = if descending {
                chunk.rev().collect()
            } else {
                chunk.collect()
            };
            for height in heights {
                let (block, source) = match cached.remove(&height) {
                    Some(block) => (block, BlockSource::Cache),
                    None => (
                        fetch::compact_block(node.as_ref(), height)
                            .await
                            .map_err(|err| errors::block_fetch_to_status(err, height))?,
                        BlockSource::Node,
                    ),
                };
                if let Some(previous) = &link {
                    previous.check(&block, height, descending, source, repair.as_ref())?;
                }
                link = Some(ChainLink::after(height, &block, descending, source));
                yield transform(block);
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tonic::{Code, Request};

    use crate::proto::compact_tx_streamer_server::CompactTxStreamer;
    use crate::proto::{BlockId, BlockRange, PoolType};
    use crate::testutil::{FakeNode, temp_cache};

    use super::super::Streamer;
    use super::{MAX_BLOCK_RANGE, validate_block_range};

    fn streamer() -> (tempfile::TempDir, Streamer) {
        let (dir, cache) = temp_cache();
        let node = Arc::new(FakeNode::default());
        let streamer = Streamer::new(node, Arc::new(cache), "main".to_string(), None);
        (dir, streamer)
    }

    fn range(start: u64, end: u64) -> BlockRange {
        BlockRange {
            start: Some(BlockId {
                height: start,
                hash: vec![],
            }),
            end: Some(BlockId {
                height: end,
                hash: vec![],
            }),
            pool_types: vec![],
        }
    }

    #[tokio::test]
    async fn get_block_range_rejects_zero_start() {
        let (_dir, streamer) = streamer();
        let status = streamer
            .get_block_range(Request::new(range(0, 10)))
            .await
            .err()
            .unwrap();
        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn get_block_range_rejects_zero_end() {
        let (_dir, streamer) = streamer();
        let status = streamer
            .get_block_range(Request::new(range(10, 0)))
            .await
            .err()
            .unwrap();
        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn get_block_range_rejects_span_over_cap() {
        let (_dir, streamer) = streamer();
        let status = streamer
            .get_block_range(Request::new(range(1, MAX_BLOCK_RANGE + 1)))
            .await
            .err()
            .unwrap();
        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn get_block_range_nullifiers_rejects_span_over_cap() {
        let (_dir, streamer) = streamer();
        let status = streamer
            .get_block_range_nullifiers(Request::new(range(1, MAX_BLOCK_RANGE + 1)))
            .await
            .err()
            .unwrap();
        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[test]
    fn cache_chunks_covers_a_short_range_in_one_chunk() {
        assert_eq!(super::cache_chunks(10, 20, false), vec![10..=20]);
    }

    #[test]
    fn cache_chunks_splits_a_long_range_at_the_chunk_size() {
        assert_eq!(
            super::cache_chunks(1, 2 * super::CACHE_READ_CHUNK, false),
            vec![
                1..=super::CACHE_READ_CHUNK,
                (super::CACHE_READ_CHUNK + 1)..=(2 * super::CACHE_READ_CHUNK)
            ]
        );
    }

    #[test]
    fn cache_chunks_are_ordered_high_to_low_when_descending() {
        assert_eq!(
            super::cache_chunks(1, 2 * super::CACHE_READ_CHUNK, true),
            vec![
                (super::CACHE_READ_CHUNK + 1)..=(2 * super::CACHE_READ_CHUNK),
                1..=super::CACHE_READ_CHUNK
            ]
        );
    }

    #[test]
    fn cache_chunks_covers_a_single_height() {
        assert_eq!(super::cache_chunks(7, 7, false), vec![7..=7]);
    }

    #[test]
    fn validate_block_range_accepts_small_window() {
        assert!(validate_block_range(1, 3).is_ok());
    }

    #[test]
    fn validate_block_range_accepts_span_at_cap() {
        assert!(validate_block_range(1, MAX_BLOCK_RANGE).is_ok());
    }

    #[tokio::test]
    async fn get_block_range_rejects_invalid_pool_type() {
        let (_dir, streamer) = streamer();
        let request = BlockRange {
            start: Some(BlockId {
                height: 1,
                hash: vec![],
            }),
            end: Some(BlockId {
                height: 10,
                hash: vec![],
            }),
            pool_types: vec![PoolType::Invalid as i32],
        };
        let status = streamer
            .get_block_range(Request::new(request))
            .await
            .err()
            .unwrap();
        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn get_block_range_nullifiers_rejects_invalid_pool_type() {
        let (_dir, streamer) = streamer();
        let request = BlockRange {
            start: Some(BlockId {
                height: 1,
                hash: vec![],
            }),
            end: Some(BlockId {
                height: 10,
                hash: vec![],
            }),
            pool_types: vec![PoolType::Invalid as i32],
        };
        let status = streamer
            .get_block_range_nullifiers(Request::new(request))
            .await
            .err()
            .unwrap();
        assert_eq!(status.code(), Code::InvalidArgument);
    }
}
