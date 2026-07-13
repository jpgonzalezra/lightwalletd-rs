//! Block-serving methods: `GetBlock`, `GetBlockNullifiers`, `GetBlockRange`, and
//! `GetBlockRangeNullifiers`.

use std::sync::Arc;

use async_stream::try_stream;
use tonic::{Request, Response, Status};

use crate::cache::Cache;
use crate::filter;
use crate::node::NodeRpc;
use crate::proto::{BlockId, BlockRange, BoxStream, CompactBlock};

use super::{Streamer, block_at};

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
    // pools are otherwise honored (transparent is always dropped — see
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
        start,
        end,
        move |block| filter::filter_block_to_pools_nullifiers_only(block, &pool_types),
    );
    Ok(Response::new(stream))
}

/// Stream the blocks in the range (ascending if `start <= end`, otherwise descending), reading each
/// from the cache or the node and applying `transform` before yielding it. Shared by `GetBlockRange`
/// and `GetBlockRangeNullifiers`, which differ only in that final transform.
fn block_range_stream(
    cache: Arc<Cache>,
    node: Arc<dyn NodeRpc>,
    start: u64,
    end: u64,
    transform: impl Fn(CompactBlock) -> CompactBlock + Send + 'static,
) -> BoxStream<CompactBlock> {
    Box::pin(try_stream! {
        let heights: Vec<u64> = if start <= end {
            (start..=end).collect()
        } else {
            (end..=start).rev().collect()
        };
        for height in heights {
            let block = block_at(&cache, node.as_ref(), height).await?;
            yield transform(block);
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
