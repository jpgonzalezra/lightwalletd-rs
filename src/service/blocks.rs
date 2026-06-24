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
    let (Some(start), Some(end)) = (range.start, range.end) else {
        return Err(Status::invalid_argument(
            "get_block_range: must specify start and end heights",
        ));
    };
    let (start, end) = (start.height, end.height);
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
    let (Some(start), Some(end)) = (range.start, range.end) else {
        return Err(Status::invalid_argument(
            "get_block_range_nullifiers: must specify start and end heights",
        ));
    };
    let (start, end) = (start.height, end.height);
    let stream = block_range_stream(
        streamer.cache.clone(),
        streamer.node.clone(),
        start,
        end,
        filter::nullifiers_only,
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
