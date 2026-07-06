//! Subtree-roots method: `GetSubtreeRoots`.

use async_stream::try_stream;
use tonic::{Request, Response, Status};

use crate::encoding;
use crate::proto::{BoxStream, GetSubtreeRootsArg, ShieldedProtocol, SubtreeRoot};

use super::{Streamer, block_at, decode_hex};

pub(super) async fn get_subtree_roots(
    streamer: &Streamer,
    request: Request<GetSubtreeRootsArg>,
) -> Result<Response<BoxStream<SubtreeRoot>>, Status> {
    let arg = request.into_inner();
    let protocol = match ShieldedProtocol::try_from(arg.shielded_protocol) {
        Ok(ShieldedProtocol::Sapling) => "sapling",
        Ok(ShieldedProtocol::Orchard) => "orchard",
        Ok(ShieldedProtocol::Ironwood) => "ironwood",
        Err(_) => return Err(Status::invalid_argument("unrecognized shielded protocol")),
    };
    // In darkside mode the roots are staged complete (with their completing block already set),
    // so they are served verbatim rather than computed from the cached blocks.
    if let Some(state) = &streamer.darkside {
        let roots = state.lock().await.subtree_roots_for(
            arg.shielded_protocol,
            arg.start_index,
            arg.max_entries,
        );
        let stream = tokio_stream::iter(roots.into_iter().map(Ok::<_, Status>));
        return Ok(Response::new(Box::pin(stream)));
    }
    let subtrees = streamer
        .node
        .get_subtrees(protocol, arg.start_index, arg.max_entries)
        .await?;
    let node = streamer.node.clone();
    let cache = streamer.cache.clone();

    let stream = try_stream! {
        for subtree in subtrees.subtrees {
            let block = block_at(&cache, node.as_ref(), subtree.end_height).await?;
            let root_hash = decode_hex(&subtree.root, "subtree root")?;
            // The block hash is in protocol order; upstream sends it display-order here.
            let completing_block_hash = encoding::wire_to_display_bytes(&block.hash);
            yield SubtreeRoot {
                root_hash,
                completing_block_hash,
                completing_block_height: block.height,
            };
        }
    };
    Ok(Response::new(Box::pin(stream)))
}
