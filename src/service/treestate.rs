//! Tree-state methods: `GetTreeState` and `GetLatestTreeState`.

use tonic::{Request, Response, Status};

use crate::node;
use crate::proto::{BlockId, TreeState};

use super::Streamer;

pub(super) async fn get_tree_state(
    streamer: &Streamer,
    request: Request<BlockId>,
) -> Result<Response<TreeState>, Status> {
    let block_id = request.into_inner();
    if !block_id.hash.is_empty() {
        return Err(Status::unimplemented(
            "get_tree_state by hash is not yet supported",
        ));
    }
    let tree_state = streamer
        .node
        .get_treestate(&block_id.height.to_string())
        .await?;
    Ok(Response::new(node_tree_state_to_proto(
        &streamer.network,
        tree_state,
    )))
}

pub(super) async fn get_latest_tree_state(
    streamer: &Streamer,
) -> Result<Response<TreeState>, Status> {
    let chain_info = streamer.node.get_blockchain_info().await?;
    let tree_state = streamer
        .node
        .get_treestate(&chain_info.blocks.to_string())
        .await?;
    Ok(Response::new(node_tree_state_to_proto(
        &streamer.network,
        tree_state,
    )))
}

/// Build the gRPC `TreeState` from a node `z_gettreestate` response and the network name.
pub(super) fn node_tree_state_to_proto(network: &str, tree_state: node::GetTreeState) -> TreeState {
    TreeState {
        network: network.to_string(),
        height: tree_state.height,
        hash: tree_state.hash,
        time: tree_state.time,
        sapling_tree: tree_state.sapling.commitments.final_state,
        orchard_tree: tree_state.orchard.commitments.final_state,
    }
}
