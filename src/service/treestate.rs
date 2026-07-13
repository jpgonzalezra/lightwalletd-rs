//! Tree-state methods: `GetTreeState` and `GetLatestTreeState`.

use tonic::{Request, Response, Status};

use crate::encoding;
use crate::node;
use crate::proto::{BlockId, TreeState};

use super::Streamer;

/// A `BlockID`'s hash is a fixed 32-byte protocol-order (little-endian) block hash — same width as
/// every other hash field on the wire (`TxFilter.hash`, `SubtreeRoot.completing_block_hash`, ...).
const HASH_LEN: usize = 32;

pub(super) async fn get_tree_state(
    streamer: &Streamer,
    request: Request<BlockId>,
) -> Result<Response<TreeState>, Status> {
    let block_id = request.into_inner();
    let id = treestate_id(&block_id)?;
    let tree_state = streamer.node.get_treestate(&id).await?;
    Ok(Response::new(node_tree_state_to_proto(
        &streamer.network,
        tree_state,
    )?))
}

/// Resolve a `BlockID` to the opaque id string `z_gettreestate` expects: a decimal height, or a
/// display-order (big-endian) hex block hash.
///
/// Height takes precedence when both are set, matching the Go reference (`frontend/service.go`
/// `GetTreeState`: `if id.Height > 0 { ... } else { use id.Hash }`).
///
/// Go's `GetTreeState` also retries the RPC in a loop, walking `z_gettreestate`'s `SkipHash` field
/// back through the chain until it lands on a block with a non-empty Sapling tree. That loop is a
/// zcashd-only affordance: `z_gettreestate`'s `SkipHash` lets a caller cheaply find the first
/// post-Sapling-activation block without walking heights one at a time. zebrad's `z_gettreestate`
/// response has no `SkipHash` field (see `zebra-rpc` `trees.rs`) — it always answers directly for the
/// requested height or hash — so there is nothing to walk here, and the loop is intentionally not
/// replicated. `node_tree_state_to_proto` still rejects a pre-Sapling response's empty frontier with
/// `InvalidArgument`, matching Go's end state for that case.
fn treestate_id(block_id: &BlockId) -> Result<String, Status> {
    if block_id.height == 0 && block_id.hash.is_empty() {
        return Err(Status::invalid_argument(
            "get_tree_state: must specify a block height or hash",
        ));
    }
    if block_id.height > 0 {
        return Ok(block_id.height.to_string());
    }
    if block_id.hash.len() != HASH_LEN {
        return Err(Status::invalid_argument(format!(
            "get_tree_state: block hash has invalid length: {}",
            block_id.hash.len()
        )));
    }
    // BlockID.hash is wire order (little-endian) on the gRPC wire; z_gettreestate, like every other
    // node RPC that takes a hash, wants display-order (big-endian) hex.
    Ok(encoding::wire_to_display_hex(&block_id.hash))
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
    )?))
}

/// Build the gRPC `TreeState` from a node `z_gettreestate` response and the network name. A response
/// with an empty frontier for every pool — a height before Sapling activation — is rejected with
/// `InvalidArgument` rather than returned as a malformed, empty `TreeState`. An empty Ironwood
/// frontier alone is normal (every height before NU6.3 activation) and maps to an empty string.
pub(super) fn node_tree_state_to_proto(
    network: &str,
    tree_state: node::GetTreeState,
) -> Result<TreeState, Status> {
    let sapling_tree = tree_state.sapling.commitments.final_state;
    let orchard_tree = tree_state.orchard.commitments.final_state;
    let ironwood_tree = tree_state.ironwood.commitments.final_state;
    if sapling_tree.is_empty() && orchard_tree.is_empty() && ironwood_tree.is_empty() {
        return Err(Status::invalid_argument(format!(
            "get_tree_state: no tree state at height {} (before Sapling activation?)",
            tree_state.height
        )));
    }
    Ok(TreeState {
        network: network.to_string(),
        height: tree_state.height,
        hash: tree_state.hash,
        time: tree_state.time,
        sapling_tree,
        orchard_tree,
        ironwood_tree,
    })
}
