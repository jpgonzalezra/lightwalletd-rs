//! Chain-info methods: `GetLatestBlock` and `GetLightdInfo`.

use tonic::{Response, Status};

use crate::encoding;
use crate::proto::{BlockId, LightdInfo};

use super::Streamer;

pub(super) async fn get_latest_block(streamer: &Streamer) -> Result<Response<BlockId>, Status> {
    let info = streamer.node.get_blockchain_info().await?;
    let hash = encoding::display_hex_to_wire(&info.bestblockhash)
        .map_err(|e| Status::internal(format!("decoding best block hash: {e}")))?;
    Ok(Response::new(BlockId {
        height: info.blocks,
        hash,
    }))
}

/// Consensus branch ID of the Sapling upgrade. The `upgrades` map is keyed by branch ID, which is
/// stable across node versions — unlike the human-readable name — so the activation height is looked
/// up by this key. Absent on regtest, where it defaults to 0.
const SAPLING_BRANCH_ID: &str = "76b809bb";

/// Version of [lightwallet-protocol] this server implements, reported in `LightdInfo`. It is a
/// constant rather than a build stamp: it describes the protocol served, not the binary. Clients
/// read it to decide whether they may request non-default `poolTypes`, so it tracks the vendored
/// `proto/` set and moves only once the server actually serves everything the named version
/// specifies.
///
/// [lightwallet-protocol]: https://github.com/zcash/lightwallet-protocol
const LIGHTWALLET_PROTOCOL_VERSION: &str = "v0.5.0";

pub(super) async fn get_lightd_info(streamer: &Streamer) -> Result<Response<LightdInfo>, Status> {
    let node_info = streamer.node.get_info().await?;
    let chain = streamer.node.get_blockchain_info().await?;

    let sapling_activation_height = chain
        .upgrades
        .get(SAPLING_BRANCH_ID)
        .map(|upgrade| upgrade.activationheight)
        .unwrap_or(0);

    // The next pending upgrade, by lowest activation height; ("", 0) when none is pending.
    let (upgrade_name, upgrade_height) = chain
        .upgrades
        .values()
        .filter(|upgrade| upgrade.status == "pending")
        .min_by_key(|upgrade| upgrade.activationheight)
        .map(|upgrade| (upgrade.name.clone(), upgrade.activationheight))
        .unwrap_or_default();

    let info = LightdInfo {
        version: env!("CARGO_PKG_VERSION").to_string(),
        vendor: "lightwalletd-rs".to_string(),
        git_commit: env!("GIT_COMMIT").to_string(),
        taddr_support: true,
        chain_name: chain.chain,
        sapling_activation_height,
        consensus_branch_id: chain.consensus.chaintip,
        block_height: chain.blocks,
        estimated_height: chain.estimatedheight,
        zcashd_build: node_info.build,
        zcashd_subversion: node_info.subversion,
        donation_address: streamer.donation_address.clone().unwrap_or_default(),
        upgrade_name,
        upgrade_height,
        lightwallet_protocol_version: LIGHTWALLET_PROTOCOL_VERSION.to_string(),
        ..Default::default()
    };
    Ok(Response::new(info))
}
