//! Typed deserialization targets for the zebrad JSON-RPC responses we consume.
//!
//! Field names use zebrad's lowercase JSON keys.

use std::collections::HashMap;

use serde::Deserialize;

/// Response of the `getinfo` RPC (only the fields we use).
#[derive(Debug, Deserialize)]
pub struct GetInfo {
    /// Node build string, e.g. `v2.4.0`.
    pub build: String,
    /// Node subversion string, e.g. `/MagicBean:5.10.0/`.
    pub subversion: String,
}

/// Response of the `getblockchaininfo` RPC (only the fields we use).
#[derive(Debug, Deserialize)]
pub struct GetBlockchainInfo {
    /// Network name: `main`, `test`, or `regtest`.
    pub chain: String,
    /// Height of the best chain tip.
    pub blocks: u64,
    /// Hash of the best chain tip, big-endian hex (display order).
    pub bestblockhash: String,
    /// Estimated height of the chain; may exceed `blocks` while syncing.
    #[serde(default)]
    pub estimatedheight: u64,
    /// Consensus branch IDs for the current and next block.
    pub consensus: Consensus,
    /// Network upgrades keyed by branch ID.
    #[serde(default)]
    pub upgrades: HashMap<String, Upgrade>,
}

/// Consensus branch IDs reported by `getblockchaininfo`.
#[derive(Debug, Deserialize)]
pub struct Consensus {
    /// Branch ID in effect at the chain tip.
    pub chaintip: String,
}

/// A single network upgrade entry from `getblockchaininfo`.
#[derive(Debug, Deserialize)]
pub struct Upgrade {
    /// Upgrade name, e.g. `Sapling`, `Orchard`.
    pub name: String,
    /// Height at which the upgrade activates.
    pub activationheight: u64,
}

/// Response of the verbose (`verbosity = 1`) `getblock` RPC (only the fields we use).
#[derive(Debug, Deserialize)]
pub struct GetBlockVerbose {
    /// Block hash, big-endian hex (display order).
    pub hash: String,
    /// Note-commitment tree sizes as of this block.
    #[serde(default)]
    pub trees: Trees,
}

/// Note-commitment tree sizes reported by verbose `getblock`.
#[derive(Debug, Default, Deserialize)]
pub struct Trees {
    /// Sapling tree.
    #[serde(default)]
    pub sapling: TreeSize,
    /// Orchard tree.
    #[serde(default)]
    pub orchard: TreeSize,
}

/// The `size` of a note-commitment tree.
#[derive(Debug, Default, Deserialize)]
pub struct TreeSize {
    /// Number of leaves in the tree as of the end of this block.
    #[serde(default)]
    pub size: u32,
}

/// Response of the verbose (`verbosity = 1`) `getrawtransaction` RPC (only the fields we use).
#[derive(Debug, Deserialize)]
pub struct GetRawTransaction {
    /// The raw transaction, hex-encoded.
    pub hex: String,
    /// Block height the tx was mined at; `-1` if it is in the index but not on the main chain, and
    /// absent (defaulting to `0`) for a mempool transaction.
    #[serde(default)]
    pub height: i64,
}

/// Response of the `z_gettreestate` RPC (only the fields we use).
#[derive(Debug, Deserialize)]
pub struct GetTreeState {
    /// Block hash, big-endian hex (display order).
    pub hash: String,
    /// Block height.
    pub height: u64,
    /// Unix epoch time the block was mined.
    pub time: u32,
    /// Sapling note-commitment tree.
    #[serde(default)]
    pub sapling: TreePool,
    /// Orchard note-commitment tree.
    #[serde(default)]
    pub orchard: TreePool,
}

/// A shielded pool's tree state inside `z_gettreestate`.
#[derive(Debug, Default, Deserialize)]
pub struct TreePool {
    /// Commitment tree data.
    #[serde(default)]
    pub commitments: TreeCommitments,
}

/// The commitment tree data of a shielded pool.
#[derive(Debug, Default, Deserialize)]
pub struct TreeCommitments {
    /// Hex-encoded serialized commitment tree as of this block.
    #[serde(default, rename = "finalState")]
    pub final_state: String,
}
