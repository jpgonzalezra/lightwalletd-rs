//! The `readstate` backend: serve reads from a co-located zebrad's state, in-process.
//!
//! [`ZebraStateNode`] is a second [`NodeRpc`] implementation (ADR 0023). Reads go to zebra's
//! [`ReadStateService`] — a read-only secondary RocksDB instance kept at the true chain tip by
//! `zebra-rpc`'s `TrustedChainSync` over the zebrad indexer gRPC — while transaction submission,
//! the mempool, and `getinfo` stay on the JSON-RPC client (the node-only surfaces).
//!
//! Absent blocks/transactions are reported with the same JSON-RPC error codes zebrad would use
//! (`-8` / `-5`), so the per-method-family gRPC status mapping (ADR 0010) applies unchanged.
//!
//! The mapping layer is generic over `S: tower::Service<ReadRequest>` and a [`TipSource`], so unit
//! tests drive it with a scripted service and a fixed tip; production instantiates it with
//! [`ReadStateService`] and [`LatestChainTip`].

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use hex::ToHex;
use tower::{Service, ServiceExt};
use zebra_chain::block::{self, Block};
use zebra_chain::parameters::{Network, NetworkUpgrade};
use zebra_chain::serialization::ZcashSerialize;
use zebra_chain::subtree::NoteCommitmentSubtreeIndex;
use zebra_chain::transparent;
use zebra_state::{HashOrHeight, ReadRequest, ReadResponse};

use crate::node::{
    AddressUtxo, Consensus, GetAddressBalance, GetBlockVerbose, GetBlockchainInfo, GetInfo,
    GetRawTransaction, GetSubtrees, GetTreeState, NodeClient, NodeError, NodeRpc, Subtree,
    TreeCommitments, TreePool, TreeSize, Trees, Upgrade,
};

/// JSON-RPC error code zebrad uses for "block not found / out of range".
const RPC_BLOCK_NOT_FOUND: i64 = -8;
/// JSON-RPC error code zebrad uses for "transaction not found" and invalid addresses.
const RPC_MISC_NOT_FOUND: i64 = -5;

/// The best chain tip, abstracted so tests can pin it. Implemented for
/// [`zebra_state::LatestChainTip`] in production.
pub trait TipSource: Send + Sync {
    /// The best tip as `(height, display-order hex hash)`, or `None` before the first tip.
    fn best_tip(&self) -> Option<(u64, String)>;
}

impl TipSource for zebra_state::LatestChainTip {
    fn best_tip(&self) -> Option<(u64, String)> {
        use zebra_chain::chain_tip::ChainTip;
        // One atomic read: two separate height/hash reads could straddle a tip advance and pair
        // height N with the hash of N+1 — wallet-visible via GetLatestBlock.
        let (height, hash) = self.best_tip_height_and_hash()?;
        Some((height.0 as u64, hash.to_string()))
    }
}

/// A [`NodeRpc`] implementation over zebra's read state (reads) and JSON-RPC (node-only surfaces).
pub struct ZebraStateNode<S, T> {
    read_state: S,
    tip: T,
    rpc: NodeClient,
    network: Network,
}

impl<S, T> ZebraStateNode<S, T>
where
    S: Service<ReadRequest, Response = ReadResponse, Error = zebra_state::BoxError>
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send,
    T: TipSource,
{
    /// Build the backend from its parts. `rpc` keeps serving the node-only surfaces.
    pub fn new(read_state: S, tip: T, rpc: NodeClient, network: Network) -> Self {
        Self {
            read_state,
            tip,
            rpc,
            network,
        }
    }

    /// Issue one read request against the state service.
    async fn read(&self, request: ReadRequest) -> Result<ReadResponse, NodeError> {
        self.read_state
            .clone()
            .ready()
            .await
            .map_err(NodeError::State)?
            .call(request)
            .await
            .map_err(NodeError::State)
    }

    /// The best tip, or a synthesized "not ready" error before the state has one.
    fn best_tip(&self) -> Result<(u64, String), NodeError> {
        self.tip
            .best_tip()
            .ok_or_else(|| NodeError::State("read state has no chain tip yet".into()))
    }

    /// Fetch the block at `hash_or_height`, or the zebrad-compatible `-8` error when absent.
    async fn block(&self, id: HashOrHeight) -> Result<Arc<Block>, NodeError> {
        match self.read(ReadRequest::Block(id)).await? {
            ReadResponse::Block(Some(block)) => Ok(block),
            ReadResponse::Block(None) => Err(NodeError::Rpc {
                code: RPC_BLOCK_NOT_FOUND,
                message: "Block not found".to_string(),
            }),
            other => Err(unexpected(&other)),
        }
    }

    /// The note-commitment tree sizes as of `hash_or_height` (0 for a pool that has no tree yet).
    async fn tree_sizes(&self, id: HashOrHeight) -> Result<Trees, NodeError> {
        let sapling = match self.read(ReadRequest::SaplingTree(id)).await? {
            ReadResponse::SaplingTree(tree) => tree.map(|t| t.count()).unwrap_or(0),
            other => return Err(unexpected(&other)),
        };
        let orchard = match self.read(ReadRequest::OrchardTree(id)).await? {
            ReadResponse::OrchardTree(tree) => tree.map(|t| t.count()).unwrap_or(0),
            other => return Err(unexpected(&other)),
        };
        let ironwood = match self.read(ReadRequest::IronwoodTree(id)).await? {
            ReadResponse::IronwoodTree(tree) => tree.map(|t| t.count()).unwrap_or(0),
            other => return Err(unexpected(&other)),
        };
        Ok(Trees {
            sapling: TreeSize {
                size: sapling as u32,
            },
            orchard: TreeSize {
                size: orchard as u32,
            },
            ironwood: TreeSize {
                size: ironwood as u32,
            },
        })
    }

    /// Resolve a `z_gettreestate`-style id (decimal height or display-order hex hash).
    fn parse_hash_or_height(id: &str) -> Result<HashOrHeight, NodeError> {
        id.parse::<HashOrHeight>().map_err(|_| NodeError::Rpc {
            code: RPC_BLOCK_NOT_FOUND,
            message: format!("invalid block id: {id}"),
        })
    }

    /// Parse transparent addresses, reporting the zcashd-compatible `-5` on a bad one.
    fn parse_addresses(
        &self,
        addresses: &[String],
    ) -> Result<HashSet<transparent::Address>, NodeError> {
        addresses
            .iter()
            .map(|address| {
                address
                    .parse::<transparent::Address>()
                    .map_err(|_| NodeError::Rpc {
                        code: RPC_MISC_NOT_FOUND,
                        message: format!("Invalid address: {address}"),
                    })
            })
            .collect()
    }
}

/// A response variant the request cannot produce — a bug in the mapping, not a node condition.
fn unexpected(response: &ReadResponse) -> NodeError {
    NodeError::State(format!("unexpected read response: {response:?}").into())
}

/// A wire height (u64) in zebra's height domain, or `None` when it exceeds
/// [`block::Height::MAX`] and therefore cannot exist in the state — callers must not let such a
/// height wrap around an `as u32` cast into some other block's height.
fn state_height(height: u64) -> Option<block::Height> {
    u32::try_from(height)
        .ok()
        .map(block::Height)
        .filter(|height| *height <= block::Height::MAX)
}

/// The branded upgrade name zebrad reports (`"NU6.3"`, not the Rust identifier `Nu6_3`): zebra
/// keeps that branding in `NetworkUpgrade`'s serde rename table, so round-trip through serde
/// rather than `Display` (which is the debug form). Verified by the live parity sweep.
fn upgrade_name(upgrade: NetworkUpgrade) -> String {
    serde_json::to_value(upgrade)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_else(|| format!("{upgrade}"))
}

/// Whether `upgrade` is active at `height` — gates which pools `get_treestate` includes, matching
/// zebrad's `z_gettreestate` (which omits a pool entirely before its activation instead of
/// serializing an empty frontier). Verified by the live parity sweep.
fn pool_active(network: &Network, upgrade: NetworkUpgrade, height: block::Height) -> bool {
    upgrade
        .activation_height(network)
        .is_some_and(|activation| height >= activation)
}

/// The chain name zcashd/zebrad report for `network` (`main`/`test`/`regtest`).
fn chain_name(network: &Network) -> String {
    match network {
        Network::Mainnet => "main".to_string(),
        Network::Testnet(params) if params.is_default_testnet() => "test".to_string(),
        Network::Testnet(params) if params.is_regtest() => "regtest".to_string(),
        Network::Testnet(_) => "test".to_string(),
    }
}

#[async_trait::async_trait]
impl<S, T> NodeRpc for ZebraStateNode<S, T>
where
    S: Service<ReadRequest, Response = ReadResponse, Error = zebra_state::BoxError>
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send,
    T: TipSource,
{
    async fn get_info(&self) -> Result<GetInfo, NodeError> {
        self.rpc.get_info().await
    }

    async fn get_blockchain_info(&self) -> Result<GetBlockchainInfo, NodeError> {
        let (blocks, bestblockhash) = self.best_tip()?;

        // The upgrade table comes from the network parameters, not the node: identical content,
        // no RPC round-trip. Branch IDs key the map, exactly like zebrad's JSON.
        let mut upgrades = HashMap::new();
        for (height, upgrade) in self.network.full_activation_list() {
            let Some(branch_id) = upgrade.branch_id() else {
                continue; // pre-Overwinter upgrades have no branch id and aren't listed
            };
            upgrades.insert(
                branch_id.encode_hex::<String>(),
                Upgrade {
                    name: upgrade_name(upgrade),
                    activationheight: height.0 as u64,
                    status: if blocks >= height.0 as u64 {
                        "active".to_string()
                    } else {
                        "pending".to_string()
                    },
                },
            );
        }
        let tip_height = state_height(blocks).unwrap_or(block::Height::MAX);
        let chaintip = NetworkUpgrade::current(&self.network, tip_height)
            .branch_id()
            .map(|id| id.encode_hex::<String>())
            .unwrap_or_default();

        Ok(GetBlockchainInfo {
            chain: chain_name(&self.network),
            blocks,
            bestblockhash,
            estimatedheight: blocks,
            consensus: Consensus { chaintip },
            upgrades,
        })
    }

    async fn get_block_verbose(&self, height: u64) -> Result<GetBlockVerbose, NodeError> {
        // A height past the representable range cannot exist: same -8 as an absent block.
        let height = state_height(height).ok_or_else(|| NodeError::Rpc {
            code: RPC_BLOCK_NOT_FOUND,
            message: "Block not found".to_string(),
        })?;
        let id = HashOrHeight::Height(height);
        let block = self.block(id).await?;
        let hash = block.hash().to_string();

        let tx = match self.read(ReadRequest::TransactionIdsForBlock(id)).await? {
            ReadResponse::TransactionIdsForBlock(Some(ids)) => {
                ids.iter().map(|txid| txid.to_string()).collect()
            }
            ReadResponse::TransactionIdsForBlock(None) => Vec::new(),
            other => return Err(unexpected(&other)),
        };
        let trees = self.tree_sizes(id).await?;

        Ok(GetBlockVerbose { hash, tx, trees })
    }

    async fn get_block_count(&self) -> Result<u64, NodeError> {
        Ok(self.best_tip()?.0)
    }

    async fn get_block_raw(&self, hash: &str) -> Result<Vec<u8>, NodeError> {
        let id = Self::parse_hash_or_height(hash)?;
        let block = self.block(id).await?;
        block
            .zcash_serialize_to_vec()
            .map_err(|error| NodeError::State(format!("serializing block: {error}").into()))
    }

    async fn get_raw_transaction(&self, txid: &str) -> Result<GetRawTransaction, NodeError> {
        let hash: zebra_chain::transaction::Hash = txid.parse().map_err(|_| NodeError::Rpc {
            code: RPC_MISC_NOT_FOUND,
            message: format!("invalid txid: {txid}"),
        })?;
        match self.read(ReadRequest::Transaction(hash)).await? {
            ReadResponse::Transaction(Some(mined)) => {
                let hex = mined
                    .tx
                    .zcash_serialize_to_vec()
                    .map_err(|error| NodeError::State(format!("serializing tx: {error}").into()))?;
                Ok(GetRawTransaction {
                    hex: hex::encode(hex),
                    height: mined.height.0 as i64,
                })
            }
            // Not mined: fall through to the node, which still knows mempool transactions.
            ReadResponse::Transaction(None) => self.rpc.get_raw_transaction(txid).await,
            other => Err(unexpected(&other)),
        }
    }

    async fn send_raw_transaction(&self, hex: &str) -> Result<String, NodeError> {
        self.rpc.send_raw_transaction(hex).await
    }

    async fn get_treestate(&self, id: &str) -> Result<GetTreeState, NodeError> {
        let id = Self::parse_hash_or_height(id)?;
        let block = self.block(id).await?;
        let height = block
            .coinbase_height()
            .ok_or_else(|| NodeError::State("block has no coinbase height".into()))?;
        let hash = block.hash();

        let sapling = match self.read(ReadRequest::SaplingTree(id)).await? {
            ReadResponse::SaplingTree(tree) => tree,
            other => return Err(unexpected(&other)),
        };
        let orchard = match self.read(ReadRequest::OrchardTree(id)).await? {
            ReadResponse::OrchardTree(tree) => tree,
            other => return Err(unexpected(&other)),
        };
        let ironwood = match self.read(ReadRequest::IronwoodTree(id)).await? {
            ReadResponse::IronwoodTree(tree) => tree,
            other => return Err(unexpected(&other)),
        };

        // zebrad's `z_gettreestate` omits a pool entirely before its activation upgrade rather
        // than serializing an empty frontier ("000000"), and the rpc backend inherits that; gate
        // each pool the same way so both backends are wire-identical at every height.
        let sapling = pool_active(&self.network, NetworkUpgrade::Sapling, height)
            .then(|| sapling.map(|tree| hex::encode(tree.to_rpc_bytes())))
            .flatten()
            .unwrap_or_default();
        let orchard = pool_active(&self.network, NetworkUpgrade::Nu5, height)
            .then(|| orchard.map(|tree| hex::encode(tree.to_rpc_bytes())))
            .flatten()
            .unwrap_or_default();
        let ironwood = pool_active(&self.network, NetworkUpgrade::Nu6_3, height)
            .then(|| ironwood.map(|tree| hex::encode(tree.to_rpc_bytes())))
            .flatten()
            .unwrap_or_default();

        Ok(GetTreeState {
            hash: hash.to_string(),
            height: height.0 as u64,
            time: block.header.time.timestamp() as u32,
            sapling: TreePool {
                commitments: TreeCommitments {
                    final_state: sapling,
                },
            },
            orchard: TreePool {
                commitments: TreeCommitments {
                    final_state: orchard,
                },
            },
            ironwood: TreePool {
                commitments: TreeCommitments {
                    final_state: ironwood,
                },
            },
        })
    }

    async fn get_address_balance(
        &self,
        addresses: &[String],
    ) -> Result<GetAddressBalance, NodeError> {
        let addresses = self.parse_addresses(addresses)?;
        match self.read(ReadRequest::AddressBalance(addresses)).await? {
            ReadResponse::AddressBalance { balance, .. } => Ok(GetAddressBalance {
                balance: i64::from(balance),
            }),
            other => Err(unexpected(&other)),
        }
    }

    async fn get_address_utxos(&self, addresses: &[String]) -> Result<Vec<AddressUtxo>, NodeError> {
        let parsed = self.parse_addresses(addresses)?;
        match self.read(ReadRequest::UtxosByAddresses(parsed)).await? {
            ReadResponse::AddressUtxos(utxos) => Ok(utxos
                .utxos()
                .map(|(address, txid, location, output)| AddressUtxo {
                    address: address.to_string(),
                    txid: txid.to_string(),
                    output_index: i64::from(location.output_index().index()),
                    script: hex::encode(output.lock_script.as_raw_bytes()),
                    satoshis: u64::from(output.value()),
                    height: location.height().0 as u64,
                })
                .collect()),
            other => Err(unexpected(&other)),
        }
    }

    async fn get_address_txids(
        &self,
        addresses: &[String],
        start: u64,
        end: u64,
    ) -> Result<Vec<String>, NodeError> {
        let parsed = self.parse_addresses(addresses)?;
        let (tip, _) = self.best_tip()?;
        // Replicate zebrad's `build_height_range` (zebra-rpc): both bounds clamp to the chain tip
        // (an `end` of 0 means the tip), and a start past the clamped end is the same error zebrad
        // returns verbatim — not a silently-empty inverted state read. The `-1` code is zebrad's
        // wire code for this case (verified against a live node), not the JSON-RPC spec's -32602.
        let tip = state_height(tip).unwrap_or(block::Height::MAX);
        let start = state_height(start).map_or(tip, |height| height.min(tip));
        let end = if end == 0 {
            tip
        } else {
            state_height(end).map_or(tip, |height| height.min(tip))
        };
        if start > end {
            return Err(NodeError::Rpc {
                code: -1,
                message: format!("start {start:?} must be less than or equal to end {end:?}"),
            });
        }
        match self
            .read(ReadRequest::TransactionIdsByAddresses {
                addresses: parsed,
                height_range: start..=end,
            })
            .await?
        {
            ReadResponse::AddressesTransactionIds(ids) => {
                Ok(ids.values().map(|txid| txid.to_string()).collect())
            }
            other => Err(unexpected(&other)),
        }
    }

    async fn get_subtrees(
        &self,
        protocol: &str,
        start_index: u32,
        max_entries: u32,
    ) -> Result<GetSubtrees, NodeError> {
        // Subtree indexes are u16 in zebra's state, so a start past u16::MAX cannot match any
        // stored subtree: answer with the empty set instead of silently wrapping around and
        // serving the wrong subtrees.
        let Ok(start_index) = u16::try_from(start_index) else {
            return Ok(GetSubtrees {
                subtrees: Vec::new(),
            });
        };
        let start_index = NoteCommitmentSubtreeIndex(start_index);
        // Likewise a max_entries beyond u16::MAX cannot bound a u16-indexed range: clamp instead
        // of truncating (65536 must not become a limit of 0 == an empty response).
        let limit = (max_entries > 0)
            .then(|| u16::try_from(max_entries).unwrap_or(u16::MAX))
            .map(NoteCommitmentSubtreeIndex);
        let subtrees = match protocol {
            "sapling" => match self
                .read(ReadRequest::SaplingSubtrees { start_index, limit })
                .await?
            {
                ReadResponse::SaplingSubtrees(map) => map
                    .into_values()
                    .map(|data| Subtree {
                        root: hex::encode(data.root.to_bytes()),
                        end_height: data.end_height.0 as u64,
                    })
                    .collect(),
                other => return Err(unexpected(&other)),
            },
            "orchard" => match self
                .read(ReadRequest::OrchardSubtrees { start_index, limit })
                .await?
            {
                ReadResponse::OrchardSubtrees(map) => map
                    .into_values()
                    .map(|data| Subtree {
                        root: data.root.encode_hex::<String>(),
                        end_height: data.end_height.0 as u64,
                    })
                    .collect(),
                other => return Err(unexpected(&other)),
            },
            "ironwood" => match self
                .read(ReadRequest::IronwoodSubtrees { start_index, limit })
                .await?
            {
                ReadResponse::IronwoodSubtrees(map) => map
                    .into_values()
                    .map(|data| Subtree {
                        root: data.root.encode_hex::<String>(),
                        end_height: data.end_height.0 as u64,
                    })
                    .collect(),
                other => return Err(unexpected(&other)),
            },
            other => {
                // Same error shape zebrad produces, so the pre-NU6.3 empty-stream handling in
                // the subtrees service (which matches on this message) behaves identically.
                return Err(NodeError::Rpc {
                    code: -1,
                    message: format!(
                        "invalid pool name, must be one of: [\"sapling\", \"orchard\", \"ironwood\"], got: {other}"
                    ),
                });
            }
        };
        Ok(GetSubtrees { subtrees })
    }

    async fn get_raw_mempool(&self) -> Result<Vec<String>, NodeError> {
        self.rpc.get_raw_mempool().await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::task::{Context, Poll};

    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};
    use zebra_chain::orchard::tree::NoteCommitmentTree as OrchardTree;
    use zebra_chain::sapling::tree::NoteCommitmentTree as SaplingTree;
    use zebra_chain::serialization::ZcashDeserialize;
    use zebra_chain::transaction;
    use zebra_state::MinedTx;

    use super::*;
    use crate::config::NodeConfig;

    /// A [`TipSource`] pinned to a fixed value, for tests.
    struct FixedTip(Option<(u64, String)>);

    impl TipSource for FixedTip {
        fn best_tip(&self) -> Option<(u64, String)> {
            self.0.clone()
        }
    }

    /// A scripted [`tower::Service<ReadRequest>`] double: pops queued [`ReadResponse`]s in call
    /// order and records every [`ReadRequest`] it receives, so a test can both drive the mapping
    /// layer's inputs and assert what it asked the state for.
    #[derive(Clone)]
    struct ScriptedReadState {
        responses: Arc<Mutex<VecDeque<ReadResponse>>>,
        requests: Arc<Mutex<Vec<ReadRequest>>>,
    }

    impl ScriptedReadState {
        fn new(responses: Vec<ReadResponse>) -> Self {
            Self {
                responses: Arc::new(Mutex::new(responses.into_iter().collect())),
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }

        /// Every request received so far, in call order.
        fn requests(&self) -> Vec<ReadRequest> {
            self.requests.lock().unwrap().clone()
        }
    }

    impl Service<ReadRequest> for ScriptedReadState {
        type Response = ReadResponse;
        type Error = zebra_state::BoxError;
        type Future = std::future::Ready<Result<ReadResponse, zebra_state::BoxError>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: ReadRequest) -> Self::Future {
            self.requests.lock().unwrap().push(request.clone());
            let response = self.responses.lock().unwrap().pop_front();
            std::future::ready(response.ok_or_else(|| -> zebra_state::BoxError {
                format!("ScriptedReadState: no queued response left for {request:?}").into()
            }))
        }
    }

    /// A [`NodeClient`] pointed at an address nothing listens on, so any test that accidentally
    /// falls through to RPC fails loudly (connection refused) instead of quietly hitting a real
    /// node.
    fn unreachable_rpc() -> NodeClient {
        NodeClient::new(&NodeConfig {
            url: "http://127.0.0.1:1".to_string(),
            user: String::new(),
            password: String::new(),
        })
        .unwrap()
    }

    /// Build a [`ZebraStateNode`] over a scripted read state and a fixed tip, returning the
    /// scripted service too so a test can inspect the requests it received.
    fn node_with(
        responses: Vec<ReadResponse>,
        tip: Option<(u64, String)>,
    ) -> (
        ZebraStateNode<ScriptedReadState, FixedTip>,
        ScriptedReadState,
    ) {
        node_with_rpc(responses, tip, unreachable_rpc())
    }

    /// Like [`node_with`], but with a caller-supplied RPC client (e.g. a `wiremock` server) for
    /// tests that exercise the RPC-fallback paths.
    fn node_with_rpc(
        responses: Vec<ReadResponse>,
        tip: Option<(u64, String)>,
        rpc: NodeClient,
    ) -> (
        ZebraStateNode<ScriptedReadState, FixedTip>,
        ScriptedReadState,
    ) {
        let read_state = ScriptedReadState::new(responses);
        let node = ZebraStateNode::new(read_state.clone(), FixedTip(tip), rpc, Network::Mainnet);
        (node, read_state)
    }

    /// The first testdata block, parsed into a real [`Block`].
    fn first_testdata_block() -> (Vec<u8>, Block) {
        let raw = crate::testutil::testdata_blocks()[0].clone();
        let block = Block::zcash_deserialize(&raw[..]).expect("valid testdata block");
        (raw, block)
    }

    #[tokio::test]
    async fn get_block_raw_round_trips_real_bytes() {
        let (raw, block) = first_testdata_block();
        let (node, _) = node_with(vec![ReadResponse::Block(Some(Arc::new(block)))], None);

        let bytes = node.get_block_raw("380640").await.unwrap();

        // Byte-identical to the original wire bytes: no re-encoding, no hex, no JSON round trip.
        assert_eq!(bytes, raw);
    }

    #[tokio::test]
    async fn get_block_verbose_maps_hash_txids_and_tree_sizes() {
        let (raw, block) = first_testdata_block();
        let expected_hash = block.hash().to_string();
        let txids: Arc<[transaction::Hash]> =
            block.transactions.iter().map(|tx| tx.hash()).collect();

        // Cross-check against the very list the compact-block parser would compute from the same
        // raw bytes: this is the parity guarantee the readstate backend must preserve (R2).
        let expected_txids: Vec<String> = crate::compact::to_compact_block(&raw)
            .expect("valid testdata block")
            .vtx
            .iter()
            .map(|tx| crate::encoding::wire_to_display_hex(&tx.txid))
            .collect();

        let mut sapling_tree = SaplingTree::default();
        sapling_tree
            .append(
                zebra_chain::sapling::tree::NoteCommitmentUpdate::from_bytes(&[0u8; 32]).unwrap(),
            )
            .unwrap();

        let mut orchard_tree = OrchardTree::default();
        orchard_tree
            .append(zebra_chain::orchard::tree::NoteCommitmentUpdate::from(1u64))
            .unwrap();
        orchard_tree
            .append(zebra_chain::orchard::tree::NoteCommitmentUpdate::from(2u64))
            .unwrap();

        let mut ironwood_tree = OrchardTree::default();
        for value in 10..13u64 {
            ironwood_tree
                .append(zebra_chain::orchard::tree::NoteCommitmentUpdate::from(
                    value,
                ))
                .unwrap();
        }

        let (node, _) = node_with(
            vec![
                ReadResponse::Block(Some(Arc::new(block))),
                ReadResponse::TransactionIdsForBlock(Some(txids)),
                ReadResponse::SaplingTree(Some(Arc::new(sapling_tree))),
                ReadResponse::OrchardTree(Some(Arc::new(orchard_tree))),
                ReadResponse::IronwoodTree(Some(Arc::new(ironwood_tree))),
            ],
            None,
        );

        let verbose = node.get_block_verbose(380_640).await.unwrap();

        assert_eq!(verbose.hash, expected_hash);
        assert_eq!(verbose.tx, expected_txids);
        assert_eq!(verbose.trees.sapling.size, 1);
        assert_eq!(verbose.trees.orchard.size, 2);
        assert_eq!(verbose.trees.ironwood.size, 3);
    }

    #[tokio::test]
    async fn get_block_verbose_reports_zero_sized_trees_when_absent() {
        let (_raw, block) = first_testdata_block();
        let txids: Arc<[transaction::Hash]> =
            block.transactions.iter().map(|tx| tx.hash()).collect();

        let (node, _) = node_with(
            vec![
                ReadResponse::Block(Some(Arc::new(block))),
                ReadResponse::TransactionIdsForBlock(Some(txids)),
                ReadResponse::SaplingTree(None),
                ReadResponse::OrchardTree(None),
                ReadResponse::IronwoodTree(None),
            ],
            None,
        );

        let verbose = node.get_block_verbose(380_640).await.unwrap();

        assert_eq!(verbose.trees.sapling.size, 0);
        assert_eq!(verbose.trees.orchard.size, 0);
        assert_eq!(verbose.trees.ironwood.size, 0);
    }

    #[tokio::test]
    async fn absent_block_maps_to_the_zebrad_out_of_range_rpc_error() {
        let (node, _) = node_with(vec![ReadResponse::Block(None)], None);

        let error = node.get_block_raw("380640").await.unwrap_err();

        assert!(matches!(
            error,
            NodeError::Rpc {
                code: RPC_BLOCK_NOT_FOUND,
                ..
            }
        ));
    }

    /// Mainnet activation height of `upgrade`, looked up from the zebra_chain API (not
    /// hardcoded), so tests track whatever the pinned zebra-chain version actually activates
    /// instead of silently drifting from it.
    fn mainnet_activation_height(upgrade: NetworkUpgrade) -> u64 {
        Network::Mainnet
            .full_activation_list()
            .into_iter()
            .find(|(_, entry)| *entry == upgrade)
            .map(|(height, _)| height.0 as u64)
            .unwrap_or_else(|| panic!("{upgrade:?} must be activated on mainnet"))
    }

    /// The hex branch id `get_blockchain_info` keys its upgrade map by.
    fn branch_id_hex(upgrade: NetworkUpgrade) -> String {
        upgrade
            .branch_id()
            .unwrap_or_else(|| panic!("{upgrade:?} must have a branch id"))
            .encode_hex()
    }

    #[tokio::test]
    async fn get_blockchain_info_reports_the_pinned_tip() {
        let (node, _) = node_with(vec![], Some((1_000_000, "ab".repeat(32))));

        let info = node.get_blockchain_info().await.unwrap();

        assert_eq!(info.chain, "main");
        assert_eq!(info.blocks, 1_000_000);
        assert_eq!(info.bestblockhash, "ab".repeat(32));
    }

    #[tokio::test]
    async fn get_blockchain_info_keys_upgrades_by_branch_id() {
        let nu5_height = mainnet_activation_height(NetworkUpgrade::Nu5);
        let (node, _) = node_with(vec![], Some((nu5_height, "ab".repeat(32))));

        let info = node.get_blockchain_info().await.unwrap();

        let nu5 = info
            .upgrades
            .get(&branch_id_hex(NetworkUpgrade::Nu5))
            .expect("NU5 entry present, keyed by its branch id");
        assert_eq!(nu5.activationheight, nu5_height);
        assert_eq!(nu5.status, "active");
    }

    #[tokio::test]
    async fn get_blockchain_info_reports_nu6_2_chaintip_before_nu6_3_activation() {
        let nu6_3_height = mainnet_activation_height(NetworkUpgrade::Nu6_3);
        let (node, _) = node_with(vec![], Some((nu6_3_height - 1, "ab".repeat(32))));

        let info = node.get_blockchain_info().await.unwrap();

        assert_eq!(
            info.consensus.chaintip,
            branch_id_hex(NetworkUpgrade::Nu6_2)
        );
    }

    #[tokio::test]
    async fn get_blockchain_info_flips_chaintip_to_nu6_3_at_activation() {
        let nu6_3_height = mainnet_activation_height(NetworkUpgrade::Nu6_3);
        let (node, _) = node_with(vec![], Some((nu6_3_height, "cd".repeat(32))));

        let info = node.get_blockchain_info().await.unwrap();

        assert_eq!(
            info.consensus.chaintip,
            branch_id_hex(NetworkUpgrade::Nu6_3)
        );
        let nu6_3 = info
            .upgrades
            .get(&branch_id_hex(NetworkUpgrade::Nu6_3))
            .expect("NU6.3 entry present");
        assert_eq!(nu6_3.status, "active");
    }

    #[tokio::test]
    async fn get_blockchain_info_reports_a_pending_upgrade_before_its_activation() {
        let nu6_3_height = mainnet_activation_height(NetworkUpgrade::Nu6_3);
        let (node, _) = node_with(vec![], Some((nu6_3_height - 1, "00".repeat(32))));

        let info = node.get_blockchain_info().await.unwrap();

        let nu6_3 = info
            .upgrades
            .get(&branch_id_hex(NetworkUpgrade::Nu6_3))
            .expect("NU6.3 entry present");
        assert_eq!(nu6_3.status, "pending");
    }

    #[tokio::test]
    async fn get_raw_transaction_falls_back_to_rpc_when_not_mined() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": { "hex": "deadbeef", "height": 0 },
            })))
            .mount(&server)
            .await;
        let rpc = NodeClient::new(&NodeConfig {
            url: server.uri(),
            user: String::new(),
            password: String::new(),
        })
        .unwrap();
        let txid = "0".repeat(64);
        let (node, _) = node_with_rpc(vec![ReadResponse::Transaction(None)], None, rpc);

        let result = node.get_raw_transaction(&txid).await.unwrap();

        assert_eq!(result.hex, "deadbeef");
        assert_eq!(result.height, 0);
    }

    #[tokio::test]
    async fn get_raw_transaction_serves_a_mined_transaction_from_the_state() {
        let (_raw, block) = first_testdata_block();
        let tx = block.transactions[0].clone();
        let expected_hex = hex::encode(tx.zcash_serialize_to_vec().unwrap());
        let expected_height = block::Height(380_640);
        let mined = MinedTx::new(tx.clone(), expected_height, 1, block.header.time);
        let txid = tx.hash().to_string();

        let (node, _) = node_with(vec![ReadResponse::Transaction(Some(mined))], None);

        let result = node.get_raw_transaction(&txid).await.unwrap();

        assert_eq!(result.hex, expected_hex);
        assert_eq!(result.height, expected_height.0 as i64);
    }

    #[tokio::test]
    async fn get_address_txids_with_an_open_upper_bound_queries_up_to_the_tip() {
        let addresses = vec![crate::testutil::example_taddress()];
        let (node, read_state) = node_with(
            vec![ReadResponse::AddressesTransactionIds(Default::default())],
            Some((500, "0".repeat(64))),
        );

        let result = node.get_address_txids(&addresses, 100, 0).await.unwrap();

        assert!(result.is_empty());
        let requests = read_state.requests();
        assert_eq!(requests.len(), 1);
        match &requests[0] {
            ReadRequest::TransactionIdsByAddresses { height_range, .. } => {
                assert_eq!(*height_range, block::Height(100)..=block::Height(500));
            }
            other => panic!("expected TransactionIdsByAddresses, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn get_address_txids_clamps_a_start_past_the_tip_to_the_tip() {
        // zebrad's build_height_range clamps a start above the tip down to the tip (querying the
        // tip alone), not to an empty inverted range.
        let addresses = vec![crate::testutil::example_taddress()];
        let (node, read_state) = node_with(
            vec![ReadResponse::AddressesTransactionIds(Default::default())],
            Some((500, "0".repeat(64))),
        );

        node.get_address_txids(&addresses, 100_000, 0)
            .await
            .unwrap();

        match &read_state.requests()[0] {
            ReadRequest::TransactionIdsByAddresses { height_range, .. } => {
                assert_eq!(*height_range, block::Height(500)..=block::Height(500));
            }
            other => panic!("expected TransactionIdsByAddresses, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn get_address_txids_rejects_an_inverted_range_like_zebrad() {
        // After clamping both bounds to the tip, a start past the end is the error zebrad returns
        // verbatim (code and message), not a silently-empty inverted state read.
        let addresses = vec![crate::testutil::example_taddress()];
        let (node, read_state) = node_with(vec![], Some((500, "0".repeat(64))));

        let error = node
            .get_address_txids(&addresses, 400, 200)
            .await
            .unwrap_err();

        match error {
            NodeError::Rpc { code, message } => {
                assert_eq!(code, -1);
                assert_eq!(
                    message,
                    "start Height(400) must be less than or equal to end Height(200)"
                );
            }
            other => panic!("expected NodeError::Rpc, got {other:?}"),
        }
        assert!(read_state.requests().is_empty());
    }

    /// A regtest network where Sapling and NU5 activate at height 1 and NU6.3 never does, so the
    /// testdata block's height (380640) has sapling+orchard active and ironwood inactive.
    fn regtest_with_sapling_and_nu5() -> Network {
        use zebra_chain::parameters::testnet::{ConfiguredActivationHeights, RegtestParameters};
        Network::new_regtest(RegtestParameters {
            activation_heights: ConfiguredActivationHeights {
                sapling: Some(1),
                nu5: Some(1),
                ..Default::default()
            },
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn get_treestate_omits_every_pool_inactive_at_the_blocks_height() {
        // On mainnet the testdata block (380640) predates Sapling activation: all three pools
        // must be omitted (empty final_state) even though the state has trees to serve —
        // matching zebrad, which omits a pool rather than serializing an empty frontier.
        let (_raw, block) = first_testdata_block();
        let (node, _) = node_with(
            vec![
                ReadResponse::Block(Some(Arc::new(block))),
                ReadResponse::SaplingTree(Some(Arc::new(SaplingTree::default()))),
                ReadResponse::OrchardTree(Some(Arc::new(OrchardTree::default()))),
                ReadResponse::IronwoodTree(Some(Arc::new(OrchardTree::default()))),
            ],
            None,
        );

        let treestate = node.get_treestate("380640").await.unwrap();

        assert_eq!(treestate.height, 380_640);
        assert_eq!(treestate.sapling.commitments.final_state, "");
        assert_eq!(treestate.orchard.commitments.final_state, "");
        assert_eq!(treestate.ironwood.commitments.final_state, "");
    }

    #[tokio::test]
    async fn get_treestate_serializes_only_the_pools_active_at_the_blocks_height() {
        // The same block on a regtest where Sapling and NU5 are active from height 1 (and NU6.3
        // never activates): sapling and orchard serialize their trees through `to_rpc_bytes`,
        // ironwood stays omitted — the activation boundary in one response.
        let (_raw, block) = first_testdata_block();
        let expected_hash = block.hash().to_string();
        let sapling_tree = SaplingTree::default();
        let orchard_tree = OrchardTree::default();
        let expected_sapling = hex::encode(sapling_tree.to_rpc_bytes());
        let expected_orchard = hex::encode(orchard_tree.to_rpc_bytes());
        let read_state = ScriptedReadState::new(vec![
            ReadResponse::Block(Some(Arc::new(block))),
            ReadResponse::SaplingTree(Some(Arc::new(sapling_tree))),
            ReadResponse::OrchardTree(Some(Arc::new(orchard_tree))),
            ReadResponse::IronwoodTree(None),
        ]);
        let node = ZebraStateNode::new(
            read_state,
            FixedTip(None),
            unreachable_rpc(),
            regtest_with_sapling_and_nu5(),
        );

        let treestate = node.get_treestate("380640").await.unwrap();

        assert_eq!(treestate.hash, expected_hash);
        assert!(!expected_sapling.is_empty());
        assert_eq!(treestate.sapling.commitments.final_state, expected_sapling);
        assert_eq!(treestate.orchard.commitments.final_state, expected_orchard);
        assert_eq!(treestate.ironwood.commitments.final_state, "");
    }

    #[tokio::test]
    async fn get_address_balance_maps_the_state_balance() {
        let addresses = vec![crate::testutil::example_taddress()];
        let (node, _) = node_with(
            vec![ReadResponse::AddressBalance {
                balance: zebra_chain::amount::Amount::try_from(123_456)
                    .expect("valid nonnegative amount"),
                received: 200_000,
            }],
            None,
        );

        let balance = node.get_address_balance(&addresses).await.unwrap();

        assert_eq!(balance.balance, 123_456);
    }

    #[tokio::test]
    async fn get_subtrees_maps_orchard_subtree_roots_and_heights() {
        let root = zebra_chain::orchard::tree::Node::try_from([0u8; 32])
            .expect("zero is a canonical pallas base encoding");
        let mut subtrees = std::collections::BTreeMap::new();
        subtrees.insert(
            NoteCommitmentSubtreeIndex(5),
            zebra_chain::subtree::NoteCommitmentSubtreeData::new(block::Height(1_000), root),
        );
        let (node, read_state) = node_with(vec![ReadResponse::OrchardSubtrees(subtrees)], None);

        let result = node.get_subtrees("orchard", 5, 1).await.unwrap();

        assert_eq!(result.subtrees.len(), 1);
        assert_eq!(result.subtrees[0].root, "00".repeat(32));
        assert_eq!(result.subtrees[0].end_height, 1_000);
        match &read_state.requests()[0] {
            ReadRequest::OrchardSubtrees { start_index, limit } => {
                assert_eq!(*start_index, NoteCommitmentSubtreeIndex(5));
                assert_eq!(*limit, Some(NoteCommitmentSubtreeIndex(1)));
            }
            other => panic!("expected OrchardSubtrees, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn get_subtrees_start_index_past_u16_max_returns_empty_without_a_state_read() {
        // Subtree indexes are u16 in zebra's state: a start past u16::MAX must be the empty set,
        // not a wrapped-around read of some other subtree.
        let (node, read_state) = node_with(vec![], None);

        let result = node
            .get_subtrees("sapling", u32::from(u16::MAX) + 1, 0)
            .await
            .unwrap();

        assert!(result.subtrees.is_empty());
        assert!(read_state.requests().is_empty());
    }

    #[tokio::test]
    async fn get_subtrees_clamps_an_oversized_max_entries_instead_of_truncating() {
        // 65536 as u16 would truncate to a limit of 0 — an empty response where the rpc backend
        // returns data. It must clamp to u16::MAX instead.
        let (node, read_state) = node_with(
            vec![ReadResponse::SaplingSubtrees(Default::default())],
            None,
        );

        node.get_subtrees("sapling", 0, u32::from(u16::MAX) + 1)
            .await
            .unwrap();

        match &read_state.requests()[0] {
            ReadRequest::SaplingSubtrees { limit, .. } => {
                assert_eq!(*limit, Some(NoteCommitmentSubtreeIndex(u16::MAX)));
            }
            other => panic!("expected SaplingSubtrees, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn get_subtrees_rejects_an_unknown_pool_name() {
        let (node, _) = node_with(vec![], None);

        let error = node.get_subtrees("nonsense", 0, 0).await.unwrap_err();

        match error {
            NodeError::Rpc { code, message } => {
                assert_eq!(code, -1);
                assert!(message.contains("invalid pool name"));
            }
            other => panic!("expected NodeError::Rpc, got {other:?}"),
        }
    }
}

/// Regression tests for the two wire differences the 2026-07 live parity sweep found
/// (contrib/bench/results/rss-parity-2026-07.md): upgrade-name branding and inactive-pool
/// treestate gating.
#[cfg(test)]
mod parity_regression_tests {
    use super::*;

    #[test]
    fn upgrade_names_match_zebrads_branding_not_the_rust_identifiers() {
        assert_eq!(upgrade_name(NetworkUpgrade::Nu5), "NU5");
        assert_eq!(upgrade_name(NetworkUpgrade::Nu6_3), "NU6.3");
        assert_eq!(upgrade_name(NetworkUpgrade::Sapling), "Sapling");
    }

    #[test]
    fn pool_active_gates_on_the_mainnet_activation_heights() {
        let network = Network::Mainnet;
        let sapling_activation = NetworkUpgrade::Sapling
            .activation_height(&network)
            .expect("sapling activates on mainnet");
        assert!(!pool_active(
            &network,
            NetworkUpgrade::Sapling,
            block::Height(sapling_activation.0 - 1)
        ));
        assert!(pool_active(
            &network,
            NetworkUpgrade::Sapling,
            sapling_activation
        ));
        // NU6.3 (ironwood) is pending at today's tip heights: pre-activation treestates must
        // omit the pool (empty string), not serialize an empty frontier.
        assert!(!pool_active(
            &network,
            NetworkUpgrade::Nu6_3,
            block::Height(3_400_000)
        ));
    }
}
