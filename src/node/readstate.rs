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
        let height = self.best_tip_height()?;
        let hash = self.best_tip_hash()?;
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
            .map_err(|error| NodeError::State(error.to_string()))?
            .call(request)
            .await
            .map_err(|error| NodeError::State(error.to_string()))
    }

    /// The best tip, or a synthesized "not ready" error before the state has one.
    fn best_tip(&self) -> Result<(u64, String), NodeError> {
        self.tip
            .best_tip()
            .ok_or_else(|| NodeError::State("read state has no chain tip yet".to_string()))
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
    NodeError::State(format!("unexpected read response: {response:?}"))
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
                    name: format!("{upgrade}"),
                    activationheight: height.0 as u64,
                    status: if blocks >= height.0 as u64 {
                        "active".to_string()
                    } else {
                        "pending".to_string()
                    },
                },
            );
        }
        let chaintip = NetworkUpgrade::current(&self.network, block::Height(blocks as u32))
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
        let id = HashOrHeight::Height(block::Height(height as u32));
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
            .map_err(|error| NodeError::State(format!("serializing block: {error}")))
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
                    .map_err(|error| NodeError::State(format!("serializing tx: {error}")))?;
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
            .ok_or_else(|| NodeError::State("block has no coinbase height".to_string()))?;
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

        Ok(GetTreeState {
            hash: hash.to_string(),
            height: height.0 as u64,
            time: block.header.time.timestamp() as u32,
            sapling: TreePool {
                commitments: TreeCommitments {
                    final_state: sapling
                        .map(|tree| hex::encode(tree.to_rpc_bytes()))
                        .unwrap_or_default(),
                },
            },
            orchard: TreePool {
                commitments: TreeCommitments {
                    final_state: orchard
                        .map(|tree| hex::encode(tree.to_rpc_bytes()))
                        .unwrap_or_default(),
                },
            },
            ironwood: TreePool {
                commitments: TreeCommitments {
                    final_state: ironwood
                        .map(|tree| hex::encode(tree.to_rpc_bytes()))
                        .unwrap_or_default(),
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
        // Match the JSON-RPC contract: start/end of 0 mean an open bound.
        let start = block::Height(start.max(1) as u32);
        let end = block::Height(if end == 0 { tip as u32 } else { end as u32 });
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
        let start_index = NoteCommitmentSubtreeIndex(start_index as u16);
        let limit = (max_entries > 0).then_some(NoteCommitmentSubtreeIndex(max_entries as u16));
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

    #[tokio::test]
    async fn get_blockchain_info_reports_chain_upgrades_and_chaintip() {
        let network = Network::Mainnet;

        // Looked up from the zebra_chain API (not hardcoded), so the test tracks whatever the
        // pinned zebra-chain version actually activates instead of silently drifting from it.
        let activation_height = |target: NetworkUpgrade| -> u64 {
            network
                .full_activation_list()
                .into_iter()
                .find(|(_, upgrade)| *upgrade == target)
                .map(|(height, _)| height.0 as u64)
                .unwrap_or_else(|| panic!("{target:?} must be activated on mainnet"))
        };
        let nu5_height = activation_height(NetworkUpgrade::Nu5);
        let nu5_branch_id = NetworkUpgrade::Nu5
            .branch_id()
            .unwrap()
            .encode_hex::<String>();
        let nu6_3_height = activation_height(NetworkUpgrade::Nu6_3);
        let nu6_2_branch_id = NetworkUpgrade::Nu6_2
            .branch_id()
            .unwrap()
            .encode_hex::<String>();
        let nu6_3_branch_id = NetworkUpgrade::Nu6_3
            .branch_id()
            .unwrap()
            .encode_hex::<String>();

        // Pre-NU6.3 tip: the chaintip branch id is still NU6.2's.
        let (node, _) = node_with(vec![], Some((nu6_3_height - 1, "ab".repeat(32))));
        let info = node.get_blockchain_info().await.unwrap();
        assert_eq!(info.chain, "main");
        assert_eq!(info.blocks, nu6_3_height - 1);
        assert_eq!(info.bestblockhash, "ab".repeat(32));
        let nu5 = info
            .upgrades
            .get(&nu5_branch_id)
            .expect("NU5 entry present, keyed by its branch id");
        assert_eq!(nu5.activationheight, nu5_height);
        assert_eq!(nu5.status, "active");
        assert_eq!(info.consensus.chaintip, nu6_2_branch_id);

        // Post-NU6.3 tip: the chaintip branch id flips to NU6.3's, and NU6.3's own upgrade entry
        // (if present) reports itself active.
        let (node, _) = node_with(vec![], Some((nu6_3_height, "cd".repeat(32))));
        let info = node.get_blockchain_info().await.unwrap();
        assert_eq!(info.consensus.chaintip, nu6_3_branch_id);
        if let Some(nu6_3) = info.upgrades.get(&nu6_3_branch_id) {
            assert_eq!(nu6_3.status, "active");
        }
    }

    #[tokio::test]
    async fn get_blockchain_info_reports_a_pending_upgrade_before_its_activation() {
        let network = Network::Mainnet;
        let nu6_3_height = network
            .full_activation_list()
            .into_iter()
            .find(|(_, upgrade)| *upgrade == NetworkUpgrade::Nu6_3)
            .map(|(height, _)| height.0 as u64)
            .expect("NU6.3 must be activated on mainnet");
        let nu6_3_branch_id = NetworkUpgrade::Nu6_3
            .branch_id()
            .unwrap()
            .encode_hex::<String>();

        let (node, _) = node_with(vec![], Some((nu6_3_height - 1, "00".repeat(32))));

        let info = node.get_blockchain_info().await.unwrap();

        let nu6_3 = info
            .upgrades
            .get(&nu6_3_branch_id)
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
