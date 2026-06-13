//! Implementation of the `CompactTxStreamer` gRPC service.
//!
//! Implemented: chain info, block serving (`GetBlock`/`GetBlockRange`), `GetTransaction`,
//! `SendTransaction`, tree state, transparent-address balance and UTXOs, and `Ping`. The mempool
//! streams, subtree roots, transparent-address transaction listings, and the nullifier-only block
//! variants still return `unimplemented`.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use async_stream::try_stream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::cache::{Cache, CacheError};
use crate::encoding;
use crate::fetch::{self, FetchError};
use crate::filter;
use crate::node::{self, NodeError, NodeRpc};
use crate::proto::compact_tx_streamer_server::CompactTxStreamer;
use crate::proto::{
    Address, AddressList, Balance, BlockId, BlockRange, ChainSpec, CompactBlock, CompactTx,
    Duration, Empty, GetAddressUtxosArg, GetAddressUtxosReply, GetAddressUtxosReplyList,
    GetMempoolTxRequest, GetSubtreeRootsArg, LightdInfo, PingResponse, RawTransaction,
    SendResponse, SubtreeRoot, TransparentAddressBlockFilter, TreeState, TxFilter,
};

/// Boxed server-streaming response, shared by every streaming method's associated type.
type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

/// The gRPC service. Holds a client to the backend node, the block cache, and the network name.
#[derive(Clone)]
pub struct Streamer {
    node: Arc<dyn NodeRpc>,
    cache: Arc<Cache>,
    network: String,
    /// Number of `Ping` calls currently in flight, shared across cloned services (testing only).
    ping_count: Arc<AtomicI64>,
}

impl Streamer {
    /// Build the service from a node client, a shared block cache, and the network name.
    pub fn new(node: Arc<dyn NodeRpc>, cache: Arc<Cache>, network: String) -> Self {
        Self {
            node,
            cache,
            network,
            ping_count: Arc::new(AtomicI64::new(0)),
        }
    }

    /// Fetch the UTXOs for the requested addresses, apply the `startHeight`/`maxEntries` filters, and
    /// convert them into the gRPC reply shape.
    async fn collect_utxos(
        &self,
        arg: &GetAddressUtxosArg,
    ) -> Result<Vec<GetAddressUtxosReply>, Status> {
        let utxos = self.node.get_address_utxos(&arg.addresses).await?;
        let mut replies = Vec::new();
        for utxo in utxos {
            if utxo.height < arg.start_height {
                continue;
            }
            if arg.max_entries > 0 && replies.len() as u32 >= arg.max_entries {
                break;
            }
            let txid = encoding::display_hex_to_wire(&utxo.txid)
                .map_err(|e| Status::internal(format!("decoding utxo txid: {e}")))?;
            let script = hex::decode(&utxo.script)
                .map_err(|e| Status::internal(format!("decoding utxo script: {e}")))?;
            replies.push(GetAddressUtxosReply {
                address: utxo.address,
                txid,
                index: utxo.output_index as i32,
                script,
                value_zat: utxo.satoshis as i64,
                height: utxo.height,
            });
        }
        Ok(replies)
    }
}

/// Build the gRPC `TreeState` from a node `z_gettreestate` response and the network name.
fn to_tree_state(network: &str, tree_state: node::GetTreeState) -> TreeState {
    TreeState {
        network: network.to_string(),
        height: tree_state.height,
        hash: tree_state.hash,
        time: tree_state.time,
        sapling_tree: tree_state.sapling.commitments.final_state,
        orchard_tree: tree_state.orchard.commitments.final_state,
    }
}

impl From<NodeError> for Status {
    fn from(err: NodeError) -> Self {
        Status::unavailable(err.to_string())
    }
}

impl From<FetchError> for Status {
    fn from(err: FetchError) -> Self {
        match err {
            FetchError::Node(e) => Status::unavailable(e.to_string()),
            FetchError::Parse(e) => Status::internal(e.to_string()),
        }
    }
}

impl From<CacheError> for Status {
    fn from(err: CacheError) -> Self {
        Status::internal(err.to_string())
    }
}

#[tonic::async_trait]
impl CompactTxStreamer for Streamer {
    async fn get_latest_block(
        &self,
        _request: Request<ChainSpec>,
    ) -> Result<Response<BlockId>, Status> {
        let info = self.node.get_blockchain_info().await?;
        let hash = encoding::display_hex_to_wire(&info.bestblockhash)
            .map_err(|e| Status::internal(format!("decoding best block hash: {e}")))?;
        Ok(Response::new(BlockId {
            height: info.blocks,
            hash,
        }))
    }

    async fn get_lightd_info(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<LightdInfo>, Status> {
        let node_info = self.node.get_info().await?;
        let chain = self.node.get_blockchain_info().await?;

        let sapling_activation_height = chain
            .upgrades
            .values()
            .find(|u| u.name.eq_ignore_ascii_case("sapling"))
            .map(|u| u.activationheight)
            .unwrap_or(0);

        let info = LightdInfo {
            version: env!("CARGO_PKG_VERSION").to_string(),
            vendor: "lightwalletd-rs".to_string(),
            taddr_support: true,
            chain_name: chain.chain,
            sapling_activation_height,
            consensus_branch_id: chain.consensus.chaintip,
            block_height: chain.blocks,
            estimated_height: chain.estimatedheight,
            zcashd_build: node_info.build,
            zcashd_subversion: node_info.subversion,
            ..Default::default()
        };
        Ok(Response::new(info))
    }

    async fn get_block(&self, request: Request<BlockId>) -> Result<Response<CompactBlock>, Status> {
        let block_id = request.into_inner();
        if !block_id.hash.is_empty() {
            return Err(Status::unimplemented(
                "get_block by hash is not yet supported",
            ));
        }
        if let Some(block) = self.cache.get(block_id.height)? {
            return Ok(Response::new(block));
        }
        let block = fetch::compact_block(self.node.as_ref(), block_id.height).await?;
        Ok(Response::new(block))
    }

    async fn get_block_nullifiers(
        &self,
        request: Request<BlockId>,
    ) -> Result<Response<CompactBlock>, Status> {
        let block_id = request.into_inner();
        if !block_id.hash.is_empty() {
            return Err(Status::unimplemented(
                "get_block_nullifiers by hash is not yet supported",
            ));
        }
        let block = match self.cache.get(block_id.height)? {
            Some(block) => block,
            None => fetch::compact_block(self.node.as_ref(), block_id.height).await?,
        };
        Ok(Response::new(filter::nullifiers_only(block)))
    }

    type GetBlockRangeStream = BoxStream<CompactBlock>;
    async fn get_block_range(
        &self,
        request: Request<BlockRange>,
    ) -> Result<Response<Self::GetBlockRangeStream>, Status> {
        let range = request.into_inner();
        let start = range.start.map(|b| b.height).unwrap_or(0);
        let end = range.end.map(|b| b.height).unwrap_or(0);
        let pool_types = range.pool_types;
        let node = self.node.clone();
        let cache = self.cache.clone();

        let stream = try_stream! {
            // start <= end yields ascending heights; otherwise descending.
            let heights: Vec<u64> = if start <= end {
                (start..=end).collect()
            } else {
                (end..=start).rev().collect()
            };
            for height in heights {
                let block = match cache.get(height)? {
                    Some(block) => block,
                    None => fetch::compact_block(node.as_ref(), height).await?,
                };
                yield filter::filter_block_to_pools(block, &pool_types);
            }
        };
        Ok(Response::new(Box::pin(stream)))
    }

    type GetBlockRangeNullifiersStream = BoxStream<CompactBlock>;
    async fn get_block_range_nullifiers(
        &self,
        request: Request<BlockRange>,
    ) -> Result<Response<Self::GetBlockRangeNullifiersStream>, Status> {
        let range = request.into_inner();
        let start = range.start.map(|b| b.height).unwrap_or(0);
        let end = range.end.map(|b| b.height).unwrap_or(0);
        let node = self.node.clone();
        let cache = self.cache.clone();

        let stream = try_stream! {
            let heights: Vec<u64> = if start <= end {
                (start..=end).collect()
            } else {
                (end..=start).rev().collect()
            };
            for height in heights {
                let block = match cache.get(height)? {
                    Some(block) => block,
                    None => fetch::compact_block(node.as_ref(), height).await?,
                };
                yield filter::nullifiers_only(block);
            }
        };
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_transaction(
        &self,
        request: Request<TxFilter>,
    ) -> Result<Response<RawTransaction>, Status> {
        let filter = request.into_inner();
        if filter.hash.is_empty() {
            return Err(Status::unimplemented(
                "get_transaction requires a txid hash",
            ));
        }
        let txid = encoding::wire_to_display_hex(&filter.hash);
        let raw = self.node.get_raw_transaction(&txid).await?;
        let data = hex::decode(&raw.hex)
            .map_err(|e| Status::internal(format!("decoding transaction hex: {e}")))?;
        // A negative height means the tx is not on the main chain.
        let height = if raw.height < 0 {
            u64::MAX
        } else {
            raw.height as u64
        };
        Ok(Response::new(RawTransaction { data, height }))
    }

    async fn send_transaction(
        &self,
        request: Request<RawTransaction>,
    ) -> Result<Response<SendResponse>, Status> {
        let raw = request.into_inner();
        match self
            .node
            .send_raw_transaction(&hex::encode(&raw.data))
            .await
        {
            Ok(txid) => Ok(Response::new(SendResponse {
                error_code: 0,
                error_message: txid,
            })),
            // A node-side rejection is reported in-band in the SendResponse, not as a gRPC error.
            Err(NodeError::Rpc { code, message }) => Ok(Response::new(SendResponse {
                error_code: code as i32,
                error_message: message,
            })),
            Err(other) => Err(other.into()),
        }
    }

    type GetTaddressTxidsStream = BoxStream<RawTransaction>;
    async fn get_taddress_txids(
        &self,
        _request: Request<TransparentAddressBlockFilter>,
    ) -> Result<Response<Self::GetTaddressTxidsStream>, Status> {
        Err(Status::unimplemented(
            "get_taddress_txids: implemented in P4",
        ))
    }

    type GetTaddressTransactionsStream = BoxStream<RawTransaction>;
    async fn get_taddress_transactions(
        &self,
        _request: Request<TransparentAddressBlockFilter>,
    ) -> Result<Response<Self::GetTaddressTransactionsStream>, Status> {
        Err(Status::unimplemented(
            "get_taddress_transactions: implemented in P4",
        ))
    }

    async fn get_taddress_balance(
        &self,
        request: Request<AddressList>,
    ) -> Result<Response<Balance>, Status> {
        let address_list = request.into_inner();
        let balance = self
            .node
            .get_address_balance(&address_list.addresses)
            .await?;
        Ok(Response::new(Balance {
            value_zat: balance.balance,
        }))
    }

    async fn get_taddress_balance_stream(
        &self,
        request: Request<tonic::Streaming<Address>>,
    ) -> Result<Response<Balance>, Status> {
        let mut incoming = request.into_inner();
        let mut addresses = Vec::new();
        while let Some(address) = incoming.message().await? {
            addresses.push(address.address);
        }
        let balance = self.node.get_address_balance(&addresses).await?;
        Ok(Response::new(Balance {
            value_zat: balance.balance,
        }))
    }

    type GetMempoolTxStream = BoxStream<CompactTx>;
    async fn get_mempool_tx(
        &self,
        _request: Request<GetMempoolTxRequest>,
    ) -> Result<Response<Self::GetMempoolTxStream>, Status> {
        Err(Status::unimplemented("get_mempool_tx: implemented in P4"))
    }

    type GetMempoolStreamStream = BoxStream<RawTransaction>;
    async fn get_mempool_stream(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::GetMempoolStreamStream>, Status> {
        Err(Status::unimplemented(
            "get_mempool_stream: implemented in P4",
        ))
    }

    async fn get_tree_state(
        &self,
        request: Request<BlockId>,
    ) -> Result<Response<TreeState>, Status> {
        let block_id = request.into_inner();
        if !block_id.hash.is_empty() {
            return Err(Status::unimplemented(
                "get_tree_state by hash is not yet supported",
            ));
        }
        let tree_state = self
            .node
            .get_treestate(&block_id.height.to_string())
            .await?;
        Ok(Response::new(to_tree_state(&self.network, tree_state)))
    }

    async fn get_latest_tree_state(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<TreeState>, Status> {
        let chain_info = self.node.get_blockchain_info().await?;
        let tree_state = self
            .node
            .get_treestate(&chain_info.blocks.to_string())
            .await?;
        Ok(Response::new(to_tree_state(&self.network, tree_state)))
    }

    type GetSubtreeRootsStream = BoxStream<SubtreeRoot>;
    async fn get_subtree_roots(
        &self,
        _request: Request<GetSubtreeRootsArg>,
    ) -> Result<Response<Self::GetSubtreeRootsStream>, Status> {
        Err(Status::unimplemented(
            "get_subtree_roots: implemented in P4",
        ))
    }

    async fn get_address_utxos(
        &self,
        request: Request<GetAddressUtxosArg>,
    ) -> Result<Response<GetAddressUtxosReplyList>, Status> {
        let address_utxos = self.collect_utxos(&request.into_inner()).await?;
        Ok(Response::new(GetAddressUtxosReplyList { address_utxos }))
    }

    type GetAddressUtxosStreamStream = BoxStream<GetAddressUtxosReply>;
    async fn get_address_utxos_stream(
        &self,
        request: Request<GetAddressUtxosArg>,
    ) -> Result<Response<Self::GetAddressUtxosStreamStream>, Status> {
        let replies = self.collect_utxos(&request.into_inner()).await?;
        let stream = tokio_stream::iter(replies.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn ping(&self, request: Request<Duration>) -> Result<Response<PingResponse>, Status> {
        let interval_us = request.into_inner().interval_us;
        let entry = self.ping_count.fetch_add(1, Ordering::SeqCst) + 1;
        if interval_us > 0 {
            tokio::time::sleep(std::time::Duration::from_micros(interval_us as u64)).await;
        }
        let exit = self.ping_count.load(Ordering::SeqCst);
        self.ping_count.fetch_sub(1, Ordering::SeqCst);
        Ok(Response::new(PingResponse { entry, exit }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::FakeNode;
    use serde_json::json;

    fn streamer_with(node: Arc<dyn NodeRpc>) -> (tempfile::TempDir, Streamer) {
        let dir = tempfile::tempdir().unwrap();
        let cache = Arc::new(Cache::open(&dir.path().join("blocks.redb")).unwrap());
        (dir, Streamer::new(node, cache, "main".to_string()))
    }

    fn address_utxo(txid: &str, height: u64) -> node::AddressUtxo {
        serde_json::from_value(json!({
            "address": "t1",
            "txid": txid,
            "outputIndex": 2,
            "script": "abcd",
            "satoshis": 7,
            "height": height,
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn get_latest_block_reverses_display_hash_to_wire() {
        let display_hash = "0011223344556677889900aabbccddeeff00112233445566778899aabbccddee";
        let fake = Arc::new(FakeNode {
            blockchain_info: Some(
                serde_json::from_value(json!({
                    "chain": "main",
                    "blocks": 1000,
                    "bestblockhash": display_hash,
                    "consensus": { "chaintip": "00000000" },
                }))
                .unwrap(),
            ),
            ..Default::default()
        });
        let (_dir, streamer) = streamer_with(fake);

        let response = streamer
            .get_latest_block(Request::new(ChainSpec::default()))
            .await
            .unwrap()
            .into_inner();

        let mut wire = hex::decode(display_hash).unwrap();
        wire.reverse();
        assert_eq!(
            response,
            BlockId {
                height: 1000,
                hash: wire
            }
        );
    }

    #[tokio::test]
    async fn get_transaction_reverses_filter_txid_and_maps_offchain_height() {
        let fake = Arc::new(FakeNode {
            raw_transaction: Some(
                serde_json::from_value(json!({ "hex": "deadbeef", "height": -1 })).unwrap(),
            ),
            ..Default::default()
        });
        let (_dir, streamer) = streamer_with(fake.clone());

        let wire_txid = vec![0xaa, 0xbb, 0xcc, 0xdd];
        let response = streamer
            .get_transaction(Request::new(TxFilter {
                hash: wire_txid.clone(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();

        // The node is called with the display-order (reversed) hex of the wire txid.
        let mut display = wire_txid;
        display.reverse();
        assert_eq!(
            *fake.requested_txid.lock().unwrap(),
            Some(hex::encode(display))
        );
        assert_eq!(
            response,
            RawTransaction {
                data: hex::decode("deadbeef").unwrap(),
                height: u64::MAX,
            }
        );
    }

    #[tokio::test]
    async fn send_transaction_returns_txid_on_success() {
        let fake = Arc::new(FakeNode {
            send_ok: Some("txid-abc".to_string()),
            ..Default::default()
        });
        let (_dir, streamer) = streamer_with(fake);

        let response = streamer
            .send_transaction(Request::new(RawTransaction {
                data: vec![1, 2, 3],
                height: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            response,
            SendResponse {
                error_code: 0,
                error_message: "txid-abc".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn send_transaction_reports_node_rejection_in_band() {
        let fake = Arc::new(FakeNode {
            send_err: Some((-26, "tx rejected".to_string())),
            ..Default::default()
        });
        let (_dir, streamer) = streamer_with(fake);

        let response = streamer
            .send_transaction(Request::new(RawTransaction {
                data: vec![1, 2, 3],
                height: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            response,
            SendResponse {
                error_code: -26,
                error_message: "tx rejected".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn collect_utxos_reverses_txid_and_applies_start_height_and_max_entries() {
        let utxos = vec![
            address_utxo("00112233", 100),
            address_utxo("44556677", 200),
            address_utxo("8899aabb", 300),
        ];
        let fake = Arc::new(FakeNode {
            address_utxos: Some(utxos),
            ..Default::default()
        });
        let (_dir, streamer) = streamer_with(fake);

        let replies = streamer
            .collect_utxos(&GetAddressUtxosArg {
                addresses: vec!["t1".to_string()],
                start_height: 150,
                max_entries: 1,
            })
            .await
            .unwrap();

        assert_eq!(
            replies,
            vec![GetAddressUtxosReply {
                address: "t1".to_string(),
                txid: vec![0x77, 0x66, 0x55, 0x44],
                index: 2,
                script: vec![0xab, 0xcd],
                value_zat: 7,
                height: 200,
            }]
        );
    }

    #[test]
    fn to_tree_state_maps_final_state_per_pool() {
        let tree_state: node::GetTreeState = serde_json::from_value(json!({
            "hash": "abcd",
            "height": 1234,
            "time": 42,
            "sapling": { "commitments": { "finalState": "aa" } },
            "orchard": { "commitments": { "finalState": "bb" } },
        }))
        .unwrap();

        assert_eq!(
            to_tree_state("main", tree_state),
            TreeState {
                network: "main".to_string(),
                height: 1234,
                hash: "abcd".to_string(),
                time: 42,
                sapling_tree: "aa".to_string(),
                orchard_tree: "bb".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn get_taddress_balance_returns_value_zat() {
        let fake = Arc::new(FakeNode {
            address_balance: Some(serde_json::from_value(json!({ "balance": 4242 })).unwrap()),
            ..Default::default()
        });
        let (_dir, streamer) = streamer_with(fake);

        let response = streamer
            .get_taddress_balance(Request::new(AddressList {
                addresses: vec!["t1".to_string()],
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response, Balance { value_zat: 4242 });
    }
}
