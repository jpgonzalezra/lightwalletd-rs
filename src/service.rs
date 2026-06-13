//! Implementation of the `CompactTxStreamer` gRPC service.
//!
//! Implemented so far: `GetLightdInfo`, `GetLatestBlock`, and `GetBlock`. Every other method returns
//! `unimplemented` and is filled in by later phases.

use std::pin::Pin;
use std::sync::Arc;

use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::cache::{Cache, CacheError};
use crate::fetch::{self, FetchError};
use crate::node::{NodeClient, NodeError};
use crate::proto::compact_tx_streamer_server::CompactTxStreamer;
use crate::proto::{
    Address, AddressList, Balance, BlockId, BlockRange, ChainSpec, CompactBlock, CompactTx,
    Duration, Empty, GetAddressUtxosArg, GetAddressUtxosReply, GetAddressUtxosReplyList,
    GetMempoolTxRequest, GetSubtreeRootsArg, LightdInfo, PingResponse, RawTransaction,
    SendResponse, SubtreeRoot, TransparentAddressBlockFilter, TreeState, TxFilter,
};

/// Boxed server-streaming response, shared by every streaming method's associated type.
type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

/// The gRPC service. Holds a client to the backend node and the block cache.
#[derive(Clone)]
pub struct Streamer {
    node: NodeClient,
    cache: Arc<Cache>,
}

impl Streamer {
    /// Build the service from a node client and a shared block cache.
    pub fn new(node: NodeClient, cache: Arc<Cache>) -> Self {
        Self { node, cache }
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
        // zebrad reports the hash in big-endian (display) hex; the wire format is little-endian.
        let mut hash = hex::decode(&info.bestblockhash)
            .map_err(|e| Status::internal(format!("decoding best block hash: {e}")))?;
        hash.reverse();
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
        let block = fetch::compact_block(&self.node, block_id.height).await?;
        Ok(Response::new(block))
    }

    async fn get_block_nullifiers(
        &self,
        _request: Request<BlockId>,
    ) -> Result<Response<CompactBlock>, Status> {
        Err(Status::unimplemented(
            "get_block_nullifiers: implemented in F4",
        ))
    }

    type GetBlockRangeStream = BoxStream<CompactBlock>;
    async fn get_block_range(
        &self,
        _request: Request<BlockRange>,
    ) -> Result<Response<Self::GetBlockRangeStream>, Status> {
        Err(Status::unimplemented("get_block_range: implemented in F2"))
    }

    type GetBlockRangeNullifiersStream = BoxStream<CompactBlock>;
    async fn get_block_range_nullifiers(
        &self,
        _request: Request<BlockRange>,
    ) -> Result<Response<Self::GetBlockRangeNullifiersStream>, Status> {
        Err(Status::unimplemented(
            "get_block_range_nullifiers: implemented in F4",
        ))
    }

    async fn get_transaction(
        &self,
        _request: Request<TxFilter>,
    ) -> Result<Response<RawTransaction>, Status> {
        Err(Status::unimplemented("get_transaction: implemented in F3"))
    }

    async fn send_transaction(
        &self,
        _request: Request<RawTransaction>,
    ) -> Result<Response<SendResponse>, Status> {
        Err(Status::unimplemented("send_transaction: implemented in F3"))
    }

    type GetTaddressTxidsStream = BoxStream<RawTransaction>;
    async fn get_taddress_txids(
        &self,
        _request: Request<TransparentAddressBlockFilter>,
    ) -> Result<Response<Self::GetTaddressTxidsStream>, Status> {
        Err(Status::unimplemented(
            "get_taddress_txids: implemented in F4",
        ))
    }

    type GetTaddressTransactionsStream = BoxStream<RawTransaction>;
    async fn get_taddress_transactions(
        &self,
        _request: Request<TransparentAddressBlockFilter>,
    ) -> Result<Response<Self::GetTaddressTransactionsStream>, Status> {
        Err(Status::unimplemented(
            "get_taddress_transactions: implemented in F4",
        ))
    }

    async fn get_taddress_balance(
        &self,
        _request: Request<AddressList>,
    ) -> Result<Response<Balance>, Status> {
        Err(Status::unimplemented(
            "get_taddress_balance: implemented in F3",
        ))
    }

    async fn get_taddress_balance_stream(
        &self,
        _request: Request<tonic::Streaming<Address>>,
    ) -> Result<Response<Balance>, Status> {
        Err(Status::unimplemented(
            "get_taddress_balance_stream: implemented in F3",
        ))
    }

    type GetMempoolTxStream = BoxStream<CompactTx>;
    async fn get_mempool_tx(
        &self,
        _request: Request<GetMempoolTxRequest>,
    ) -> Result<Response<Self::GetMempoolTxStream>, Status> {
        Err(Status::unimplemented("get_mempool_tx: implemented in F4"))
    }

    type GetMempoolStreamStream = BoxStream<RawTransaction>;
    async fn get_mempool_stream(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::GetMempoolStreamStream>, Status> {
        Err(Status::unimplemented(
            "get_mempool_stream: implemented in F4",
        ))
    }

    async fn get_tree_state(
        &self,
        _request: Request<BlockId>,
    ) -> Result<Response<TreeState>, Status> {
        Err(Status::unimplemented("get_tree_state: implemented in F3"))
    }

    async fn get_latest_tree_state(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<TreeState>, Status> {
        Err(Status::unimplemented(
            "get_latest_tree_state: implemented in F3",
        ))
    }

    type GetSubtreeRootsStream = BoxStream<SubtreeRoot>;
    async fn get_subtree_roots(
        &self,
        _request: Request<GetSubtreeRootsArg>,
    ) -> Result<Response<Self::GetSubtreeRootsStream>, Status> {
        Err(Status::unimplemented(
            "get_subtree_roots: implemented in F4",
        ))
    }

    async fn get_address_utxos(
        &self,
        _request: Request<GetAddressUtxosArg>,
    ) -> Result<Response<GetAddressUtxosReplyList>, Status> {
        Err(Status::unimplemented(
            "get_address_utxos: implemented in F3",
        ))
    }

    type GetAddressUtxosStreamStream = BoxStream<GetAddressUtxosReply>;
    async fn get_address_utxos_stream(
        &self,
        _request: Request<GetAddressUtxosArg>,
    ) -> Result<Response<Self::GetAddressUtxosStreamStream>, Status> {
        Err(Status::unimplemented(
            "get_address_utxos_stream: implemented in F3",
        ))
    }

    async fn ping(&self, _request: Request<Duration>) -> Result<Response<PingResponse>, Status> {
        Err(Status::unimplemented(
            "ping: testing-only, implemented in F3",
        ))
    }
}
