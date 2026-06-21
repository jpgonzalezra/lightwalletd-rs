//! Implementation of the `CompactTxStreamer` gRPC service.
//!
//! All `CompactTxStreamer` methods are implemented: chain info, block serving (`GetBlock`/
//! `GetBlockRange` and their nullifier-only variants), `GetTransaction`, `SendTransaction`, tree
//! state, transparent-address balance/UTXOs/transaction listings, subtree roots, the mempool streams,
//! and `Ping`. Block/tree-state lookup by hash (rather than height) is the one sub-case still
//! returning `unimplemented`.
//!
//! The trait implementation is a thin dispatcher: each method delegates to a free function in the
//! submodule that owns its method family (`chain`, `blocks`, `transactions`, `address`, `mempool`,
//! `treestate`, `subtrees`, `ping`).

use std::sync::Arc;
use std::sync::atomic::AtomicI64;

use tonic::{Request, Response, Status};

use crate::cache::Cache;
use crate::fetch;
use crate::node::NodeRpc;
use crate::proto::compact_tx_streamer_server::CompactTxStreamer;
use crate::proto::{
    Address, AddressList, Balance, BlockId, BlockRange, BoxStream, ChainSpec, CompactBlock,
    CompactTx, Duration, Empty, GetAddressUtxosArg, GetAddressUtxosReply, GetAddressUtxosReplyList,
    GetMempoolTxRequest, GetSubtreeRootsArg, LightdInfo, PingResponse, RawTransaction,
    SendResponse, SubtreeRoot, TransparentAddressBlockFilter, TreeState, TxFilter,
};

mod address;
mod blocks;
mod chain;
mod errors;
mod mempool;
mod ping;
mod subtrees;
mod transactions;
mod treestate;

#[cfg(test)]
mod tests;

/// The gRPC service. Holds a client to the backend node, the block cache, and the network name.
#[derive(Clone)]
pub struct Streamer {
    node: Arc<dyn NodeRpc>,
    cache: Arc<Cache>,
    network: String,
    /// In darkside mode, the shared mock state used to serve `GetSubtreeRoots` directly; `None` live.
    darkside: Option<crate::darkside::DarksideHandle>,
    /// Number of `Ping` calls currently in flight, shared across cloned services (testing only).
    ping_count: Arc<AtomicI64>,
}

impl Streamer {
    /// Build the service from a node client, a shared block cache, and the network name. `darkside`
    /// is `Some` only in darkside mode, where it overrides `GetSubtreeRoots`.
    pub fn new(
        node: Arc<dyn NodeRpc>,
        cache: Arc<Cache>,
        network: String,
        darkside: Option<crate::darkside::DarksideHandle>,
    ) -> Self {
        Self {
            node,
            cache,
            network,
            darkside,
            ping_count: Arc::new(AtomicI64::new(0)),
        }
    }
}

/// Read the compact block at `height` from the cache, falling back to the node on a miss.
async fn block_at(cache: &Cache, node: &dyn NodeRpc, height: u64) -> Result<CompactBlock, Status> {
    match cache.get(height)? {
        Some(block) => Ok(block),
        None => fetch::compact_block(node, height)
            .await
            .map_err(|err| errors::block_fetch_to_status(err, height)),
    }
}

/// Decode a hex string, tagging a failure as an internal error mentioning `context`.
fn decode_hex(value: &str, context: &str) -> Result<Vec<u8>, Status> {
    hex::decode(value).map_err(|error| Status::internal(format!("decoding {context}: {error}")))
}

/// Map a node-reported height to the gRPC convention: a negative (off-chain) height becomes `u64::MAX`.
fn mined_height(height: i64) -> u64 {
    if height < 0 { u64::MAX } else { height as u64 }
}

#[tonic::async_trait]
impl CompactTxStreamer for Streamer {
    async fn get_latest_block(
        &self,
        _request: Request<ChainSpec>,
    ) -> Result<Response<BlockId>, Status> {
        chain::get_latest_block(self).await
    }

    async fn get_lightd_info(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<LightdInfo>, Status> {
        chain::get_lightd_info(self).await
    }

    async fn get_block(&self, request: Request<BlockId>) -> Result<Response<CompactBlock>, Status> {
        blocks::get_block(self, request).await
    }

    async fn get_block_nullifiers(
        &self,
        request: Request<BlockId>,
    ) -> Result<Response<CompactBlock>, Status> {
        blocks::get_block_nullifiers(self, request).await
    }

    type GetBlockRangeStream = BoxStream<CompactBlock>;
    async fn get_block_range(
        &self,
        request: Request<BlockRange>,
    ) -> Result<Response<Self::GetBlockRangeStream>, Status> {
        blocks::get_block_range(self, request).await
    }

    type GetBlockRangeNullifiersStream = BoxStream<CompactBlock>;
    async fn get_block_range_nullifiers(
        &self,
        request: Request<BlockRange>,
    ) -> Result<Response<Self::GetBlockRangeNullifiersStream>, Status> {
        blocks::get_block_range_nullifiers(self, request).await
    }

    async fn get_transaction(
        &self,
        request: Request<TxFilter>,
    ) -> Result<Response<RawTransaction>, Status> {
        transactions::get_transaction(self, request).await
    }

    async fn send_transaction(
        &self,
        request: Request<RawTransaction>,
    ) -> Result<Response<SendResponse>, Status> {
        transactions::send_transaction(self, request).await
    }

    type GetTaddressTxidsStream = BoxStream<RawTransaction>;
    async fn get_taddress_txids(
        &self,
        request: Request<TransparentAddressBlockFilter>,
    ) -> Result<Response<Self::GetTaddressTxidsStream>, Status> {
        address::get_taddress_txids(self, request).await
    }

    type GetTaddressTransactionsStream = BoxStream<RawTransaction>;
    async fn get_taddress_transactions(
        &self,
        request: Request<TransparentAddressBlockFilter>,
    ) -> Result<Response<Self::GetTaddressTransactionsStream>, Status> {
        address::get_taddress_transactions(self, request).await
    }

    async fn get_taddress_balance(
        &self,
        request: Request<AddressList>,
    ) -> Result<Response<Balance>, Status> {
        address::get_taddress_balance(self, request).await
    }

    async fn get_taddress_balance_stream(
        &self,
        request: Request<tonic::Streaming<Address>>,
    ) -> Result<Response<Balance>, Status> {
        address::get_taddress_balance_stream(self, request).await
    }

    type GetMempoolTxStream = BoxStream<CompactTx>;
    async fn get_mempool_tx(
        &self,
        request: Request<GetMempoolTxRequest>,
    ) -> Result<Response<Self::GetMempoolTxStream>, Status> {
        mempool::get_mempool_tx(self, request).await
    }

    type GetMempoolStreamStream = BoxStream<RawTransaction>;
    async fn get_mempool_stream(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::GetMempoolStreamStream>, Status> {
        mempool::get_mempool_stream(self).await
    }

    async fn get_tree_state(
        &self,
        request: Request<BlockId>,
    ) -> Result<Response<TreeState>, Status> {
        treestate::get_tree_state(self, request).await
    }

    async fn get_latest_tree_state(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<TreeState>, Status> {
        treestate::get_latest_tree_state(self).await
    }

    type GetSubtreeRootsStream = BoxStream<SubtreeRoot>;
    async fn get_subtree_roots(
        &self,
        request: Request<GetSubtreeRootsArg>,
    ) -> Result<Response<Self::GetSubtreeRootsStream>, Status> {
        subtrees::get_subtree_roots(self, request).await
    }

    async fn get_address_utxos(
        &self,
        request: Request<GetAddressUtxosArg>,
    ) -> Result<Response<GetAddressUtxosReplyList>, Status> {
        address::get_address_utxos(self, request).await
    }

    type GetAddressUtxosStreamStream = BoxStream<GetAddressUtxosReply>;
    async fn get_address_utxos_stream(
        &self,
        request: Request<GetAddressUtxosArg>,
    ) -> Result<Response<Self::GetAddressUtxosStreamStream>, Status> {
        address::get_address_utxos_stream(self, request).await
    }

    async fn ping(&self, request: Request<Duration>) -> Result<Response<PingResponse>, Status> {
        ping::ping(self, request).await
    }
}
