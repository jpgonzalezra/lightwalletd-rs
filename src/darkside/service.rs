//! [`DarksideService`]: the `DarksideStreamer` control-plane service that fabricates the mock chain.

use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::proto::darkside_streamer_server::DarksideStreamer;
use crate::proto::{
    BlockId, BoxStream, DarksideAddressTransaction, DarksideBlock, DarksideBlocksUrl,
    DarksideEmptyBlocks, DarksideHeight, DarksideMetaState, DarksideSubtreeRoots,
    DarksideTransactionsUrl, Empty, GetAddressUtxosReply, RawTransaction, TreeState,
};

use super::state::DarksideHandle;

/// The `DarksideStreamer` control-plane service. Shares state with [`DarksideNode`](super::DarksideNode)
/// and holds the shutdown notifier for `Stop`.
pub struct DarksideService {
    state: DarksideHandle,
    shutdown: Arc<tokio::sync::Notify>,
}

impl DarksideService {
    /// Build the control service over the shared `state` and a shutdown notifier.
    pub fn new(state: DarksideHandle, shutdown: Arc<tokio::sync::Notify>) -> Self {
        Self { state, shutdown }
    }
}

/// Fetch a URL's body as text (used by the URL-based staging RPCs).
async fn fetch_lines(url: &str) -> Result<String, Status> {
    reqwest::get(url)
        .await
        .map_err(|error| Status::unavailable(format!("fetch failed: {error}")))?
        .text()
        .await
        .map_err(|error| Status::unavailable(format!("reading body failed: {error}")))
}

/// Decode a hex `value` from a staging argument, mapping a failure to `bad {context} hex`.
fn decode_hex_arg(value: &str, context: &str) -> Result<Vec<u8>, Status> {
    hex::decode(value)
        .map_err(|error| Status::invalid_argument(format!("bad {context} hex: {error}")))
}

/// Parse a fetched staging body into raw bytes per non-empty line, decoding each as hex. A
/// `404: Not Found` body (what a missing URL returns) becomes a `not_found` error.
fn parse_hex_lines(body: &str, context: &str) -> Result<Vec<Vec<u8>>, Status> {
    let mut raws = Vec::new();
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if line == "404: Not Found" {
            return Err(Status::not_found(line.to_string()));
        }
        raws.push(decode_hex_arg(line, context)?);
    }
    Ok(raws)
}

#[tonic::async_trait]
impl DarksideStreamer for DarksideService {
    async fn reset(&self, request: Request<DarksideMetaState>) -> Result<Response<Empty>, Status> {
        let meta = request.into_inner();
        if meta.branch_id.is_empty() || !meta.branch_id.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(Status::invalid_argument(format!(
                "Reset: invalid BranchID (must be hex): {}",
                meta.branch_id
            )));
        }
        if meta.chain_name.is_empty() || !meta.chain_name.chars().all(|c| c.is_ascii_alphanumeric())
        {
            return Err(Status::invalid_argument("invalid chain name"));
        }
        self.state.lock().await.reset(&meta);
        Ok(Response::new(Empty {}))
    }

    async fn stage_blocks_stream(
        &self,
        request: Request<tonic::Streaming<DarksideBlock>>,
    ) -> Result<Response<Empty>, Status> {
        let mut stream = request.into_inner();
        while let Some(block) = stream.message().await? {
            let raw = decode_hex_arg(&block.block, "block")?;
            self.state.lock().await.stage_block(raw)?;
        }
        Ok(Response::new(Empty {}))
    }

    async fn stage_blocks(
        &self,
        request: Request<DarksideBlocksUrl>,
    ) -> Result<Response<Empty>, Status> {
        let body = fetch_lines(&request.into_inner().url).await?;
        let raws = parse_hex_lines(&body, "block")?;
        let mut state = self.state.lock().await;
        for raw in raws {
            state.stage_block(raw)?;
        }
        Ok(Response::new(Empty {}))
    }

    async fn stage_blocks_create(
        &self,
        request: Request<DarksideEmptyBlocks>,
    ) -> Result<Response<Empty>, Status> {
        let arg = request.into_inner();
        self.state
            .lock()
            .await
            .stage_blocks_create(arg.height, arg.nonce, arg.count)?;
        Ok(Response::new(Empty {}))
    }

    async fn stage_transactions_stream(
        &self,
        request: Request<tonic::Streaming<RawTransaction>>,
    ) -> Result<Response<Empty>, Status> {
        let mut stream = request.into_inner();
        while let Some(tx) = stream.message().await? {
            self.state
                .lock()
                .await
                .stage_transaction(tx.height, tx.data)?;
        }
        Ok(Response::new(Empty {}))
    }

    async fn stage_transactions(
        &self,
        request: Request<DarksideTransactionsUrl>,
    ) -> Result<Response<Empty>, Status> {
        let arg = request.into_inner();
        let body = fetch_lines(&arg.url).await?;
        let height = arg.height.max(0) as u64;
        let raws = parse_hex_lines(&body, "transaction")?;
        let mut state = self.state.lock().await;
        for raw in raws {
            state.stage_transaction(height, raw)?;
        }
        Ok(Response::new(Empty {}))
    }

    async fn apply_staged(
        &self,
        request: Request<DarksideHeight>,
    ) -> Result<Response<Empty>, Status> {
        let height = request.into_inner().height as i64;
        self.state.lock().await.apply_staged(height)?;
        Ok(Response::new(Empty {}))
    }

    type GetIncomingTransactionsStream = BoxStream<RawTransaction>;
    async fn get_incoming_transactions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::GetIncomingTransactionsStream>, Status> {
        let incoming = self.state.lock().await.incoming_txs.clone();
        let replies: Vec<Result<RawTransaction, Status>> = incoming
            .into_iter()
            .map(|data| Ok(RawTransaction { data, height: 0 }))
            .collect();
        Ok(Response::new(Box::pin(tokio_stream::iter(replies))))
    }

    async fn clear_incoming_transactions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Empty>, Status> {
        self.state.lock().await.clear_incoming();
        Ok(Response::new(Empty {}))
    }

    async fn add_address_utxo(
        &self,
        request: Request<GetAddressUtxosReply>,
    ) -> Result<Response<Empty>, Status> {
        self.state.lock().await.add_utxo(request.into_inner());
        Ok(Response::new(Empty {}))
    }

    async fn clear_address_utxo(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Empty>, Status> {
        self.state.lock().await.clear_utxos();
        Ok(Response::new(Empty {}))
    }

    async fn add_address_transaction(
        &self,
        request: Request<DarksideAddressTransaction>,
    ) -> Result<Response<Empty>, Status> {
        let arg = request.into_inner();
        self.state
            .lock()
            .await
            .add_addr_tx(arg.address, arg.data, arg.height);
        Ok(Response::new(Empty {}))
    }

    async fn clear_address_transactions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Empty>, Status> {
        self.state.lock().await.clear_addr_txs();
        Ok(Response::new(Empty {}))
    }

    async fn add_tree_state(&self, request: Request<TreeState>) -> Result<Response<Empty>, Status> {
        self.state
            .lock()
            .await
            .add_treestate(request.into_inner())?;
        Ok(Response::new(Empty {}))
    }

    async fn remove_tree_state(
        &self,
        request: Request<BlockId>,
    ) -> Result<Response<Empty>, Status> {
        let block_id = request.into_inner();
        self.state
            .lock()
            .await
            .remove_treestate(block_id.height, &block_id.hash);
        Ok(Response::new(Empty {}))
    }

    async fn clear_all_tree_states(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Empty>, Status> {
        self.state.lock().await.clear_treestates();
        Ok(Response::new(Empty {}))
    }

    async fn set_subtree_roots(
        &self,
        request: Request<DarksideSubtreeRoots>,
    ) -> Result<Response<Empty>, Status> {
        self.state
            .lock()
            .await
            .set_subtree_roots(request.into_inner());
        Ok(Response::new(Empty {}))
    }

    async fn stop(&self, _request: Request<Empty>) -> Result<Response<Empty>, Status> {
        tracing::info!("stop requested via gRPC");
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            // Let the reply reach the client before the server stops accepting connections.
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            shutdown.notify_one();
        });
        Ok(Response::new(Empty {}))
    }
}
