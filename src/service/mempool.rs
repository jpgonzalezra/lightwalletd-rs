//! Mempool streams: `GetMempoolTx` (compact transactions) and `GetMempoolStream` (raw transactions).

use std::collections::HashSet;
use std::time::Duration;

use async_stream::try_stream;
use tonic::{Request, Response, Status};

use crate::compact;
use crate::encoding;
use crate::filter;
use crate::proto::{BoxStream, CompactTx, GetMempoolTxRequest, RawTransaction};

use super::{Streamer, decode_hex};

pub(super) async fn get_mempool_tx(
    streamer: &Streamer,
    request: Request<GetMempoolTxRequest>,
) -> Result<Response<BoxStream<CompactTx>>, Status> {
    let mempool_request = request.into_inner();
    let exclude = mempool_request.exclude_txid_suffixes;
    let pool_types = mempool_request.pool_types;
    let txids = streamer.node.get_raw_mempool().await?;
    let node = streamer.node.clone();

    let stream = try_stream! {
        let pools = filter::Pools::from_pool_types(&pool_types);
        for (index, txid) in txids.into_iter().enumerate() {
            // The txid is display-order hex; exclusion suffixes are compared in protocol order.
            let wire_txid = encoding::display_hex_to_wire(&txid)
                .map_err(|e| Status::internal(format!("decoding mempool txid: {e}")))?;
            if exclude.iter().any(|suffix| wire_txid.ends_with(suffix)) {
                continue;
            }
            let raw = node.get_raw_transaction(&txid).await?;
            let bytes = decode_hex(&raw.hex, "mempool tx")?;
            let mut compact = compact::compact_tx_from_raw(index as u64, &bytes)
                .map_err(|e| Status::internal(format!("parsing mempool tx: {e}")))?;
            filter::filter_tx_to_pools(&mut compact, pools);
            yield compact;
        }
    };
    Ok(Response::new(Box::pin(stream)))
}

pub(super) async fn get_mempool_stream(
    streamer: &Streamer,
) -> Result<Response<BoxStream<RawTransaction>>, Status> {
    let node = streamer.node.clone();
    let stream = try_stream! {
        // Snapshot the tip; when it changes a new block was mined and we end the stream.
        let start = node.get_blockchain_info().await?;
        let height = start.blocks;
        let mut seen = HashSet::new();
        loop {
            if node.get_blockchain_info().await?.bestblockhash != start.bestblockhash {
                break;
            }
            for txid in node.get_raw_mempool().await? {
                if !seen.insert(txid.clone()) {
                    continue;
                }
                let raw = node.get_raw_transaction(&txid).await?;
                // A non-zero height means the tx is already mined, not in the mempool.
                if raw.height != 0 {
                    continue;
                }
                let data = decode_hex(&raw.hex, "mempool tx")?;
                yield RawTransaction { data, height };
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    };
    Ok(Response::new(Box::pin(stream)))
}
