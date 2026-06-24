//! Single-transaction methods: `GetTransaction` and `SendTransaction`.

use tonic::{Request, Response, Status};

use crate::encoding;
use crate::node::NodeError;
use crate::proto::{RawTransaction, SendResponse, TxFilter};

use super::{Streamer, decode_hex, mined_height};

pub(super) async fn get_transaction(
    streamer: &Streamer,
    request: Request<TxFilter>,
) -> Result<Response<RawTransaction>, Status> {
    let filter = request.into_inner();
    if filter.hash.is_empty() {
        if filter.block.is_some_and(|block| !block.hash.is_empty()) {
            return Err(Status::invalid_argument(
                "get_transaction: specify a txid, not a blockhash",
            ));
        }
        return Err(Status::invalid_argument("get_transaction: specify a txid"));
    }
    if filter.hash.len() != 32 {
        return Err(Status::invalid_argument(format!(
            "get_transaction: transaction ID has invalid length: {}",
            filter.hash.len()
        )));
    }
    let txid = encoding::wire_to_display_hex(&filter.hash);
    let raw = streamer
        .node
        .get_raw_transaction(&txid)
        .await
        .map_err(super::errors::transaction_lookup_to_status)?;
    let data = decode_hex(&raw.hex, "transaction hex")?;
    Ok(Response::new(RawTransaction {
        data,
        height: mined_height(raw.height),
    }))
}

pub(super) async fn send_transaction(
    streamer: &Streamer,
    request: Request<RawTransaction>,
) -> Result<Response<SendResponse>, Status> {
    let raw_transaction = request.into_inner();
    if raw_transaction.data.is_empty() {
        return Err(Status::invalid_argument(
            "send_transaction: bad transaction data",
        ));
    }
    match streamer
        .node
        .send_raw_transaction(&hex::encode(&raw_transaction.data))
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
