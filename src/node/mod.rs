//! JSON-RPC client for the backend zebrad node.
//!
//! Exposes a generic [`NodeClient::raw_request`] plus typed wrappers for the specific RPCs the
//! service needs. The transport is plain HTTP `POST` with HTTP Basic auth.

mod types;

pub use types::{
    AddressUtxo, GetAddressBalance, GetBlockVerbose, GetBlockchainInfo, GetInfo, GetRawTransaction,
    GetTreeState,
};

use serde::{Deserialize, Serialize};

use crate::config::NodeConfig;

/// Errors returned by the node client.
#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    /// The HTTP request itself failed (connection, timeout, decoding the body).
    #[error("node HTTP transport error: {0}")]
    Http(#[from] reqwest::Error),
    /// The node returned a JSON-RPC error object.
    #[error("node RPC error {code}: {message}")]
    Rpc {
        /// JSON-RPC error code.
        code: i64,
        /// Human-readable error message.
        message: String,
    },
    /// The JSON-RPC `result` could not be decoded into the expected type.
    #[error("decoding RPC result: {0}")]
    Decode(#[from] serde_json::Error),
    /// A hex-encoded field could not be decoded.
    #[error("decoding hex: {0}")]
    Hex(#[from] hex::FromHexError),
    /// The response had neither a `result` nor an `error`.
    #[error("RPC response had no result")]
    EmptyResult,
}

/// A client for the zebrad JSON-RPC endpoint.
#[derive(Debug, Clone)]
pub struct NodeClient {
    http: reqwest::Client,
    url: String,
    user: String,
    password: String,
}

impl NodeClient {
    /// Build a client from the resolved node configuration.
    pub fn new(config: &NodeConfig) -> Self {
        Self {
            http: reqwest::Client::new(),
            url: config.url.clone(),
            user: config.user.clone(),
            password: config.password.clone(),
        }
    }

    /// Issue a raw JSON-RPC call and return the decoded `result` value.
    pub async fn raw_request(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, NodeError> {
        let request = RpcRequest {
            jsonrpc: "1.0",
            id: "lwd",
            method,
            params,
        };
        let response: RpcResponse = self
            .http
            .post(&self.url)
            .basic_auth(&self.user, Some(&self.password))
            .json(&request)
            .send()
            .await?
            .json()
            .await?;

        if let Some(error) = response.error {
            return Err(NodeError::Rpc {
                code: error.code,
                message: error.message,
            });
        }
        response.result.ok_or(NodeError::EmptyResult)
    }

    /// Call `getinfo`.
    pub async fn get_info(&self) -> Result<GetInfo, NodeError> {
        let value = self.raw_request("getinfo", serde_json::json!([])).await?;
        Ok(serde_json::from_value(value)?)
    }

    /// Call `getblockchaininfo`.
    pub async fn get_blockchain_info(&self) -> Result<GetBlockchainInfo, NodeError> {
        let value = self
            .raw_request("getblockchaininfo", serde_json::json!([]))
            .await?;
        Ok(serde_json::from_value(value)?)
    }

    /// Call `getblock <height> 1` (verbose) to obtain the block hash and tree sizes.
    pub async fn get_block_verbose(&self, height: u64) -> Result<GetBlockVerbose, NodeError> {
        let value = self
            .raw_request("getblock", serde_json::json!([height.to_string(), 1]))
            .await?;
        Ok(serde_json::from_value(value)?)
    }

    /// Call `getblockcount` to get the height of the best chain tip.
    pub async fn get_block_count(&self) -> Result<u64, NodeError> {
        let value = self
            .raw_request("getblockcount", serde_json::json!([]))
            .await?;
        Ok(serde_json::from_value(value)?)
    }

    /// Call `getblock <hash> 0` (raw) and return the decoded block bytes.
    pub async fn get_block_raw(&self, hash: &str) -> Result<Vec<u8>, NodeError> {
        let value = self
            .raw_request("getblock", serde_json::json!([hash, 0]))
            .await?;
        let hex_str: String = serde_json::from_value(value)?;
        Ok(hex::decode(hex_str)?)
    }

    /// Call `getrawtransaction <txid> 1` (verbose) for a transaction's bytes and mined height.
    pub async fn get_raw_transaction(&self, txid: &str) -> Result<GetRawTransaction, NodeError> {
        let value = self
            .raw_request("getrawtransaction", serde_json::json!([txid, 1]))
            .await?;
        Ok(serde_json::from_value(value)?)
    }

    /// Call `sendrawtransaction <hex>` and return the resulting txid on success.
    pub async fn send_raw_transaction(&self, hex: &str) -> Result<String, NodeError> {
        let value = self
            .raw_request("sendrawtransaction", serde_json::json!([hex]))
            .await?;
        Ok(serde_json::from_value(value)?)
    }

    /// Call `z_gettreestate <id>` for the note-commitment tree state, where `id` is a height or hash.
    pub async fn get_treestate(&self, id: &str) -> Result<GetTreeState, NodeError> {
        let value = self
            .raw_request("z_gettreestate", serde_json::json!([id]))
            .await?;
        Ok(serde_json::from_value(value)?)
    }

    /// Call `getaddressbalance` for the combined balance of the given transparent addresses.
    pub async fn get_address_balance(
        &self,
        addresses: &[String],
    ) -> Result<GetAddressBalance, NodeError> {
        let value = self
            .raw_request(
                "getaddressbalance",
                serde_json::json!([{ "addresses": addresses }]),
            )
            .await?;
        Ok(serde_json::from_value(value)?)
    }

    /// Call `getaddressutxos` for the unspent outputs of the given transparent addresses.
    pub async fn get_address_utxos(
        &self,
        addresses: &[String],
    ) -> Result<Vec<AddressUtxo>, NodeError> {
        let value = self
            .raw_request(
                "getaddressutxos",
                serde_json::json!([{ "addresses": addresses }]),
            )
            .await?;
        Ok(serde_json::from_value(value)?)
    }
}

/// JSON-RPC request envelope.
#[derive(Serialize)]
struct RpcRequest<'a> {
    jsonrpc: &'a str,
    id: &'a str,
    method: &'a str,
    params: serde_json::Value,
}

/// JSON-RPC response envelope.
#[derive(Deserialize)]
struct RpcResponse {
    result: Option<serde_json::Value>,
    error: Option<RpcErrorObject>,
}

/// JSON-RPC error object.
#[derive(Deserialize)]
struct RpcErrorObject {
    code: i64,
    message: String,
}
