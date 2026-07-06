//! JSON-RPC client for the backend zebrad node.
//!
//! The [`NodeRpc`] trait is the typed RPC surface the service, ingestor, and fetch depend on;
//! [`NodeClient`] implements it over a generic [`NodeClient::raw_request`]. The transport is plain HTTP
//! `POST` with HTTP Basic auth.

mod types;

pub use types::{
    AddressUtxo, Consensus, GetAddressBalance, GetBlockVerbose, GetBlockchainInfo, GetInfo,
    GetRawTransaction, GetSubtrees, GetTreeState, TreeCommitments, TreePool, TreeSize, Trees,
    Upgrade,
};

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::config::NodeConfig;

/// Per-call request timeout: generously above the slowest legitimate call (a verbose `getblock` /
/// `getrawtransaction`), so a stalled node surfaces as a retryable error instead of hanging forever.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// TCP connect timeout for the node HTTP client.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

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
#[derive(Clone)]
pub struct NodeClient {
    http: reqwest::Client,
    url: String,
    user: String,
    password: String,
}

impl std::fmt::Debug for NodeClient {
    /// Hand-written so a stray `{:?}` on the client can never leak the node credential into a log.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeClient")
            .field("url", &self.url)
            .field("user", &self.user)
            .field("password", &"***")
            .finish()
    }
}

/// The typed surface of the backend node's JSON-RPC API.
///
/// Abstracts [`NodeClient`] so the service, ingestor, and fetch logic can be tested against a fake.
/// The generic `raw_request` stays inherent to `NodeClient`; only the typed wrappers belong to the trait.
#[async_trait::async_trait]
pub trait NodeRpc: Send + Sync {
    /// Call `getinfo`.
    async fn get_info(&self) -> Result<GetInfo, NodeError>;
    /// Call `getblockchaininfo`.
    async fn get_blockchain_info(&self) -> Result<GetBlockchainInfo, NodeError>;
    /// Call `getblock <height> 1` (verbose) to obtain the block hash and tree sizes.
    async fn get_block_verbose(&self, height: u64) -> Result<GetBlockVerbose, NodeError>;
    /// Call `getblockcount` to get the height of the best chain tip.
    async fn get_block_count(&self) -> Result<u64, NodeError>;
    /// Call `getblock <hash> 0` (raw) and return the decoded block bytes.
    async fn get_block_raw(&self, hash: &str) -> Result<Vec<u8>, NodeError>;
    /// Call `getrawtransaction <txid> 1` (verbose) for a transaction's bytes and mined height.
    async fn get_raw_transaction(&self, txid: &str) -> Result<GetRawTransaction, NodeError>;
    /// Call `sendrawtransaction <hex>` and return the resulting txid on success.
    async fn send_raw_transaction(&self, hex: &str) -> Result<String, NodeError>;
    /// Call `z_gettreestate <id>` for the note-commitment tree state, where `id` is a height or hash.
    async fn get_treestate(&self, id: &str) -> Result<GetTreeState, NodeError>;
    /// Call `getaddressbalance` for the combined balance of the given transparent addresses.
    async fn get_address_balance(
        &self,
        addresses: &[String],
    ) -> Result<GetAddressBalance, NodeError>;
    /// Call `getaddressutxos` for the unspent outputs of the given transparent addresses.
    async fn get_address_utxos(&self, addresses: &[String]) -> Result<Vec<AddressUtxo>, NodeError>;
    /// Call `getaddresstxids` for the txids touching the given addresses within `[start, end]`.
    async fn get_address_txids(
        &self,
        addresses: &[String],
        start: u64,
        end: u64,
    ) -> Result<Vec<String>, NodeError>;
    /// Call `z_getsubtreesbyindex` for note-commitment subtrees of a shielded `protocol`
    /// (`"sapling"` or `"orchard"`), starting at `start_index` (`max_entries == 0` means no limit).
    async fn get_subtrees(
        &self,
        protocol: &str,
        start_index: u32,
        max_entries: u32,
    ) -> Result<GetSubtrees, NodeError>;
    /// Call `getrawmempool` for the txids currently in the mempool.
    async fn get_raw_mempool(&self) -> Result<Vec<String>, NodeError>;
}

impl NodeClient {
    /// Build a client from the resolved node configuration, using the default request and connect
    /// timeouts.
    pub fn new(config: &NodeConfig) -> Result<Self, reqwest::Error> {
        Self::with_timeouts(config, REQUEST_TIMEOUT, CONNECT_TIMEOUT)
    }

    /// Build a client with explicit request and connect timeouts.
    fn with_timeouts(
        config: &NodeConfig,
        request_timeout: Duration,
        connect_timeout: Duration,
    ) -> Result<Self, reqwest::Error> {
        let http = reqwest::Client::builder()
            .timeout(request_timeout)
            .connect_timeout(connect_timeout)
            .build()?;
        Ok(Self {
            http,
            url: config.url.clone(),
            user: config.user.clone(),
            password: config.password.clone(),
        })
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

    /// Issue a JSON-RPC call and deserialize its `result` into `T`.
    async fn request<T: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T, NodeError> {
        let value = self.raw_request(method, params).await?;
        Ok(serde_json::from_value(value)?)
    }
}

#[async_trait::async_trait]
impl NodeRpc for NodeClient {
    async fn get_info(&self) -> Result<GetInfo, NodeError> {
        self.request("getinfo", serde_json::json!([])).await
    }

    async fn get_blockchain_info(&self) -> Result<GetBlockchainInfo, NodeError> {
        self.request("getblockchaininfo", serde_json::json!([]))
            .await
    }

    async fn get_block_verbose(&self, height: u64) -> Result<GetBlockVerbose, NodeError> {
        self.request("getblock", serde_json::json!([height.to_string(), 1]))
            .await
    }

    async fn get_block_count(&self) -> Result<u64, NodeError> {
        self.request("getblockcount", serde_json::json!([])).await
    }

    async fn get_block_raw(&self, hash: &str) -> Result<Vec<u8>, NodeError> {
        let hex_str: String = self
            .request("getblock", serde_json::json!([hash, 0]))
            .await?;
        Ok(hex::decode(hex_str)?)
    }

    async fn get_raw_transaction(&self, txid: &str) -> Result<GetRawTransaction, NodeError> {
        self.request("getrawtransaction", serde_json::json!([txid, 1]))
            .await
    }

    async fn send_raw_transaction(&self, hex: &str) -> Result<String, NodeError> {
        self.request("sendrawtransaction", serde_json::json!([hex]))
            .await
    }

    async fn get_treestate(&self, id: &str) -> Result<GetTreeState, NodeError> {
        self.request("z_gettreestate", serde_json::json!([id]))
            .await
    }

    async fn get_address_balance(
        &self,
        addresses: &[String],
    ) -> Result<GetAddressBalance, NodeError> {
        self.request(
            "getaddressbalance",
            serde_json::json!([{ "addresses": addresses }]),
        )
        .await
    }

    async fn get_address_utxos(&self, addresses: &[String]) -> Result<Vec<AddressUtxo>, NodeError> {
        self.request(
            "getaddressutxos",
            serde_json::json!([{ "addresses": addresses }]),
        )
        .await
    }

    async fn get_address_txids(
        &self,
        addresses: &[String],
        start: u64,
        end: u64,
    ) -> Result<Vec<String>, NodeError> {
        self.request(
            "getaddresstxids",
            serde_json::json!([{ "addresses": addresses, "start": start, "end": end }]),
        )
        .await
    }

    async fn get_subtrees(
        &self,
        protocol: &str,
        start_index: u32,
        max_entries: u32,
    ) -> Result<GetSubtrees, NodeError> {
        let params = if max_entries > 0 {
            serde_json::json!([protocol, start_index, max_entries])
        } else {
            serde_json::json!([protocol, start_index])
        };
        self.request("z_getsubtreesbyindex", params).await
    }

    async fn get_raw_mempool(&self) -> Result<Vec<String>, NodeError> {
        self.request("getrawmempool", serde_json::json!([])).await
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

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{header, method};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn client_for(server: &MockServer) -> NodeClient {
        NodeClient::new(&NodeConfig {
            url: server.uri(),
            user: "rpcuser".to_string(),
            password: "rpcpass".to_string(),
        })
        .unwrap()
    }

    async fn mock_response(body: serde_json::Value) -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;
        server
    }

    #[tokio::test]
    async fn raw_request_returns_the_result_value() {
        let server = mock_response(serde_json::json!({ "result": { "foo": 1 } })).await;
        let value = client_for(&server)
            .raw_request("anything", serde_json::json!([]))
            .await
            .unwrap();
        assert_eq!(value, serde_json::json!({ "foo": 1 }));
    }

    #[tokio::test]
    async fn raw_request_maps_jsonrpc_error_to_rpc_variant() {
        let server = mock_response(
            serde_json::json!({ "error": { "code": -8, "message": "out of range" } }),
        )
        .await;
        let error = client_for(&server)
            .raw_request("getblock", serde_json::json!([]))
            .await
            .unwrap_err();
        assert!(matches!(error, NodeError::Rpc { code: -8, message } if message == "out of range"));
    }

    #[tokio::test]
    async fn raw_request_without_result_or_error_is_empty_result() {
        let server = mock_response(serde_json::json!({ "id": "lwd" })).await;
        let error = client_for(&server)
            .raw_request("getinfo", serde_json::json!([]))
            .await
            .unwrap_err();
        assert!(matches!(error, NodeError::EmptyResult));
    }

    #[tokio::test]
    async fn raw_request_sends_the_basic_auth_header() {
        let server = MockServer::start().await;
        // Mounted with the expected `Authorization` header; a mismatch yields a 404 and fails below.
        Mock::given(method("POST"))
            .and(header("authorization", "Basic cnBjdXNlcjpycGNwYXNz"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": "ok",
            })))
            .mount(&server)
            .await;
        let value = client_for(&server)
            .raw_request("getinfo", serde_json::json!([]))
            .await
            .unwrap();
        assert_eq!(value, serde_json::json!("ok"));
    }

    #[tokio::test]
    async fn get_blockchain_info_deserializes_the_typed_response() {
        let server = mock_response(serde_json::json!({
            "result": {
                "chain": "main",
                "blocks": 12345,
                "bestblockhash": "abcd",
                "consensus": { "chaintip": "5437f330" },
            },
        }))
        .await;
        let info = client_for(&server).get_blockchain_info().await.unwrap();
        assert_eq!(info.chain, "main");
        assert_eq!(info.blocks, 12345);
        assert_eq!(info.bestblockhash, "abcd");
        assert_eq!(info.consensus.chaintip, "5437f330");
    }

    #[tokio::test]
    async fn get_block_raw_hex_decodes_the_result() {
        let server = mock_response(serde_json::json!({ "result": "deadbeef" })).await;
        let bytes = client_for(&server).get_block_raw("somehash").await.unwrap();
        assert_eq!(bytes, vec![0xde, 0xad, 0xbe, 0xef]);
    }

    #[test]
    fn node_client_debug_redacts_password() {
        let client = NodeClient::new(&NodeConfig {
            url: "http://127.0.0.1:8232".to_string(),
            user: "rpcuser".to_string(),
            password: "supersecret".to_string(),
        })
        .unwrap();
        let rendered = format!("{client:?}");
        assert!(rendered.contains("***"));
        assert!(!rendered.contains("supersecret"));
    }

    #[tokio::test(start_paused = true)]
    async fn raw_request_times_out_on_a_stalled_node_instead_of_hanging() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({ "result": "ok" }))
                    .set_delay(Duration::from_secs(60)),
            )
            .mount(&server)
            .await;
        let client = NodeClient::with_timeouts(
            &NodeConfig {
                url: server.uri(),
                user: "rpcuser".to_string(),
                password: "rpcpass".to_string(),
            },
            Duration::from_millis(50),
            Duration::from_millis(50),
        )
        .unwrap();

        let error = client
            .raw_request("getinfo", serde_json::json!([]))
            .await
            .unwrap_err();

        assert!(matches!(error, NodeError::Http(_)));
    }
}
