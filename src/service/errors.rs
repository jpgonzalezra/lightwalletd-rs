//! Translating backend-node errors into gRPC `Status` codes.
//!
//! The generic `From` impls give the safe default (`Unavailable` for a node/transport failure,
//! `Internal` for a parse/decode failure). The per-method helpers below upgrade specific JSON-RPC
//! error codes to the status a wallet expects. The numeric code — not the message — is authoritative,
//! since the node's error messages are not stable across versions.

use tonic::Status;

use crate::cache::CacheError;
use crate::fetch::FetchError;
use crate::node::NodeError;

/// `getblock` for a height past the chain tip (the node reports "block height not in best chain").
const RPC_INVALID_PARAMETER: i64 = -8;
/// Missing transaction or unparseable address. The node returns it for `getrawtransaction` on an
/// unknown txid and for the address RPCs on a malformed address.
const RPC_INVALID_ADDRESS_OR_KEY: i64 = -5;

impl From<NodeError> for Status {
    fn from(err: NodeError) -> Self {
        // `NodeError::Http` wraps a `reqwest::Error`, whose Display can include the backend node's
        // URL (and any userinfo credentials embedded in `--rpc-url`). Keep that detail server-side
        // and hand the client a generic message instead.
        if let NodeError::Http(_) = err {
            tracing::warn!(%err, "node transport error");
            return Status::unavailable("backend node unavailable");
        }
        Status::unavailable(err.to_string())
    }
}

impl From<FetchError> for Status {
    fn from(err: FetchError) -> Self {
        match err {
            FetchError::Node(e) => e.into(),
            FetchError::Parse(e) => Status::internal(e.to_string()),
            FetchError::UnexpectedHeight { requested, got } => Status::unavailable(format!(
                "node returned block at height {got}, expected {requested}"
            )),
            FetchError::HashMismatch {
                requested,
                computed,
            } => Status::unavailable(format!(
                "node returned bytes hashing to {computed}, expected {requested}"
            )),
            // A txid/tx-count divergence between our parser and the node is an integrity failure on
            // our side of the trust boundary, not a transient node condition: not retryable.
            err @ (FetchError::TxidMismatch { .. } | FetchError::TxCountMismatch { .. }) => {
                Status::internal(err.to_string())
            }
            FetchError::ParseTask(e) => Status::internal(e.to_string()),
        }
    }
}

impl From<CacheError> for Status {
    fn from(err: CacheError) -> Self {
        Status::internal(err.to_string())
    }
}

/// Map a block-fetch failure: a `-8` (height past the tip) becomes `OutOfRange`; everything else keeps
/// the default mapping.
pub(super) fn block_fetch_to_status(err: FetchError, height: u64) -> Status {
    if let FetchError::Node(NodeError::Rpc {
        code: RPC_INVALID_PARAMETER,
        ..
    }) = err
    {
        return Status::out_of_range(format!("block {height} is newer than the latest block"));
    }
    err.into()
}

/// Map a `getrawtransaction` failure: a `-5` (unknown txid) becomes `NotFound`.
pub(super) fn transaction_lookup_to_status(err: NodeError) -> Status {
    if let NodeError::Rpc {
        code: RPC_INVALID_ADDRESS_OR_KEY,
        ref message,
    } = err
    {
        return Status::not_found(format!("transaction not found: {message}"));
    }
    err.into()
}

/// Map an address-RPC failure: a `-5` is a malformed address (`InvalidArgument`), except the
/// "No information available" case (`NotFound`). The node reports `-5` "parse error: invalid Bech32
/// encoding" for a bad address; the message branch is a safety net for nodes that distinguish the
/// not-found case.
pub(super) fn address_query_to_status(err: NodeError) -> Status {
    if let NodeError::Rpc {
        code: RPC_INVALID_ADDRESS_OR_KEY,
        ref message,
    } = err
    {
        if message.contains("No information available") {
            return Status::not_found(message.clone());
        }
        return Status::invalid_argument(format!("invalid transparent address: {message}"));
    }
    err.into()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::config::NodeConfig;
    use crate::node::NodeClient;

    /// Produce a real `NodeError::Http` (a request timeout against a stalled mock node) plus the
    /// mock server's URI, to assert the URI never reaches a client-facing `Status`.
    async fn http_transport_error() -> (NodeError, String) {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(60)))
            .mount(&server)
            .await;
        let client = NodeClient::new(&NodeConfig {
            url: server.uri(),
            user: "rpcuser".to_string(),
            password: "rpcpass".to_string(),
        })
        .unwrap();

        let error = client
            .raw_request("getinfo", serde_json::json!([]))
            .await
            .unwrap_err();
        assert!(matches!(error, NodeError::Http(_)));
        (error, server.uri())
    }

    #[tokio::test(start_paused = true)]
    async fn node_http_transport_error_maps_to_generic_unavailable_status() {
        let (error, server_uri) = http_transport_error().await;

        let status: Status = error.into();

        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(status.message(), "backend node unavailable");
        assert!(!status.message().contains(&server_uri));
    }

    #[tokio::test(start_paused = true)]
    async fn fetch_error_wrapping_http_transport_error_maps_to_generic_unavailable_status() {
        let (error, server_uri) = http_transport_error().await;

        let status: Status = FetchError::Node(error).into();

        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(status.message(), "backend node unavailable");
        assert!(!status.message().contains(&server_uri));
    }
}
