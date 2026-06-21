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
