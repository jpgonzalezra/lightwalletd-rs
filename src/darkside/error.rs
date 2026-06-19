//! The darkside state error type and its conversions into [`NodeError`] and [`Status`].

use tonic::Status;

use crate::compact::ParseError;
use crate::node::NodeError;

/// Errors from mutating or reading the darkside state.
#[derive(Debug, thiserror::Error)]
pub enum DarksideError {
    /// An operation ran before `Reset` initialized the state.
    #[error("please call Reset first")]
    NotReset,
    /// A staging/apply invariant was violated (gap, bad height, missing entry, ...).
    #[error("{0}")]
    Invalid(String),
    /// A block or transaction failed to parse.
    #[error(transparent)]
    Parse(#[from] ParseError),
    /// A hex-encoded input could not be decoded.
    #[error("decoding hex: {0}")]
    Hex(#[from] hex::FromHexError),
}

impl From<DarksideError> for NodeError {
    fn from(error: DarksideError) -> Self {
        NodeError::Rpc {
            code: -1,
            message: error.to_string(),
        }
    }
}

impl From<DarksideError> for Status {
    fn from(error: DarksideError) -> Self {
        match error {
            DarksideError::NotReset => Status::failed_precondition(error.to_string()),
            other => Status::invalid_argument(other.to_string()),
        }
    }
}
