//! Darkside mode: a controllable, in-memory mock chain for deterministic wallet tests.
//!
//! Instead of talking to a real node, lightwalletd-rs serves block data from a [`DarksideState`] that
//! a test fabricates over gRPC. [`DarksideNode`] implements the [`NodeRpc`](crate::node::NodeRpc) seam
//! over that state, so the cache, ingestor, and `CompactTxStreamer` service are reused unchanged;
//! [`DarksideService`] is the `DarksideStreamer` control plane that mutates the same state (stage
//! blocks/transactions, apply them, trigger reorgs, capture sent transactions).
//!
//! Submodules: `error` (the error type), `block` (the raw-block helpers and the held `ActiveBlock`),
//! `state` (the mock chain `DarksideState`), `node` (the `NodeRpc` seam), and `service` (the control
//! plane).

mod block;
mod error;
mod node;
mod service;
mod state;

#[cfg(test)]
mod tests;

pub use error::DarksideError;
pub use node::DarksideNode;
pub use service::DarksideService;
pub use state::{DarksideHandle, DarksideState};
