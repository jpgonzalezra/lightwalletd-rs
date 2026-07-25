//! Snapshot bootstrap: serving and consuming a portable copy of the compact-block cache.
//!
//! A fresh instance would otherwise ingest millions of blocks from its own node before it can serve
//! anything. Instead it can download the same blocks from a peer that already holds them, verify
//! every one of them against its own node, and then let the normal ingestor carry it to the tip.
//!
//! The artifact is a serialization of the cache contents cut into fixed epochs, not a copy of the
//! `redb` file: a file copy would carry a storage-format version, free-page overhead, and no way to
//! be read consistently while the ingestor writes, and it would bypass every [`crate::cache::Cache`]
//! invariant on the way in.

pub mod export;
pub mod format;

use crate::cache::CacheError;

/// Errors from building, serving or consuming a snapshot.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    /// Reading the blocks or the stored epoch digests failed.
    #[error(transparent)]
    Cache(#[from] CacheError),
    /// Writing an epoch body failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// An epoch body did not match the format.
    #[error("malformed epoch body: {0}")]
    Malformed(String),
    /// The chain name cannot be carried in an epoch header.
    #[error("chain name {0:?} cannot be encoded in an epoch header")]
    InvalidChain(String),
    /// The requested epoch is not one this server publishes.
    #[error("epoch {index} is not available in this snapshot")]
    UnknownEpoch {
        /// The requested epoch.
        index: u64,
    },
    /// A stored digest row could not be decoded, so the epoch it describes cannot be published.
    #[error("stored digest for epoch {index} is malformed")]
    MalformedDigest {
        /// The epoch whose row is unreadable.
        index: u64,
    },
}
