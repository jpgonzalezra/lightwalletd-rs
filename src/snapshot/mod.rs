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
pub mod import;
pub mod serve;

use crate::cache::CacheError;
use crate::node::NodeError;

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
    /// The snapshot describes a different chain than the node this instance is connected to.
    #[error("snapshot is for chain {found:?}, but this node is on {expected:?}")]
    ChainMismatch {
        /// The chain the local node reports.
        expected: String,
        /// The chain the manifest claims.
        found: String,
    },
    /// The snapshot was produced by an incompatible format version.
    #[error("snapshot format version {found} is not supported (this build speaks {expected})")]
    UnsupportedVersion {
        /// The version this build understands.
        expected: u8,
        /// The version the manifest claims.
        found: u8,
    },
    /// The snapshot cuts epochs at different boundaries, so its bodies are not ours to verify.
    #[error("snapshot epoch size {found} does not match {expected}")]
    EpochSizeMismatch {
        /// The epoch size this build uses.
        expected: u64,
        /// The epoch size the manifest claims.
        found: u64,
    },
    /// The manifest describes itself inconsistently, before any epoch is even fetched.
    #[error("malformed manifest: {0}")]
    MalformedManifest(String),
    /// Importing would leave a hole between what the cache holds and what the snapshot offers.
    #[error(
        "snapshot starts at height {snapshot_base}, which would leave a gap above the cached tip \
         {cache_tip:?}"
    )]
    Gap {
        /// The highest height the cache holds, if any.
        cache_tip: Option<u64>,
        /// The lowest height the snapshot can supply.
        snapshot_base: u64,
    },
    /// An epoch body failed a whole-body check.
    #[error("epoch {epoch}: {check} check failed: {detail}")]
    EpochRejected {
        /// The epoch that failed.
        epoch: u64,
        /// Which of the verification layers rejected it.
        check: &'static str,
        /// What exactly disagreed.
        detail: String,
    },
    /// An epoch body failed a check that pinpoints a height.
    #[error("epoch {epoch}: {check} check failed at height {height}: {detail}")]
    BlockRejected {
        /// The epoch that failed.
        epoch: u64,
        /// The height the failure was localized to.
        height: u64,
        /// Which of the verification layers rejected it.
        check: &'static str,
        /// What exactly disagreed.
        detail: String,
    },
    /// The node could not answer at all.
    #[error(transparent)]
    Node(#[from] NodeError),
    /// A block-hash lookup task died, which is a bug rather than a verification failure.
    #[error("block hash lookup task failed: {0}")]
    LookupTask(#[from] tokio::task::JoinError),
    /// The node could not answer while a snapshot was being verified against it.
    #[error("looking up the block hash at height {height}: {source}")]
    NodeLookup {
        /// The height being verified.
        height: u64,
        /// The underlying node error.
        #[source]
        source: NodeError,
    },
}
