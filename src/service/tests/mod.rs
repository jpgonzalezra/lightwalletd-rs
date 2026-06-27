mod address;
mod blocks;
mod chain;
mod ping;
mod transactions;
mod treestate;

use std::sync::Arc;

use crate::node::NodeRpc;
use crate::testutil::temp_cache;

use super::Streamer;

/// A well-formed transparent address (the same one the integration tests use), accepted by
/// `check_taddress` so a test reaches the node path.
pub(crate) const TADDR: &str = "t1ScrubbedBeforePublicationPlan001aaaaa";

pub(crate) fn streamer_with(node: Arc<dyn NodeRpc>) -> (tempfile::TempDir, Streamer) {
    let (dir, cache) = temp_cache();
    (
        dir,
        Streamer::new(node, Arc::new(cache), "main".to_string(), None),
    )
}
