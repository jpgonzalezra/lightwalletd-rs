mod address;
mod blocks;
mod chain;
mod ping;
mod subtrees;
mod transactions;
mod treestate;

use std::sync::Arc;

use crate::node::NodeRpc;
use crate::testutil::temp_cache;

use super::Streamer;

/// A well-formed, synthetic transparent address accepted by `check_taddress` so a test reaches the
/// node path. Derived through `zcash_address`; see [`crate::testutil::example_taddress`].
pub(crate) fn taddr() -> String {
    crate::testutil::example_taddress()
}

pub(crate) fn streamer_with(node: Arc<dyn NodeRpc>) -> (tempfile::TempDir, Streamer) {
    let (dir, cache) = temp_cache();
    (
        dir,
        Streamer::new(node, Arc::new(cache), "main".to_string(), None),
    )
}
