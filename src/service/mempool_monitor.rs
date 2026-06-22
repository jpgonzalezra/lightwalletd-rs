//! Shared mempool monitor: one background task refreshes the mempool at most once every
//! [`REFRESH_INTERVAL`] and fans the deduplicated result out to all live clients through a
//! [`watch`] channel, so node load stays independent of the number of connected wallets.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use crate::compact::{self, ParseError};
use crate::encoding;
use crate::node::{NodeError, NodeRpc};
use crate::proto::CompactTx;

/// Minimum delay between two mempool refreshes; also the wallet-visible staleness bound.
const REFRESH_INTERVAL: Duration = Duration::from_secs(2);

/// One mempool transaction, parsed once and shared by both mempool methods.
#[derive(Clone)]
pub struct MempoolEntry {
    /// Display-order hex txid, as returned by `getrawmempool`.
    pub txid_display: String,
    /// Protocol-order (wire) txid bytes, for exclude-suffix matching.
    pub wire_txid: Vec<u8>,
    /// Raw transaction bytes, for `GetMempoolStream`'s `RawTransaction`.
    pub raw: Vec<u8>,
    /// Parsed compact transaction, cloned and pool-filtered per `GetMempoolTx` request.
    pub compact: CompactTx,
}

/// The mempool as of the last refresh, valid for one block interval.
pub struct MempoolSnapshot {
    /// `bestblockhash` at refresh time; a change means a new block was mined.
    pub tip_hash: String,
    /// Tip height, reported as the `RawTransaction` height for stream entries.
    pub height: u64,
    /// The deduplicated mempool entries, in `getrawmempool` order.
    pub entries: Vec<MempoolEntry>,
}

impl MempoolSnapshot {
    /// The snapshot published before the first refresh completes: no tip, no entries.
    fn empty() -> Self {
        Self {
            tip_hash: String::new(),
            height: 0,
            entries: Vec::new(),
        }
    }
}

/// Shared read handle stored on the `Streamer`.
#[derive(Clone)]
pub struct MempoolHandle {
    sender: watch::Sender<Arc<MempoolSnapshot>>,
}

impl MempoolHandle {
    /// The current snapshot, with no RPC — for `GetMempoolTx`.
    pub fn current(&self) -> Arc<MempoolSnapshot> {
        self.sender.borrow().clone()
    }

    /// A fresh subscription whose first `borrow_and_update` sees the current value — for
    /// `GetMempoolStream`.
    pub fn subscribe(&self) -> watch::Receiver<Arc<MempoolSnapshot>> {
        self.sender.subscribe()
    }

    /// Build a handle serving a single fixed snapshot, for tests that exercise the read paths
    /// without spawning the monitor or touching the node.
    #[cfg(test)]
    pub(crate) fn fixed(snapshot: MempoolSnapshot) -> Self {
        let (sender, _receiver) = watch::channel(Arc::new(snapshot));
        Self { sender }
    }
}

/// Errors a single refresh can produce. Logged and retried on the next tick; never fatal.
#[derive(Debug, thiserror::Error)]
enum RefreshError {
    #[error(transparent)]
    Node(#[from] NodeError),
    #[error("decoding mempool hex: {0}")]
    Hex(#[from] hex::FromHexError),
    #[error("parsing mempool tx: {0}")]
    Parse(#[from] ParseError),
}

/// Accumulator carried across refreshes within one block interval.
struct MonitorState {
    tip_hash: String,
    seen: HashSet<String>,
    entries: Vec<MempoolEntry>,
}

impl MonitorState {
    fn empty() -> Self {
        Self {
            tip_hash: String::new(),
            seen: HashSet::new(),
            entries: Vec::new(),
        }
    }
}

/// Refresh the mempool once. Resets the interval state when the tip changed (a new block was mined),
/// then fetches only the txids not already seen this interval, parses them once, and returns the
/// snapshot to publish. Pure with respect to time, so it is unit-tested directly.
async fn refresh(
    node: &dyn NodeRpc,
    state: &mut MonitorState,
) -> Result<MempoolSnapshot, RefreshError> {
    let info = node.get_blockchain_info().await?;
    if info.bestblockhash != state.tip_hash {
        state.tip_hash = info.bestblockhash.clone();
        state.seen.clear();
        state.entries.clear();
    }
    for txid in node.get_raw_mempool().await? {
        if !state.seen.insert(txid.clone()) {
            continue;
        }
        let raw = node.get_raw_transaction(&txid).await?;
        // A non-zero height means the tx was mined between getrawmempool listing it and this fetch.
        if raw.height != 0 {
            continue;
        }
        let bytes = hex::decode(&raw.hex)?;
        let wire_txid = encoding::display_hex_to_wire(&txid)?;
        let compact = compact::compact_tx_from_raw(state.entries.len() as u64, &bytes)?;
        state.entries.push(MempoolEntry {
            txid_display: txid,
            wire_txid,
            raw: bytes,
            compact,
        });
    }
    Ok(MempoolSnapshot {
        tip_hash: state.tip_hash.clone(),
        height: info.blocks,
        entries: state.entries.clone(),
    })
}

/// Start the monitor: publish an empty initial snapshot, spawn the background loop (refresh first,
/// then sleep), and return the read handle. A failed refresh is logged and retried on the next tick;
/// the task never exits, so clients keep serving the last good snapshot.
pub fn start(node: Arc<dyn NodeRpc>) -> MempoolHandle {
    let (sender, _receiver) = watch::channel(Arc::new(MempoolSnapshot::empty()));
    let handle = MempoolHandle {
        sender: sender.clone(),
    };
    tokio::spawn(async move {
        tracing::info!(
            refresh_interval_secs = REFRESH_INTERVAL.as_secs(),
            "mempool monitor started"
        );
        let mut state = MonitorState::empty();
        loop {
            match refresh(node.as_ref(), &mut state).await {
                // send_replace, not send: publish even with zero subscribers, since the
                // GetMempoolTx read path borrows the snapshot without ever subscribing.
                Ok(snapshot) => {
                    // One refresh per interval regardless of client count: `subscribers` is how many
                    // streams share this single fetch.
                    tracing::debug!(
                        entries = snapshot.entries.len(),
                        subscribers = sender.receiver_count(),
                        "mempool refreshed"
                    );
                    sender.send_replace(Arc::new(snapshot));
                }
                Err(error) => {
                    tracing::warn!(%error, "mempool monitor refresh failed; retrying next tick")
                }
            }
            tokio::time::sleep(REFRESH_INTERVAL).await;
        }
    });
    handle
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;

    use crate::node::{
        AddressUtxo, GetAddressBalance, GetBlockVerbose, GetInfo, GetRawTransaction, GetSubtrees,
        GetTreeState,
    };

    /// A `NodeRpc` fake that scripts the tip across successive `get_blockchain_info` calls and
    /// records every `get_raw_transaction` so tests can assert dedup. Only the three mempool RPCs
    /// are wired; any other call is unreachable in these tests.
    struct ScriptedNode {
        /// `bestblockhash` for successive `get_blockchain_info` calls; the last entry repeats.
        tips: Vec<String>,
        blockchain_calls: AtomicUsize,
        /// txids returned by every `get_raw_mempool` call.
        mempool: Vec<String>,
        /// raw-tx `(hex, height)` keyed by display-order txid.
        txs: HashMap<String, (String, i64)>,
        /// txids passed to `get_raw_transaction`, in call order.
        tx_requests: Mutex<Vec<String>>,
    }

    impl ScriptedNode {
        fn new(tips: Vec<&str>, mempool: Vec<String>, txs: Vec<(String, String, i64)>) -> Self {
            Self {
                tips: tips.into_iter().map(String::from).collect(),
                blockchain_calls: AtomicUsize::new(0),
                mempool,
                txs: txs.into_iter().map(|(id, hex, h)| (id, (hex, h))).collect(),
                tx_requests: Mutex::new(Vec::new()),
            }
        }

        fn tx_call_count(&self) -> usize {
            self.tx_requests.lock().unwrap().len()
        }
    }

    #[async_trait]
    impl NodeRpc for ScriptedNode {
        async fn get_blockchain_info(&self) -> Result<crate::node::GetBlockchainInfo, NodeError> {
            let index = self.blockchain_calls.fetch_add(1, Ordering::SeqCst);
            let tip = self
                .tips
                .get(index)
                .or_else(|| self.tips.last())
                .expect("ScriptedNode: tips is empty");
            Ok(serde_json::from_value(serde_json::json!({
                "chain": "main",
                "blocks": 100,
                "bestblockhash": tip,
                "consensus": { "chaintip": "00000000" },
            }))
            .unwrap())
        }

        async fn get_raw_mempool(&self) -> Result<Vec<String>, NodeError> {
            Ok(self.mempool.clone())
        }

        async fn get_raw_transaction(&self, txid: &str) -> Result<GetRawTransaction, NodeError> {
            self.tx_requests.lock().unwrap().push(txid.to_string());
            let (raw_hex, height) = self
                .txs
                .get(txid)
                .cloned()
                .expect("ScriptedNode: unexpected get_raw_transaction txid");
            Ok(
                serde_json::from_value(serde_json::json!({ "hex": raw_hex, "height": height }))
                    .unwrap(),
            )
        }

        async fn get_info(&self) -> Result<GetInfo, NodeError> {
            unimplemented!()
        }
        async fn get_block_verbose(&self, _height: u64) -> Result<GetBlockVerbose, NodeError> {
            unimplemented!()
        }
        async fn get_block_count(&self) -> Result<u64, NodeError> {
            unimplemented!()
        }
        async fn get_block_raw(&self, _hash: &str) -> Result<Vec<u8>, NodeError> {
            unimplemented!()
        }
        async fn send_raw_transaction(&self, _hex: &str) -> Result<String, NodeError> {
            unimplemented!()
        }
        async fn get_treestate(&self, _id: &str) -> Result<GetTreeState, NodeError> {
            unimplemented!()
        }
        async fn get_address_balance(
            &self,
            _addresses: &[String],
        ) -> Result<GetAddressBalance, NodeError> {
            unimplemented!()
        }
        async fn get_address_utxos(
            &self,
            _addresses: &[String],
        ) -> Result<Vec<AddressUtxo>, NodeError> {
            unimplemented!()
        }
        async fn get_address_txids(
            &self,
            _addresses: &[String],
            _start: u64,
            _end: u64,
        ) -> Result<Vec<String>, NodeError> {
            unimplemented!()
        }
        async fn get_subtrees(
            &self,
            _protocol: &str,
            _start_index: u32,
            _max_entries: u32,
        ) -> Result<GetSubtrees, NodeError> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn dedup_fetches_each_tx_once_across_refreshes() {
        let (raw, _, _) = crate::testutil::shielded_v5_tx();
        let txid = compact::txid_display(&raw).unwrap();
        let node = ScriptedNode::new(
            vec!["aa"],
            vec![txid.clone()],
            vec![(txid.clone(), hex::encode(&raw), 0)],
        );
        let mut state = MonitorState::empty();

        let first = refresh(&node, &mut state).await.unwrap();
        let second = refresh(&node, &mut state).await.unwrap();

        assert_eq!(node.tx_call_count(), 1);
        assert_eq!(first.entries.len(), 1);
        assert_eq!(second.entries.len(), 1);
        assert_eq!(second.entries[0].txid_display, txid);
    }

    #[tokio::test]
    async fn tip_change_resets_interval_state_and_refetches() {
        let (raw, _, _) = crate::testutil::shielded_v5_tx();
        let txid = compact::txid_display(&raw).unwrap();
        let node = ScriptedNode::new(
            vec!["aa", "bb"],
            vec![txid.clone()],
            vec![(txid.clone(), hex::encode(&raw), 0)],
        );
        let mut state = MonitorState::empty();

        let first = refresh(&node, &mut state).await.unwrap();
        let second = refresh(&node, &mut state).await.unwrap();

        assert_eq!(first.tip_hash, "aa");
        assert_eq!(second.tip_hash, "bb");
        // The tip changed, so the seen-set reset and the tx was fetched again under the new tip.
        assert_eq!(node.tx_call_count(), 2);
        assert_eq!(second.entries.len(), 1);
    }

    #[tokio::test]
    async fn already_mined_tx_is_skipped() {
        let (raw, _, _) = crate::testutil::shielded_v5_tx();
        let mempool_txid = compact::txid_display(&raw).unwrap();
        let mined_txid = "deadbeef".to_string();
        let node = ScriptedNode::new(
            vec!["aa"],
            vec![mined_txid.clone(), mempool_txid.clone()],
            // The mined tx has height != 0, so it is skipped before its hex is ever decoded.
            vec![
                (mined_txid, "00".to_string(), 5),
                (mempool_txid.clone(), hex::encode(&raw), 0),
            ],
        );
        let mut state = MonitorState::empty();

        let snapshot = refresh(&node, &mut state).await.unwrap();

        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].txid_display, mempool_txid);
    }
}
