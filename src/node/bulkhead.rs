//! A concurrency bulkhead in front of the backend node, for the wallet-facing path (ADR 0036).
//!
//! One process holds one node. The ingestor bounds its own share of it at `--ingest-concurrency`
//! and the mempool monitor is a single poller, so without a bound here the wallet-facing handlers
//! are the only consumer that can take an unlimited share. What yields when the node saturates is
//! then the component keeping the cache at the chain tip.
//!
//! [`Bulkhead`] wraps the node handle the service gets and admits calls into one of two disjoint
//! pools, chosen by whether the request bounds its own cost at the node. A call that cannot be
//! admitted within [`PERMIT_WAIT`] is refused with [`NodeError::Overloaded`], which the service maps
//! to `RESOURCE_EXHAUSTED`: shedding names the client that caused it, where queueing would spread
//! the delay over everyone.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::{
    AddressUtxo, GetAddressBalance, GetBlockVerbose, GetBlockchainInfo, GetInfo, GetRawTransaction,
    GetSubtrees, GetTreeState, NodeError, NodeRpc,
};
use crate::config::ClientNodeLimits;

/// How long a call waits for a permit before it is refused.
///
/// Long enough that an ordinary burst rides out the queue: when the work is honest the permits turn
/// over in milliseconds, so this is many turns of either pool. Short enough that a client learns it
/// was shed instead of sitting behind an address scan. A scan runs orders of magnitude longer than
/// this, and scans are what hold permits when the server is actually under load.
const PERMIT_WAIT: Duration = Duration::from_millis(250);

/// The pool a call is admitted into. Label only: the pools themselves live in [`Bulkhead`].
const SCAN_CLASS: &str = "transparent-address";
const GENERAL_CLASS: &str = "node";

/// Bounds the node calls made on behalf of wallet clients, leaving the ingestor's share untouched.
///
/// Wrap the handle the service is built with, never the one the ingestor or the mempool monitor
/// gets: the point is to keep those two out of the pools a client can fill.
pub struct Bulkhead {
    inner: Arc<dyn NodeRpc>,
    /// Transparent-address queries, whose node-side cost the caller does not bound.
    scans: Arc<Semaphore>,
    /// Everything else, whose cost is bounded by what the request names.
    general: Arc<Semaphore>,
}

impl Bulkhead {
    /// Wrap `inner` with the two pools `limits` sizes.
    pub fn new(inner: Arc<dyn NodeRpc>, limits: ClientNodeLimits) -> Self {
        Self {
            inner,
            scans: Arc::new(Semaphore::new(limits.scan_concurrency.max(1))),
            general: Arc::new(Semaphore::new(limits.concurrency.max(1))),
        }
    }

    /// Run `call` under a permit from the transparent-address pool.
    async fn scan<T, F>(&self, call: F) -> Result<T, NodeError>
    where
        T: Send + 'static,
        F: Future<Output = Result<T, NodeError>> + Send + 'static,
    {
        let permit = admit(&self.scans, SCAN_CLASS).await?;
        hold(permit, call).await
    }

    /// Run `call` under a permit from the general pool.
    async fn general<T, F>(&self, call: F) -> Result<T, NodeError>
    where
        T: Send + 'static,
        F: Future<Output = Result<T, NodeError>> + Send + 'static,
    {
        let permit = admit(&self.general, GENERAL_CLASS).await?;
        hold(permit, call).await
    }
}

/// Take a permit from `pool`, or refuse the call once [`PERMIT_WAIT`] is up.
async fn admit(
    pool: &Arc<Semaphore>,
    class: &'static str,
) -> Result<OwnedSemaphorePermit, NodeError> {
    match tokio::time::timeout(PERMIT_WAIT, Arc::clone(pool).acquire_owned()).await {
        Ok(Ok(permit)) => Ok(permit),
        // Nothing closes these semaphores. Treating a closed one as a full pool gives the same
        // answer and keeps the failure inside the call that asked.
        Ok(Err(_)) | Err(_) => Err(NodeError::Overloaded { class }),
    }
}

/// Await `call` with `permit` held by a task the caller cannot cancel.
///
/// Cancellation is the whole reason this is not a plain `let _permit = permit; call.await`. Zebra
/// dispatches a read to its blocking pool when the request arrives, so a client resetting its
/// stream, or a proxy-side deadline firing, stops nothing at the node. Handing the permit back the
/// moment the caller loses interest would let one client admit call after call while its earlier
/// ones are still running, and the pool would bound admissions rather than node work. The task
/// outlives the caller and holds the permit until the node has actually answered.
async fn hold<T, F>(permit: OwnedSemaphorePermit, call: F) -> Result<T, NodeError>
where
    T: Send + 'static,
    F: Future<Output = Result<T, NodeError>> + Send + 'static,
{
    tokio::spawn(async move {
        let _permit = permit;
        call.await
    })
    .await?
}

#[async_trait::async_trait]
impl NodeRpc for Bulkhead {
    async fn get_info(&self) -> Result<GetInfo, NodeError> {
        let inner = Arc::clone(&self.inner);
        self.general(async move { inner.get_info().await }).await
    }

    async fn get_blockchain_info(&self) -> Result<GetBlockchainInfo, NodeError> {
        let inner = Arc::clone(&self.inner);
        self.general(async move { inner.get_blockchain_info().await })
            .await
    }

    async fn get_block_verbose(&self, height: u64) -> Result<GetBlockVerbose, NodeError> {
        let inner = Arc::clone(&self.inner);
        self.general(async move { inner.get_block_verbose(height).await })
            .await
    }

    async fn get_block_count(&self) -> Result<u64, NodeError> {
        let inner = Arc::clone(&self.inner);
        self.general(async move { inner.get_block_count().await })
            .await
    }

    async fn get_block_hash(&self, height: u64) -> Result<String, NodeError> {
        let inner = Arc::clone(&self.inner);
        self.general(async move { inner.get_block_hash(height).await })
            .await
    }

    /// Overridden rather than left to the trait's default loop, which would take one permit per
    /// height and lose the single batched request that [`super::NodeClient`] answers this with.
    async fn get_block_hashes(&self, heights: &[u64]) -> Result<Vec<String>, NodeError> {
        let inner = Arc::clone(&self.inner);
        let heights = heights.to_vec();
        self.general(async move { inner.get_block_hashes(&heights).await })
            .await
    }

    async fn get_block_raw(&self, hash: &str) -> Result<Vec<u8>, NodeError> {
        let inner = Arc::clone(&self.inner);
        let hash = hash.to_owned();
        self.general(async move { inner.get_block_raw(&hash).await })
            .await
    }

    async fn get_raw_transaction(&self, txid: &str) -> Result<GetRawTransaction, NodeError> {
        let inner = Arc::clone(&self.inner);
        let txid = txid.to_owned();
        self.general(async move { inner.get_raw_transaction(&txid).await })
            .await
    }

    async fn send_raw_transaction(&self, hex: &str) -> Result<String, NodeError> {
        let inner = Arc::clone(&self.inner);
        let hex = hex.to_owned();
        self.general(async move { inner.send_raw_transaction(&hex).await })
            .await
    }

    async fn get_treestate(&self, id: &str) -> Result<GetTreeState, NodeError> {
        let inner = Arc::clone(&self.inner);
        let id = id.to_owned();
        self.general(async move { inner.get_treestate(&id).await })
            .await
    }

    async fn get_address_balance(
        &self,
        addresses: &[String],
    ) -> Result<GetAddressBalance, NodeError> {
        let inner = Arc::clone(&self.inner);
        let addresses = addresses.to_vec();
        self.scan(async move { inner.get_address_balance(&addresses).await })
            .await
    }

    async fn get_address_utxos(&self, addresses: &[String]) -> Result<Vec<AddressUtxo>, NodeError> {
        let inner = Arc::clone(&self.inner);
        let addresses = addresses.to_vec();
        self.scan(async move { inner.get_address_utxos(&addresses).await })
            .await
    }

    async fn get_address_txids(
        &self,
        addresses: &[String],
        start: u64,
        end: u64,
    ) -> Result<Vec<String>, NodeError> {
        let inner = Arc::clone(&self.inner);
        let addresses = addresses.to_vec();
        self.scan(async move { inner.get_address_txids(&addresses, start, end).await })
            .await
    }

    async fn get_subtrees(
        &self,
        protocol: &str,
        start_index: u32,
        max_entries: u32,
    ) -> Result<GetSubtrees, NodeError> {
        let inner = Arc::clone(&self.inner);
        let protocol = protocol.to_owned();
        self.general(async move {
            inner
                .get_subtrees(&protocol, start_index, max_entries)
                .await
        })
        .await
    }

    async fn get_raw_mempool(&self) -> Result<Vec<String>, NodeError> {
        let inner = Arc::clone(&self.inner);
        self.general(async move { inner.get_raw_mempool().await })
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// A node whose calls park until the test lets them through, so a test can hold permits for as
    /// long as it needs without sleeping.
    struct GatedNode {
        /// Permits added here release that many parked calls.
        gate: Arc<Semaphore>,
        /// Calls that reached the gate, released or not.
        arrived: AtomicUsize,
    }

    impl GatedNode {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                gate: Arc::new(Semaphore::new(0)),
                arrived: AtomicUsize::new(0),
            })
        }

        async fn park(&self) {
            self.arrived.fetch_add(1, Ordering::SeqCst);
            match Arc::clone(&self.gate).acquire_owned().await {
                Ok(permit) => drop(permit),
                Err(closed) => panic!("gate closed: {closed}"),
            }
        }
    }

    #[async_trait::async_trait]
    impl NodeRpc for GatedNode {
        async fn get_address_utxos(
            &self,
            _addresses: &[String],
        ) -> Result<Vec<AddressUtxo>, NodeError> {
            self.park().await;
            Ok(Vec::new())
        }

        async fn get_raw_mempool(&self) -> Result<Vec<String>, NodeError> {
            self.park().await;
            Ok(Vec::new())
        }

        async fn get_block_count(&self) -> Result<u64, NodeError> {
            Ok(7)
        }

        async fn get_info(&self) -> Result<GetInfo, NodeError> {
            unimplemented!("not called by these tests")
        }
        async fn get_blockchain_info(&self) -> Result<GetBlockchainInfo, NodeError> {
            unimplemented!("not called by these tests")
        }
        async fn get_block_verbose(&self, _height: u64) -> Result<GetBlockVerbose, NodeError> {
            unimplemented!("not called by these tests")
        }
        async fn get_block_hash(&self, _height: u64) -> Result<String, NodeError> {
            unimplemented!("not called by these tests")
        }
        async fn get_block_raw(&self, _hash: &str) -> Result<Vec<u8>, NodeError> {
            unimplemented!("not called by these tests")
        }
        async fn get_raw_transaction(&self, _txid: &str) -> Result<GetRawTransaction, NodeError> {
            unimplemented!("not called by these tests")
        }
        async fn send_raw_transaction(&self, _hex: &str) -> Result<String, NodeError> {
            unimplemented!("not called by these tests")
        }
        async fn get_treestate(&self, _id: &str) -> Result<GetTreeState, NodeError> {
            unimplemented!("not called by these tests")
        }
        async fn get_address_balance(
            &self,
            _addresses: &[String],
        ) -> Result<GetAddressBalance, NodeError> {
            unimplemented!("not called by these tests")
        }
        async fn get_address_txids(
            &self,
            _addresses: &[String],
            _start: u64,
            _end: u64,
        ) -> Result<Vec<String>, NodeError> {
            unimplemented!("not called by these tests")
        }
        async fn get_subtrees(
            &self,
            _protocol: &str,
            _start_index: u32,
            _max_entries: u32,
        ) -> Result<GetSubtrees, NodeError> {
            unimplemented!("not called by these tests")
        }
    }

    fn bulkhead(node: Arc<GatedNode>, scans: usize, general: usize) -> Arc<Bulkhead> {
        Arc::new(Bulkhead::new(
            node,
            ClientNodeLimits {
                concurrency: general,
                scan_concurrency: scans,
            },
        ))
    }

    /// Spawn a scan and wait until it is parked inside the node, so the caller knows its permit is
    /// taken rather than still queued.
    async fn scan_in_flight(
        bulkhead: &Arc<Bulkhead>,
        node: &Arc<GatedNode>,
        expected: usize,
    ) -> tokio::task::JoinHandle<Result<Vec<AddressUtxo>, NodeError>> {
        let handle = {
            let bulkhead = Arc::clone(bulkhead);
            tokio::spawn(async move { bulkhead.get_address_utxos(&[]).await })
        };
        while node.arrived.load(Ordering::SeqCst) < expected {
            tokio::task::yield_now().await;
        }
        handle
    }

    /// The pool a refused call was turned away from.
    ///
    /// Bounded, because the failure these tests exist to catch is a call admitted when it should
    /// not be. That call parks in the gated node, which never answers on its own, so without the
    /// timeout the test would hang instead of saying what went wrong.
    async fn refused_by<T>(call: impl Future<Output = Result<T, NodeError>>) -> &'static str {
        let result = tokio::time::timeout(Duration::from_secs(5), call)
            .await
            .expect("the call was admitted into a pool that should have been full");
        match result {
            Err(NodeError::Overloaded { class }) => class,
            _ => panic!("expected the call to be refused"),
        }
    }

    #[tokio::test]
    async fn a_scan_past_the_pool_is_refused_rather_than_queued() {
        let node = GatedNode::new();
        let bulkhead = bulkhead(Arc::clone(&node), 1, 4);
        let held = scan_in_flight(&bulkhead, &node, 1).await;

        assert_eq!(
            refused_by(bulkhead.get_address_utxos(&[])).await,
            SCAN_CLASS
        );

        node.gate.add_permits(1);
        held.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn a_full_scan_pool_leaves_the_general_pool_alone() {
        let node = GatedNode::new();
        let bulkhead = bulkhead(Arc::clone(&node), 1, 4);
        let held = scan_in_flight(&bulkhead, &node, 1).await;

        assert_eq!(bulkhead.get_block_count().await.unwrap(), 7);

        node.gate.add_permits(1);
        held.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn the_two_pools_are_sized_separately() {
        let node = GatedNode::new();
        let bulkhead = bulkhead(Arc::clone(&node), 4, 1);
        let held = {
            let bulkhead = Arc::clone(&bulkhead);
            tokio::spawn(async move { bulkhead.get_raw_mempool().await })
        };
        while node.arrived.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }

        assert_eq!(refused_by(bulkhead.get_raw_mempool()).await, GENERAL_CLASS);

        node.gate.add_permits(1);
        held.await.unwrap().unwrap();
    }

    /// The one that matters: a client that walks away does not hand its permit back while the node
    /// is still working, because at the node the work is committed and cancelling stops nothing.
    #[tokio::test]
    async fn abandoning_a_call_does_not_release_its_permit_early() {
        let node = GatedNode::new();
        let bulkhead = bulkhead(Arc::clone(&node), 1, 4);
        let abandoned = scan_in_flight(&bulkhead, &node, 1).await;

        abandoned.abort();

        assert_eq!(
            refused_by(bulkhead.get_address_utxos(&[])).await,
            SCAN_CLASS
        );

        // Once the node answers, the permit comes back and the next caller gets in.
        node.gate.add_permits(2);
        assert!(bulkhead.get_address_utxos(&[]).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_permit_comes_back_when_the_call_returns() {
        let node = GatedNode::new();
        let bulkhead = bulkhead(Arc::clone(&node), 1, 4);
        node.gate.add_permits(2);

        bulkhead.get_address_utxos(&[]).await.unwrap();

        assert!(bulkhead.get_address_utxos(&[]).await.unwrap().is_empty());
    }
}
