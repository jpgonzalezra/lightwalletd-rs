//! Signal from the read path to the ingestor: the lowest cached height suspected of belonging to an
//! abandoned fork.
//!
//! A range request can observe a chain discontinuity before the ingestor reaches it: the ingestor
//! rolls a reorg back one block per step, so for the duration of the repair the cache still holds
//! blocks of the abandoned fork while the node already serves the new chain. Serving is aborted
//! either way, but without a nudge the ingestor would only get there on its own schedule and every
//! retry in between would hit the same discontinuity. Reporting the height turns that into a single
//! transient failure: the ingestor truncates from it and re-ingestion refills from the node's chain.
//!
//! The cache keeps a single writer. The read path only ever reports a height here; the ingestor is
//! what acts on it.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Sentinel for "nothing reported": no block height ever reaches it.
const NOTHING: u64 = u64::MAX;

/// A shared slot holding the lowest height reported as inconsistent, cloneable across tasks.
#[derive(Clone, Debug)]
pub struct RepairSignal {
    lowest: Arc<AtomicU64>,
}

impl RepairSignal {
    pub fn new() -> Self {
        Self {
            lowest: Arc::new(AtomicU64::new(NOTHING)),
        }
    }

    /// Report `height` as suspect. The lowest report wins: truncating from it drops every block
    /// above, so the deepest suspicion subsumes the shallower ones.
    pub fn report(&self, height: u64) {
        self.lowest.fetch_min(height, Ordering::Relaxed);
    }

    /// Take the reported height, clearing the slot, or `None` if nothing was reported.
    pub fn take(&self) -> Option<u64> {
        match self.lowest.swap(NOTHING, Ordering::Relaxed) {
            NOTHING => None,
            height => Some(height),
        }
    }
}

impl Default for RepairSignal {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::RepairSignal;

    #[test]
    fn take_returns_nothing_when_no_height_was_reported() {
        assert_eq!(RepairSignal::new().take(), None);
    }

    #[test]
    fn take_returns_the_reported_height() {
        let signal = RepairSignal::new();
        signal.report(1_000);
        assert_eq!(signal.take(), Some(1_000));
    }

    #[test]
    fn take_clears_the_slot() {
        let signal = RepairSignal::new();
        signal.report(1_000);
        signal.take();
        assert_eq!(signal.take(), None);
    }

    #[test]
    fn the_lowest_of_several_reports_wins() {
        let signal = RepairSignal::new();
        signal.report(1_000);
        signal.report(400);
        signal.report(900);
        assert_eq!(signal.take(), Some(400));
    }

    #[test]
    fn a_clone_shares_the_slot() {
        let signal = RepairSignal::new();
        signal.clone().report(700);
        assert_eq!(signal.take(), Some(700));
    }

    #[test]
    fn reporting_height_zero_is_distinguishable_from_nothing() {
        let signal = RepairSignal::new();
        signal.report(0);
        assert_eq!(signal.take(), Some(0));
    }
}
