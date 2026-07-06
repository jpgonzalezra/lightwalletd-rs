//! Mempool streams: `GetMempoolTx` (compact transactions) and `GetMempoolStream` (raw transactions).
//!
//! In live mode both read from the shared [`mempool_monitor`](super::mempool_monitor) snapshot, so
//! node load is independent of the number of connected clients. In darkside mode the monitor is
//! absent and both fall back to the per-request path, which stays deterministic.

use std::collections::HashSet;
use std::time::Duration;

use async_stream::try_stream;
use tonic::{Request, Response, Status};

use crate::compact;
use crate::encoding;
use crate::filter;
use crate::proto::{BoxStream, CompactTx, GetMempoolTxRequest, RawTransaction};

use super::{Streamer, decode_hex};

/// Max exclude-txid suffixes a single `GetMempoolTx` request may submit, bounding the
/// O(suffixes × mempool entries) exclusion scan per request.
const MAX_EXCLUDE_TXID_SUFFIXES: usize = 10_000;

pub(super) async fn get_mempool_tx(
    streamer: &Streamer,
    request: Request<GetMempoolTxRequest>,
) -> Result<Response<BoxStream<CompactTx>>, Status> {
    let mempool_request = request.into_inner();
    let exclude = mempool_request.exclude_txid_suffixes;
    let pool_types = mempool_request.pool_types;

    if exclude.len() > MAX_EXCLUDE_TXID_SUFFIXES {
        return Err(Status::resource_exhausted(format!(
            "get_mempool_tx: more than {MAX_EXCLUDE_TXID_SUFFIXES} exclude txid suffixes; narrow the request"
        )));
    }
    for (index, suffix) in exclude.iter().enumerate() {
        if suffix.len() > 32 {
            return Err(Status::invalid_argument(format!(
                "exclude txid {index} is larger than 32 bytes"
            )));
        }
    }
    filter::validate_pool_types(&pool_types)?;

    let Some(handle) = &streamer.mempool else {
        return get_mempool_tx_from_node(streamer, exclude, pool_types).await;
    };

    let snapshot = handle.current();
    let stream = try_stream! {
        let pools = filter::Pools::from_pool_types(&pool_types);
        let wire_txids: Vec<&[u8]> =
            snapshot.entries.iter().map(|entry| entry.wire_txid.as_slice()).collect();
        let excluded = excluded_by_suffixes(&wire_txids, &exclude);
        for (entry, &is_excluded) in snapshot.entries.iter().zip(&excluded) {
            if is_excluded {
                continue;
            }
            let mut compact = entry.compact.clone();
            filter::filter_tx_to_pools(&mut compact, pools);
            yield compact;
        }
    };
    Ok(Response::new(Box::pin(stream)))
}

/// For each mempool tx (by protocol-order txid), whether an exclude suffix removes it. Per the
/// proto contract (`proto/service.proto`), a suffix matching two or more txs is ambiguous and
/// excludes none of them; only a suffix matching exactly one tx excludes that tx, and a suffix
/// matching nothing is ignored.
fn excluded_by_suffixes(wire_txids: &[&[u8]], exclude: &[Vec<u8>]) -> Vec<bool> {
    let match_counts: Vec<usize> = exclude
        .iter()
        .map(|suffix| {
            wire_txids
                .iter()
                .filter(|txid| txid.ends_with(suffix.as_slice()))
                .count()
        })
        .collect();
    wire_txids
        .iter()
        .map(|txid| {
            exclude
                .iter()
                .zip(&match_counts)
                .any(|(suffix, &count)| count == 1 && txid.ends_with(suffix.as_slice()))
        })
        .collect()
}

pub(super) async fn get_mempool_stream(
    streamer: &Streamer,
) -> Result<Response<BoxStream<RawTransaction>>, Status> {
    let Some(handle) = &streamer.mempool else {
        return get_mempool_stream_from_node(streamer).await;
    };

    let mut receiver = handle.subscribe();
    let stream = try_stream! {
        let mut snapshot = receiver.borrow_and_update().clone();
        // Baseline tip the stream resyncs against; established from the first non-empty snapshot,
        // since the monitor publishes an empty one (tip "") before its first refresh completes.
        let mut start_tip = String::new();
        let mut sent = 0usize;
        loop {
            if start_tip.is_empty() {
                start_tip = snapshot.tip_hash.clone();
            } else if snapshot.tip_hash != start_tip {
                // A new block was mined; end the stream so the client resyncs blocks.
                break;
            }
            // Within one block interval `entries` is append-only, so a running index emits each tx
            // once; `get` guards the degenerate case where the list shrank under us.
            for entry in snapshot.entries.get(sent..).unwrap_or(&[]) {
                // A mempool tx is reported with height 0 per the RawTransaction contract
                // (proto/service.proto); a non-zero height would mark it as mined.
                yield RawTransaction {
                    data: entry.raw.clone(),
                    height: 0,
                };
            }
            sent = snapshot.entries.len();
            if receiver.changed().await.is_err() {
                break; // monitor gone (shouldn't happen) ⇒ end the stream
            }
            snapshot = receiver.borrow_and_update().clone();
        }
    };
    Ok(Response::new(Box::pin(stream)))
}

/// Per-request `GetMempoolTx`: poll the node for the mempool and parse each tx on the spot. The
/// darkside fallback, used when no shared monitor is attached.
async fn get_mempool_tx_from_node(
    streamer: &Streamer,
    exclude: Vec<Vec<u8>>,
    pool_types: Vec<i32>,
) -> Result<Response<BoxStream<CompactTx>>, Status> {
    let txids = streamer.node.get_raw_mempool().await?;
    let node = streamer.node.clone();

    let stream = try_stream! {
        let pools = filter::Pools::from_pool_types(&pool_types);
        // The txids are display-order hex; exclusion suffixes are compared in protocol order, so
        // decode every txid up front to detect an ambiguous suffix before excluding anything.
        let mut wire_txids = Vec::with_capacity(txids.len());
        for txid in &txids {
            wire_txids.push(
                encoding::display_hex_to_wire(txid)
                    .map_err(|e| Status::internal(format!("decoding mempool txid: {e}")))?,
            );
        }
        let wire_refs: Vec<&[u8]> = wire_txids.iter().map(Vec::as_slice).collect();
        let excluded = excluded_by_suffixes(&wire_refs, &exclude);
        for (index, txid) in txids.iter().enumerate() {
            if excluded[index] {
                continue;
            }
            let raw = node.get_raw_transaction(txid).await?;
            let bytes = decode_hex(&raw.hex, "mempool tx")?;
            let mut compact = compact::compact_tx_from_raw(index as u64, &bytes)
                .map_err(|e| Status::internal(format!("parsing mempool tx: {e}")))?;
            filter::filter_tx_to_pools(&mut compact, pools);
            yield compact;
        }
    };
    Ok(Response::new(Box::pin(stream)))
}

/// Per-request `GetMempoolStream`: each client runs its own 2 s poll loop against the node. The
/// darkside fallback, used when no shared monitor is attached.
async fn get_mempool_stream_from_node(
    streamer: &Streamer,
) -> Result<Response<BoxStream<RawTransaction>>, Status> {
    let node = streamer.node.clone();
    let stream = try_stream! {
        // Snapshot the tip; when it changes a new block was mined and we end the stream.
        let start = node.get_blockchain_info().await?;
        let mut seen = HashSet::new();
        loop {
            if node.get_blockchain_info().await?.bestblockhash != start.bestblockhash {
                break;
            }
            for txid in node.get_raw_mempool().await? {
                if !seen.insert(txid.clone()) {
                    continue;
                }
                let raw = node.get_raw_transaction(&txid).await?;
                // A non-zero height means the tx is already mined, not in the mempool.
                if raw.height != 0 {
                    continue;
                }
                let data = decode_hex(&raw.hex, "mempool tx")?;
                yield RawTransaction { data, height: 0 };
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    };
    Ok(Response::new(Box::pin(stream)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio_stream::StreamExt;
    use tonic::{Code, Request};

    use crate::compact;
    use crate::encoding;
    use crate::proto::compact_tx_streamer_server::CompactTxStreamer;
    use crate::proto::{CompactTx, GetMempoolTxRequest, PoolType};
    use crate::testutil::{FakeNode, shielded_v5_tx, temp_cache};

    use super::super::Streamer;
    use super::super::mempool_monitor::{MempoolEntry, MempoolHandle, MempoolSnapshot};
    use super::MAX_EXCLUDE_TXID_SUFFIXES;

    /// A `Streamer` whose mempool is served from `snapshot`, over a `FakeNode` that panics on any
    /// RPC — so a passing test proves the snapshot path issues zero node calls.
    fn streamer_with_snapshot(snapshot: MempoolSnapshot) -> (tempfile::TempDir, Streamer) {
        let (dir, cache) = temp_cache();
        let node = Arc::new(FakeNode::default());
        let streamer = Streamer::new(node, Arc::new(cache), "main".to_string(), None)
            .with_mempool_monitor(MempoolHandle::fixed(snapshot));
        (dir, streamer)
    }

    fn entry_from(raw: &[u8]) -> MempoolEntry {
        let compact = compact::compact_tx_from_raw(0, raw).unwrap();
        let txid_display = compact::txid_display(raw).unwrap();
        let wire_txid = encoding::display_hex_to_wire(&txid_display).unwrap();
        MempoolEntry {
            txid_display,
            wire_txid,
            raw: raw.to_vec(),
            compact,
        }
    }

    fn snapshot_of(entries: Vec<MempoolEntry>) -> MempoolSnapshot {
        MempoolSnapshot {
            tip_hash: "aa".to_string(),
            entries,
        }
    }

    async fn mempool_txs(streamer: &Streamer, request: GetMempoolTxRequest) -> Vec<CompactTx> {
        streamer
            .get_mempool_tx(Request::new(request))
            .await
            .unwrap()
            .into_inner()
            .map(|tx| tx.unwrap())
            .collect()
            .await
    }

    #[tokio::test]
    async fn get_mempool_stream_reports_height_zero_for_mempool_txs() {
        let (raw, _, _) = shielded_v5_tx();
        let entry = entry_from(&raw);
        let expected_data = entry.raw.clone();
        let (_dir, streamer) = streamer_with_snapshot(snapshot_of(vec![entry]));

        let mut stream = streamer
            .get_mempool_stream(Request::new(crate::proto::Empty {}))
            .await
            .unwrap()
            .into_inner();

        // An in-mempool tx must carry height 0, not the tip height (proto/service.proto).
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.height, 0);
        assert_eq!(first.data, expected_data);
    }

    #[tokio::test]
    async fn get_mempool_tx_serves_snapshot_without_node_rpc() {
        let (raw, _, _) = shielded_v5_tx();
        let entry = entry_from(&raw);
        let expected_txid = entry.compact.txid.clone();
        let (_dir, streamer) = streamer_with_snapshot(snapshot_of(vec![entry]));

        let txids: Vec<_> = mempool_txs(&streamer, GetMempoolTxRequest::default())
            .await
            .into_iter()
            .map(|tx| tx.txid)
            .collect();

        assert_eq!(txids, vec![expected_txid]);
    }

    #[tokio::test]
    async fn get_mempool_tx_drops_excluded_suffix() {
        let (raw, _, _) = shielded_v5_tx();
        let entry = entry_from(&raw);
        // The full wire txid is a (degenerate) matching suffix.
        let suffix = entry.wire_txid.clone();
        let (_dir, streamer) = streamer_with_snapshot(snapshot_of(vec![entry]));

        let txs = mempool_txs(
            &streamer,
            GetMempoolTxRequest {
                exclude_txid_suffixes: vec![suffix],
                ..Default::default()
            },
        )
        .await;

        assert!(txs.is_empty());
    }

    #[tokio::test]
    async fn get_mempool_tx_rejects_oversized_exclude_suffix() {
        let (_dir, streamer) = streamer_with_snapshot(snapshot_of(vec![]));

        let status = streamer
            .get_mempool_tx(Request::new(GetMempoolTxRequest {
                exclude_txid_suffixes: vec![vec![0u8; 33]],
                ..Default::default()
            }))
            .await
            .err()
            .unwrap();

        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn get_mempool_tx_rejects_too_many_exclude_suffixes() {
        let (_dir, streamer) = streamer_with_snapshot(snapshot_of(vec![]));

        let status = streamer
            .get_mempool_tx(Request::new(GetMempoolTxRequest {
                exclude_txid_suffixes: vec![vec![0u8]; MAX_EXCLUDE_TXID_SUFFIXES + 1],
                ..Default::default()
            }))
            .await
            .err()
            .unwrap();

        assert_eq!(status.code(), Code::ResourceExhausted);
    }

    #[tokio::test]
    async fn get_mempool_tx_rejects_invalid_pool_type() {
        let (_dir, streamer) = streamer_with_snapshot(snapshot_of(vec![]));

        let status = streamer
            .get_mempool_tx(Request::new(GetMempoolTxRequest {
                pool_types: vec![PoolType::Invalid as i32],
                ..Default::default()
            }))
            .await
            .err()
            .unwrap();

        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn get_mempool_tx_applies_pool_filtering() {
        let (raw, _, _) = shielded_v5_tx();
        let entry = entry_from(&raw);
        let (_dir, streamer) = streamer_with_snapshot(snapshot_of(vec![entry]));

        // The vector carries Sapling outputs and Orchard actions; transparent-only strips them.
        let txs = mempool_txs(
            &streamer,
            GetMempoolTxRequest {
                pool_types: vec![PoolType::Transparent as i32],
                ..Default::default()
            },
        )
        .await;

        assert_eq!(txs.len(), 1);
        assert!(txs[0].spends.is_empty() && txs[0].outputs.is_empty() && txs[0].actions.is_empty());
    }

    #[test]
    fn ambiguous_exclude_suffix_excludes_no_matching_tx() {
        let a: &[u8] = &[0x11, 0x22, 0xff];
        let b: &[u8] = &[0x33, 0x44, 0xff];
        let c: &[u8] = &[0x55, 0x66, 0x77];
        let wire_txids = [a, b, c];
        // `[0xff]` matches a and b, so it is ambiguous and excludes neither; `[0x77]` matches only c.
        let exclude = vec![vec![0xff], vec![0x77]];
        assert_eq!(
            super::excluded_by_suffixes(&wire_txids, &exclude),
            vec![false, false, true]
        );
    }

    #[test]
    fn unique_exclude_suffix_excludes_only_its_match() {
        let a: &[u8] = &[0x11, 0x22, 0x33];
        let b: &[u8] = &[0x44, 0x55, 0x66];
        let wire_txids = [a, b];
        let exclude = vec![vec![0x33]];
        assert_eq!(
            super::excluded_by_suffixes(&wire_txids, &exclude),
            vec![true, false]
        );
    }
}
