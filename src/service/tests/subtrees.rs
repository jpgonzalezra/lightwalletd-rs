//! Unit tests for the subtree-roots method.

use std::sync::Arc;
use std::time::Duration;

use tokio_stream::StreamExt;
use tonic::{Code, Request};

use crate::encoding;
use crate::node::{GetSubtrees, Subtree};
use crate::proto::compact_tx_streamer_server::CompactTxStreamer;
use crate::proto::{GetSubtreeRootsArg, ShieldedProtocol, SubtreeRoot};
use crate::service::Streamer;
use crate::testutil::{
    FakeNode, fake_node_serving, temp_cache, testdata_blocks, verbose_for_raw_block,
};

use super::streamer_with;

/// A subtree the node reports as completed by the block at `end_height`, with a root derived from
/// that height so the assertions can tell one from another.
fn subtree(end_height: u64) -> Subtree {
    Subtree {
        root: hex::encode([end_height as u8; 32]),
        end_height,
    }
}

/// A display-order block hash derived from `height`, in the hex shape `getblockhash` answers with.
fn hash_hex(height: u64) -> String {
    hex::encode([height as u8 ^ 0x5a; 32])
}

/// Every root the streamer serves for a request over `protocol`, starting at `start_index`.
async fn roots_of(
    streamer: &Streamer,
    protocol: ShieldedProtocol,
    start_index: u32,
) -> Vec<SubtreeRoot> {
    let stream = streamer
        .get_subtree_roots(Request::new(GetSubtreeRootsArg {
            start_index,
            shielded_protocol: protocol as i32,
            max_entries: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    stream.map(Result::unwrap).collect().await
}

// The testnet reality right after NU6.3 activation: the node accepts the `ironwood` pool but has
// no completed subtrees yet. The stream must end cleanly with zero items, not error.
#[tokio::test]
async fn ironwood_subtree_roots_with_no_subtrees_is_an_empty_stream() {
    let (_dir, streamer) = streamer_with(Arc::new(FakeNode {
        subtrees: Some(GetSubtrees { subtrees: vec![] }),
        ..Default::default()
    }));

    let stream = streamer
        .get_subtree_roots(Request::new(GetSubtreeRootsArg {
            start_index: 0,
            shielded_protocol: ShieldedProtocol::Ironwood as i32,
            max_entries: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    let roots: Vec<_> = stream.collect().await;

    assert!(roots.is_empty());
}

// A pre-NU6.3 node rejects `z_getsubtreesbyindex ironwood ...` outright, because it doesn't
// recognize the pool name at all (zebra-rpc's `POOL_LIST` is `["sapling", "orchard"]` before the
// Ironwood RPC support lands). That's not a server failure (the subtree can't exist yet), so the
// stream must still end cleanly with zero items, exactly like the "recognized but empty" case above.
#[tokio::test]
async fn pre_ironwood_node_error_yields_an_empty_stream() {
    let (_dir, streamer) = streamer_with(Arc::new(FakeNode {
        subtrees_err: Some((
            -1,
            "invalid pool name, must be one of: [\"sapling\", \"orchard\"]".to_string(),
        )),
        ..Default::default()
    }));

    let stream = streamer
        .get_subtree_roots(Request::new(GetSubtreeRootsArg {
            start_index: 0,
            shielded_protocol: ShieldedProtocol::Ironwood as i32,
            max_entries: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    let roots: Vec<_> = stream.collect().await;

    assert!(roots.is_empty());
}

// The protocol types a subtree index as `uint32`, but the node addresses subtrees with a `u16`.
// `u16::MAX` is the last index it can answer for, and it must still reach the node: rejecting it
// would refuse a legitimate request.
#[tokio::test]
async fn highest_addressable_start_index_reaches_the_node() {
    let node = Arc::new(FakeNode {
        subtrees: Some(GetSubtrees { subtrees: vec![] }),
        ..Default::default()
    });
    let (_dir, streamer) = streamer_with(node.clone());

    streamer
        .get_subtree_roots(Request::new(GetSubtreeRootsArg {
            start_index: u16::MAX as u32,
            shielded_protocol: ShieldedProtocol::Sapling as i32,
            max_entries: 0,
        }))
        .await
        .unwrap();

    assert_eq!(
        *node.requested_subtree_params.lock().unwrap(),
        Some(("sapling".to_string(), u16::MAX as u32, 0))
    );
}

// One past what the node can address. The node would answer `Invalid params`, which carries a
// generic code and would surface as a retryable `Unavailable`, so a wallet would keep retrying
// input that can never succeed. It has to be rejected here as the client error it is, without
// consulting the node at all.
#[tokio::test]
async fn start_index_above_the_node_range_is_rejected_without_a_node_call() {
    let node = Arc::new(FakeNode {
        subtrees: Some(GetSubtrees { subtrees: vec![] }),
        ..Default::default()
    });
    let (_dir, streamer) = streamer_with(node.clone());

    let status = streamer
        .get_subtree_roots(Request::new(GetSubtreeRootsArg {
            start_index: u16::MAX as u32 + 1,
            shielded_protocol: ShieldedProtocol::Sapling as i32,
            max_entries: 0,
        }))
        .await
        .err()
        .unwrap();

    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(*node.requested_subtree_params.lock().unwrap(), None);
}

// A limit is a ceiling, not a position: one past the range asks for all of the subtrees, which is
// exactly what the unlimited `0` expresses. Clamping to `u16::MAX` would instead cap the count one
// short of the `u16::MAX + 1` subtrees a full pool can hold.
#[tokio::test]
async fn max_entries_above_the_node_range_is_forwarded_as_unlimited() {
    let node = Arc::new(FakeNode {
        subtrees: Some(GetSubtrees { subtrees: vec![] }),
        ..Default::default()
    });
    let (_dir, streamer) = streamer_with(node.clone());

    streamer
        .get_subtree_roots(Request::new(GetSubtreeRootsArg {
            start_index: 0,
            shielded_protocol: ShieldedProtocol::Orchard as i32,
            max_entries: 70_000,
        }))
        .await
        .unwrap();

    assert_eq!(
        *node.requested_subtree_params.lock().unwrap(),
        Some(("orchard".to_string(), 0, 0))
    );
}

// `max_entries == 0` means "no limit" and is forwarded untouched, so that `get_subtrees` keeps
// omitting the third JSON-RPC parameter instead of capping the response at zero entries.
#[tokio::test]
async fn unset_max_entries_is_forwarded_as_zero() {
    let node = Arc::new(FakeNode {
        subtrees: Some(GetSubtrees { subtrees: vec![] }),
        ..Default::default()
    });
    let (_dir, streamer) = streamer_with(node.clone());

    streamer
        .get_subtree_roots(Request::new(GetSubtreeRootsArg {
            start_index: 0,
            shielded_protocol: ShieldedProtocol::Sapling as i32,
            max_entries: 0,
        }))
        .await
        .unwrap();

    assert_eq!(
        *node.requested_subtree_params.lock().unwrap(),
        Some(("sapling".to_string(), 0, 0))
    );
}

// An unrelated node error (anything not matching the unrecognized-pool message) must still surface
// as a failed RPC, not be swallowed into an empty stream.
#[tokio::test]
async fn unrelated_node_error_still_propagates() {
    let (_dir, streamer) = streamer_with(Arc::new(FakeNode {
        subtrees_err: Some((-1, "some other failure".to_string())),
        ..Default::default()
    }));

    let status = streamer
        .get_subtree_roots(Request::new(GetSubtreeRootsArg {
            start_index: 0,
            shielded_protocol: ShieldedProtocol::Sapling as i32,
            max_entries: 0,
        }))
        .await
        .err()
        .unwrap();

    assert_eq!(status.code(), Code::Unavailable);
}

// The completing block is needed for its hash alone, and the node's answer already carries the
// height, so a height lookup takes the place of reading the block back. The node here serves whole
// blocks as well, so what this pins is that they are never asked for, not merely that the roots
// come out right.
#[tokio::test]
async fn completing_blocks_are_resolved_without_fetching_them() {
    let raws = testdata_blocks();
    // The real hash of each block, so the node answers a height lookup and a block fetch with the
    // same thing: what separates the two paths is then the cost, not the result.
    let blocks: Vec<(u64, String)> = raws
        .iter()
        .map(|raw| {
            (
                crate::compact::to_compact_block(raw).unwrap().height,
                verbose_for_raw_block(raw).0,
            )
        })
        .collect();
    let node = Arc::new(FakeNode {
        subtrees: Some(GetSubtrees {
            subtrees: blocks.iter().map(|&(height, _)| subtree(height)).collect(),
        }),
        hash_by_height: blocks.iter().cloned().collect(),
        ..fake_node_serving(&raws)
    });
    let (_dir, streamer) = streamer_with(node.clone());

    let roots = roots_of(&streamer, ShieldedProtocol::Sapling, 0).await;

    assert_eq!(
        roots,
        blocks
            .iter()
            .map(|(height, hash)| SubtreeRoot {
                root_hash: vec![*height as u8; 32],
                completing_block_hash: hex::decode(hash).unwrap(),
                completing_block_height: *height,
            })
            .collect::<Vec<_>>()
    );
    assert_eq!(*node.block_verbose_calls.lock().unwrap(), 0);
}

// A cached completing block never reaches the node, and its transactions are never decoded: the
// block here is stored with a `vtx` entry holding a truncated varint, so anything that walks into
// it fails. The hash is stored in protocol order and leaves in display order.
#[tokio::test]
async fn a_cached_completing_block_answers_without_decoding_its_transactions() {
    let node = Arc::new(FakeNode {
        subtrees: Some(GetSubtrees {
            subtrees: vec![subtree(200)],
        }),
        ..Default::default()
    });
    let (_dir, cache) = temp_cache();
    let cached_hash: Vec<u8> = (0..32u8).collect();
    let mut stored = vec![0x1a, 32];
    stored.extend_from_slice(&cached_hash);
    stored.extend_from_slice(&[0x3a, 1, 0x08]);
    cache.insert_raw(200, &stored).unwrap();
    let streamer = Streamer::new(node.clone(), Arc::new(cache), "main".to_string(), None);

    let roots = roots_of(&streamer, ShieldedProtocol::Sapling, 0).await;

    assert_eq!(
        roots,
        vec![SubtreeRoot {
            root_hash: vec![200u8; 32],
            completing_block_hash: encoding::wire_to_display_bytes(&cached_hash),
            completing_block_height: 200,
        }]
    );
    assert_eq!(*node.block_hash_calls.lock().unwrap(), 0);
}

// Subtree indexes run `start_index..=MAX_SUBTREE_INDEX`, so a request starting at the last one can
// be answered by at most a single subtree. A node returning more than it can address is answering
// with subtrees nobody can ask for again, and how much work this request does must not be its call.
#[tokio::test]
async fn a_node_answer_past_the_addressable_range_is_dropped() {
    let node = Arc::new(FakeNode {
        subtrees: Some(GetSubtrees {
            subtrees: vec![subtree(100), subtree(200), subtree(300)],
        }),
        hash_by_height: [100, 200, 300]
            .into_iter()
            .map(|height| (height, hash_hex(height)))
            .collect(),
        ..Default::default()
    });
    let (_dir, streamer) = streamer_with(node.clone());

    let roots = roots_of(&streamer, ShieldedProtocol::Sapling, u16::MAX as u32).await;

    assert_eq!(
        roots
            .iter()
            .map(|root| root.completing_block_height)
            .collect::<Vec<_>>(),
        vec![100]
    );
}

// A node that stops answering must not pin the request for as long as it takes: the work one
// request can trigger is bounded in time, the way the transparent-address scan already is.
#[tokio::test(start_paused = true)]
async fn a_node_that_stops_answering_hits_the_request_deadline() {
    let (_dir, streamer) = streamer_with(Arc::new(FakeNode {
        subtrees: Some(GetSubtrees { subtrees: vec![] }),
        subtrees_delay: Some(Duration::from_secs(600)),
        ..Default::default()
    }));

    let status = streamer
        .get_subtree_roots(Request::new(GetSubtreeRootsArg {
            start_index: 0,
            shielded_protocol: ShieldedProtocol::Sapling as i32,
            max_entries: 0,
        }))
        .await
        .err()
        .unwrap();

    assert_eq!(status.code(), Code::DeadlineExceeded);
}

// The same bound has to hold once the request is past the subtree query: the height lookups behind
// the roots are the part that fans out, so a node that stalls there must not be able to hold the
// request either.
#[tokio::test(start_paused = true)]
async fn a_stalled_height_lookup_hits_the_request_deadline() {
    let (_dir, streamer) = streamer_with(Arc::new(FakeNode {
        subtrees: Some(GetSubtrees {
            subtrees: vec![subtree(900)],
        }),
        hash_by_height: [(900, hash_hex(900))].into_iter().collect(),
        block_hash_delay: Some(Duration::from_secs(600)),
        ..Default::default()
    }));

    let status = streamer
        .get_subtree_roots(Request::new(GetSubtreeRootsArg {
            start_index: 0,
            shielded_protocol: ShieldedProtocol::Sapling as i32,
            max_entries: 0,
        }))
        .await
        .err()
        .unwrap();

    assert_eq!(status.code(), Code::DeadlineExceeded);
}
