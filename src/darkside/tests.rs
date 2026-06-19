use std::sync::Arc;

use tokio_stream::StreamExt;
use tonic::Request;

use crate::compact;
use crate::node::NodeRpc;
use crate::proto::compact_tx_streamer_server::CompactTxStreamer;
use crate::proto::{
    BlockId, BlockRange, DarksideMetaState, DarksideSubtreeRoots, GetAddressUtxosReply,
    GetMempoolTxRequest, GetSubtreeRootsArg, ShieldedProtocol, SubtreeRoot,
};
use crate::service::Streamer;
use crate::testutil::{shielded_v5_tx, temp_cache, testdata_blocks};

use super::block::{raw_block_height, synthetic_block};
use super::{DarksideError, DarksideHandle, DarksideNode, DarksideState};

/// Sapling activation height of the consecutive blocks in `testdata/blocks` (380640..=380643).
const START_HEIGHT: u64 = 380640;

fn meta(start_height: u64) -> DarksideMetaState {
    DarksideMetaState {
        sapling_activation: start_height as i32,
        branch_id: "2bb40e60".to_string(),
        chain_name: "main".to_string(),
        start_sapling_commitment_tree_size: 0,
        start_orchard_commitment_tree_size: 0,
    }
}

/// State with the first `n` blocks staged and applied, wrapped in a shared handle.
async fn applied_handle(n: usize) -> DarksideHandle {
    let mut state = DarksideState::new();
    state.reset(&meta(START_HEIGHT));
    for raw in testdata_blocks().into_iter().take(n) {
        state.stage_block(raw).unwrap();
    }
    state
        .apply_staged(START_HEIGHT as i64 + n as i64 - 1)
        .unwrap();
    Arc::new(tokio::sync::Mutex::new(state))
}

// --- Stage/apply engine ----------------------------------------------------------------------

#[test]
fn apply_staged_chains_three_blocks() {
    let mut state = DarksideState::new();
    state.reset(&meta(START_HEIGHT));
    for raw in testdata_blocks().into_iter().take(3) {
        state.stage_block(raw).unwrap();
    }
    state.apply_staged(380642).unwrap();

    assert_eq!(state.active.len(), 3);
    assert_eq!(state.latest_height, 380642);
    for index in 1..state.active.len() {
        assert_eq!(
            state.active[index].header[4..36],
            state.active[index - 1].hash(),
            "block {index} prev hash should chain onto its predecessor"
        );
    }
}

#[test]
fn apply_staged_reorg_rewrites_from_staged_height() {
    let mut state = DarksideState::new();
    state.reset(&meta(START_HEIGHT));
    for raw in testdata_blocks().into_iter().take(3) {
        state.stage_block(raw).unwrap();
    }
    state.apply_staged(380642).unwrap();
    let original_hash = state.active[1].hash();

    // A different block at height 380641 reorgs the chain from there: 380641 is replaced and
    // 380642 is dropped.
    state
        .stage_block(synthetic_block(380641, 99).unwrap())
        .unwrap();
    state.apply_staged(380641).unwrap();

    assert_eq!(state.active.len(), 2);
    assert_ne!(state.active[1].hash(), original_hash);
    assert_eq!(state.active[1].header[4..36], state.active[0].hash());
}

#[test]
fn apply_staged_starts_tree_sizes_from_reset() {
    let mut state = DarksideState::new();
    let mut meta = meta(START_HEIGHT);
    meta.start_sapling_commitment_tree_size = 100;
    meta.start_orchard_commitment_tree_size = 200;
    state.reset(&meta);
    state.stage_block(testdata_blocks()[0].clone()).unwrap();
    state.apply_staged(380640).unwrap();

    // The first testdata block is pre-Sapling (no shielded outputs), so the sizes equal the
    // configured start sizes.
    assert_eq!(state.active[0].sapling_size, 100);
    assert_eq!(state.active[0].orchard_size, 200);
}

#[test]
fn apply_staged_accumulates_mined_transaction_tree_sizes() {
    let (tx, sapling_outputs, orchard_actions) = shielded_v5_tx();
    assert!(sapling_outputs > 0 && orchard_actions > 0);

    let mut state = DarksideState::new();
    state.reset(&meta(START_HEIGHT));
    for raw in testdata_blocks().into_iter().take(3) {
        state.stage_block(raw).unwrap();
    }
    // Mine the shielded transaction into the middle block (height 380641, index 1).
    state.stage_transaction(380641, tx.clone()).unwrap();
    state.apply_staged(380642).unwrap();

    // The block before the mined transaction is unchanged; the mined block and every later block
    // grow by the transaction's shielded note counts.
    assert_eq!(
        (state.active[0].sapling_size, state.active[0].orchard_size),
        (0, 0)
    );
    assert_eq!(
        (state.active[1].sapling_size, state.active[1].orchard_size),
        (sapling_outputs, orchard_actions)
    );
    assert_eq!(
        (state.active[2].sapling_size, state.active[2].orchard_size),
        (sapling_outputs, orchard_actions)
    );

    // The mined block now reconstructs with the extra transaction (coinbase + mined tx).
    let raw = state.active[1].to_raw();
    let (_, rebuilt) = compact::split_block(&raw).unwrap();
    assert_eq!(rebuilt.len(), 2);
    assert_eq!(rebuilt[1], tx);
}

#[test]
fn stage_blocks_create_builds_parseable_consecutive_blocks() {
    let mut state = DarksideState::new();
    state.reset(&meta(1000));
    state.stage_blocks_create(1000, 7, 3).unwrap();
    state.apply_staged(1002).unwrap();

    assert_eq!(state.active.len(), 3);
    for (index, block) in state.active.iter().enumerate() {
        assert_eq!(
            raw_block_height(&block.to_raw()).unwrap(),
            1000 + index as u64
        );
    }
}

#[test]
fn stage_block_rejects_height_below_sapling_activation() {
    let mut state = DarksideState::new();
    state.reset(&meta(500000));
    let error = state.stage_block(testdata_blocks()[0].clone()).unwrap_err();
    assert!(matches!(error, DarksideError::Invalid(_)));
}

// --- Mempool: staging area served as mempool -------------------------------------------------

#[test]
fn raw_mempool_lists_staged_transactions() {
    let (tx, _, _) = shielded_v5_tx();
    let mut state = DarksideState::new();
    state.reset(&meta(START_HEIGHT));
    state.stage_transaction(START_HEIGHT, tx.clone()).unwrap();

    assert_eq!(
        state.raw_mempool().unwrap(),
        vec![compact::txid_display(&tx).unwrap()]
    );
}

#[test]
fn raw_mempool_lists_staged_block_transactions() {
    let block = testdata_blocks()[3].clone();
    let (_, txs) = compact::split_block(&block).unwrap();
    let expected: Vec<String> = txs
        .iter()
        .map(|tx| compact::txid_display(tx).unwrap())
        .collect();

    let mut state = DarksideState::new();
    state.reset(&meta(START_HEIGHT));
    state.stage_block(block).unwrap();

    assert_eq!(state.raw_mempool().unwrap(), expected);
}

#[test]
fn raw_mempool_empty_after_apply_clears_staging() {
    let (tx, _, _) = shielded_v5_tx();
    let mut state = DarksideState::new();
    state.reset(&meta(START_HEIGHT));
    state.stage_block(testdata_blocks()[0].clone()).unwrap();
    state.stage_transaction(START_HEIGHT, tx).unwrap();
    state.apply_staged(START_HEIGHT as i64).unwrap();

    assert!(state.raw_mempool().unwrap().is_empty());
}

#[test]
fn raw_transaction_finds_staged_tx_at_height_zero() {
    let (tx, _, _) = shielded_v5_tx();
    let mut state = DarksideState::new();
    state.reset(&meta(START_HEIGHT));
    state.stage_transaction(START_HEIGHT, tx.clone()).unwrap();

    let found = state
        .raw_transaction(&compact::txid_display(&tx).unwrap())
        .unwrap();
    assert_eq!(found.height, 0);
    assert_eq!(found.hex, hex::encode(&tx));
}

#[test]
fn raw_transaction_prefers_active_block_height() {
    let mut state = DarksideState::new();
    state.reset(&meta(START_HEIGHT));
    for raw in testdata_blocks().into_iter().take(4) {
        state.stage_block(raw).unwrap();
    }
    state.apply_staged(START_HEIGHT as i64 + 3).unwrap();

    // The same transaction is both mined (in block 380643) and re-staged; the active copy wins
    // and reports its block height, not the staged-transaction sentinel 0.
    let (_, txs) = compact::split_block(&testdata_blocks()[3]).unwrap();
    state
        .stage_transaction(START_HEIGHT, txs[1].clone())
        .unwrap();

    let found = state
        .raw_transaction(&compact::txid_display(&txs[1]).unwrap())
        .unwrap();
    assert_eq!(found.height, START_HEIGHT as i64 + 3);
}

// --- DarksideNode + Streamer -----------------------------------------------------------------

#[tokio::test]
async fn darkside_node_serves_block_reads() {
    let handle = applied_handle(3).await;
    let node = DarksideNode::new(handle.clone());

    assert_eq!(node.get_block_count().await.unwrap(), 380642);

    let info = node.get_blockchain_info().await.unwrap();
    assert_eq!(info.chain, "main");
    assert_eq!(info.blocks, 380642);

    let verbose = node.get_block_verbose(380640).await.unwrap();
    let raw = node.get_block_raw(&verbose.hash).await.unwrap();
    assert_eq!(raw_block_height(&raw).unwrap(), 380640);
}

#[tokio::test]
async fn darkside_node_send_transaction_populates_incoming() {
    let handle = applied_handle(1).await;
    let node = DarksideNode::new(handle.clone());
    let (_, txs) = compact::split_block(&testdata_blocks()[3]).unwrap();
    let tx = txs[1].clone();

    let txid = node.send_raw_transaction(&hex::encode(&tx)).await.unwrap();

    assert_eq!(txid, compact::txid_display(&tx).unwrap());
    assert_eq!(handle.lock().await.incoming_txs, vec![tx]);
}

#[tokio::test]
async fn darkside_node_returns_staged_utxos() {
    let handle = applied_handle(1).await;
    handle.lock().await.add_utxo(GetAddressUtxosReply {
        address: "t1".to_string(),
        txid: vec![0x11, 0x22, 0x33, 0x44],
        index: 0,
        script: vec![0xab, 0xcd],
        value_zat: 5,
        height: 380640,
    });
    let node = DarksideNode::new(handle);

    let utxos = node.get_address_utxos(&["t1".to_string()]).await.unwrap();

    assert_eq!(utxos.len(), 1);
    // The wire txid is returned in display (reversed) order, as zebrad would report it.
    assert_eq!(utxos[0].txid, "44332211");
    assert_eq!(utxos[0].satoshis, 5);
}

#[tokio::test]
async fn streamer_get_block_range_emits_staged_blocks() {
    let handle = applied_handle(3).await;
    let node: Arc<dyn NodeRpc> = Arc::new(DarksideNode::new(handle.clone()));
    let (_dir, cache) = temp_cache();
    let streamer = Streamer::new(node, Arc::new(cache), "main".to_string(), Some(handle));

    let range = BlockRange {
        start: Some(BlockId {
            height: 380640,
            hash: vec![],
        }),
        end: Some(BlockId {
            height: 380642,
            hash: vec![],
        }),
        ..Default::default()
    };
    let response = streamer
        .get_block_range(Request::new(range))
        .await
        .unwrap()
        .into_inner();
    let emitted: Vec<_> = response.collect().await;

    let heights: Vec<u64> = emitted
        .iter()
        .map(|block| block.as_ref().unwrap().height)
        .collect();
    assert_eq!(heights, vec![380640, 380641, 380642]);
    // The compact blocks chain: each block's prev hash is the previous block's hash.
    assert_eq!(
        emitted[1].as_ref().unwrap().prev_hash,
        emitted[0].as_ref().unwrap().hash
    );
}

#[tokio::test]
async fn streamer_get_subtree_roots_serves_staged_roots() {
    let handle = applied_handle(1).await;
    handle.lock().await.set_subtree_roots(DarksideSubtreeRoots {
        shielded_protocol: ShieldedProtocol::Sapling as i32,
        start_index: 0,
        subtree_roots: vec![SubtreeRoot {
            root_hash: vec![1, 2, 3],
            completing_block_hash: vec![4, 5, 6],
            completing_block_height: 380640,
        }],
    });
    let node: Arc<dyn NodeRpc> = Arc::new(DarksideNode::new(handle.clone()));
    let (_dir, cache) = temp_cache();
    let streamer = Streamer::new(node, Arc::new(cache), "main".to_string(), Some(handle));

    let arg = GetSubtreeRootsArg {
        start_index: 0,
        shielded_protocol: ShieldedProtocol::Sapling as i32,
        max_entries: 0,
    };
    let response = streamer
        .get_subtree_roots(Request::new(arg))
        .await
        .unwrap()
        .into_inner();
    let roots: Vec<_> = response.collect().await;

    assert_eq!(roots.len(), 1);
    let root = roots[0].as_ref().unwrap();
    assert_eq!(root.completing_block_height, 380640);
    assert_eq!(root.root_hash, vec![1, 2, 3]);
}

#[tokio::test]
async fn streamer_get_mempool_tx_emits_staged_transaction() {
    let handle = applied_handle(3).await;
    let (tx, _, _) = shielded_v5_tx();
    // Stage a transaction without applying it: it stays in the mempool (the staging area).
    handle
        .lock()
        .await
        .stage_transaction(START_HEIGHT, tx.clone())
        .unwrap();
    let node: Arc<dyn NodeRpc> = Arc::new(DarksideNode::new(handle.clone()));
    let (_dir, cache) = temp_cache();
    let streamer = Streamer::new(node, Arc::new(cache), "main".to_string(), Some(handle));

    let expected_txid = compact::compact_tx_from_raw(0, &tx).unwrap().txid;

    let emitted: Vec<_> = streamer
        .get_mempool_tx(Request::new(GetMempoolTxRequest::default()))
        .await
        .unwrap()
        .into_inner()
        .collect()
        .await;
    assert_eq!(emitted.len(), 1);
    assert_eq!(emitted[0].as_ref().unwrap().txid, expected_txid);

    // An exclusion suffix matching the txid (compared in protocol order) drops it.
    let excluded: Vec<_> = streamer
        .get_mempool_tx(Request::new(GetMempoolTxRequest {
            exclude_txid_suffixes: vec![expected_txid],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner()
        .collect()
        .await;
    assert!(excluded.is_empty());
}
