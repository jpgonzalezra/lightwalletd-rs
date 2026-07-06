//! Subtree roots and tree state staged through the control plane and read back: `GetSubtreeRoots`
//! serves the staged roots (honoring start index and limit), and `GetTreeState` looks up by height
//! and by hash.

mod common;

use common::TestServer;
use lightwalletd_rs::proto::{
    BlockId, DarksideSubtreeRoots, GetSubtreeRootsArg, ShieldedProtocol, SubtreeRoot, TreeState,
};

fn root(index_marker: u8, height: u64) -> SubtreeRoot {
    SubtreeRoot {
        root_hash: vec![index_marker; 32],
        completing_block_hash: vec![index_marker; 32],
        completing_block_height: height,
    }
}

async fn subtree_roots(
    server: &mut TestServer,
    start_index: u32,
    max_entries: u32,
) -> Vec<SubtreeRoot> {
    let mut stream = server
        .compact
        .get_subtree_roots(GetSubtreeRootsArg {
            start_index,
            shielded_protocol: ShieldedProtocol::Sapling as i32,
            max_entries,
        })
        .await
        .unwrap()
        .into_inner();
    let mut roots = Vec::new();
    while let Some(root) = stream.message().await.unwrap() {
        roots.push(root);
    }
    roots
}

#[tokio::test]
async fn subtree_roots_served_with_start_index_and_limit() {
    let mut server = TestServer::start().await;
    server.reset(663150, "bad", "x").await;

    let staged = vec![root(0, 663150), root(1, 663200), root(2, 663250)];
    server
        .darkside
        .set_subtree_roots(DarksideSubtreeRoots {
            shielded_protocol: ShieldedProtocol::Sapling as i32,
            start_index: 0,
            subtree_roots: staged.clone(),
        })
        .await
        .unwrap();

    assert_eq!(subtree_roots(&mut server, 0, 0).await, staged);
    // start_index skips the prefix; max_entries caps the count.
    assert_eq!(
        subtree_roots(&mut server, 1, 1).await,
        vec![root(1, 663200)]
    );
}

fn staged_tree_state() -> TreeState {
    TreeState {
        network: "main".to_string(),
        height: 663190,
        hash: "0000000000000000000000000000000000000000000000000000000000abcdef".to_string(),
        time: 1,
        sapling_tree: "aa".to_string(),
        orchard_tree: "bb".to_string(),
        ironwood_tree: String::new(),
    }
}

#[tokio::test]
async fn tree_state_looked_up_by_height() {
    let mut server = TestServer::start().await;
    server.reset(663150, "bad", "x").await;

    let tree_state = staged_tree_state();
    server
        .darkside
        .add_tree_state(tree_state.clone())
        .await
        .unwrap();

    let by_height = server
        .compact
        .get_tree_state(BlockId {
            height: 663190,
            hash: vec![],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(by_height, tree_state);
}

#[tokio::test]
async fn tree_state_lookup_by_hash_is_unimplemented() {
    let mut server = TestServer::start().await;
    server.reset(663150, "bad", "x").await;
    server
        .darkside
        .add_tree_state(staged_tree_state())
        .await
        .unwrap();

    // The service does not yet support GetTreeState by hash, even though darkside can resolve it.
    let by_hash = server
        .compact
        .get_tree_state(BlockId {
            height: 0,
            hash: vec![0xab; 32],
        })
        .await;
    assert_eq!(by_hash.unwrap_err().code(), tonic::Code::Unimplemented);
}
