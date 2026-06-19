//! Shared end-to-end harness: an in-process darkside server on an ephemeral port, exercised over the
//! wire through both generated clients.
//!
//! `dead_code` is allowed module-wide because each integration test file compiles this module into its
//! own binary and uses only the helpers it needs.
#![allow(dead_code)]

use lightwalletd_rs::proto::compact_tx_streamer_client::CompactTxStreamerClient;
use lightwalletd_rs::proto::compact_tx_streamer_server::CompactTxStreamerServer;
use lightwalletd_rs::proto::darkside_streamer_client::DarksideStreamerClient;
use lightwalletd_rs::proto::darkside_streamer_server::DarksideStreamerServer;
use lightwalletd_rs::proto::{
    DarksideBlock, DarksideEmptyBlocks, DarksideHeight, DarksideMetaState, RawTransaction,
};
use tonic::transport::{Channel, Endpoint, Server};

/// A running darkside server plus both clients connected to it. Dropping it aborts the server task
/// (closing the listener), so no port or task leaks between tests.
pub struct TestServer {
    pub compact: CompactTxStreamerClient<Channel>,
    pub darkside: DarksideStreamerClient<Channel>,
    server: tokio::task::JoinHandle<()>,
    _cache_dir: tempfile::TempDir,
}

impl TestServer {
    /// Wire the darkside components with the shared `lightwalletd_rs::darkside_components` constructor
    /// (the same one `run`'s darkside branch uses), serve on `127.0.0.1:0`, and connect both clients.
    pub async fn start() -> Self {
        let cache_dir = tempfile::tempdir().unwrap();
        let (streamer, darkside_service, _state, _shutdown) =
            lightwalletd_rs::darkside_components(&cache_dir.path().join("blocks.redb")).unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(CompactTxStreamerServer::new(streamer))
                .add_service(DarksideStreamerServer::new(darkside_service))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });

        // The listener is already bound, so a lazy channel connects on the first RPC.
        let channel = Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect_lazy();

        Self {
            compact: CompactTxStreamerClient::new(channel.clone()),
            darkside: DarksideStreamerClient::new(channel),
            server,
            _cache_dir: cache_dir,
        }
    }

    /// `Reset` the mock chain with the given Sapling activation height, branch id (hex), and chain name.
    pub async fn reset(&mut self, sapling_activation: i32, branch_id: &str, chain_name: &str) {
        self.darkside
            .reset(DarksideMetaState {
                sapling_activation,
                branch_id: branch_id.to_string(),
                chain_name: chain_name.to_string(),
                start_sapling_commitment_tree_size: 0,
                start_orchard_commitment_tree_size: 0,
            })
            .await
            .unwrap();
    }

    /// Stage raw blocks through `StageBlocksStream`.
    pub async fn stage_blocks(&mut self, blocks: Vec<Vec<u8>>) {
        let stream = tokio_stream::iter(blocks.into_iter().map(|raw| DarksideBlock {
            block: hex::encode(raw),
        }));
        self.darkside.stage_blocks_stream(stream).await.unwrap();
    }

    /// Stage `count` synthetic empty blocks at consecutive heights starting at `height`.
    pub async fn stage_blocks_create(&mut self, height: i32, nonce: i32, count: i32) {
        self.darkside
            .stage_blocks_create(DarksideEmptyBlocks {
                height,
                nonce,
                count,
            })
            .await
            .unwrap();
    }

    /// Stage raw transactions to be mined into the block at `height` through `StageTransactionsStream`.
    pub async fn stage_transactions(&mut self, height: u64, txs: Vec<Vec<u8>>) {
        let stream = tokio_stream::iter(
            txs.into_iter()
                .map(move |data| RawTransaction { data, height }),
        );
        self.darkside
            .stage_transactions_stream(stream)
            .await
            .unwrap();
    }

    /// `ApplyStaged`, advancing the presented tip to `height`.
    pub async fn apply_staged(&mut self, height: i32) {
        self.darkside
            .apply_staged(DarksideHeight { height })
            .await
            .unwrap();
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.server.abort();
    }
}

/// Display-order txid of the vendored `recv` transaction (one shielded input, two shielded outputs).
pub const RECV_TXID_DISPLAY: &str =
    "0821a89be7f2fc1311792c3fa1dd2171a8cdfb2effd98590cbd5ebcdcfcf491f";

/// Decode every non-empty hex line of a file under `testdata/`.
fn read_hex_lines(path: &str) -> Vec<Vec<u8>> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| hex::decode(line).unwrap())
        .collect()
}

/// The raw blocks in `testdata/blocks` (the four consecutive blocks 380640..=380643).
pub fn testdata_blocks() -> Vec<Vec<u8>> {
    read_hex_lines("testdata/blocks")
}

/// The vendored mainnet block at height 663150 (`basic-reorg` vector, no network).
pub fn basic_reorg_block() -> Vec<u8> {
    read_hex_lines("testdata/darkside/basic-reorg-663150.txt").remove(0)
}

/// The vendored mainnet `recv` transaction (`basic-reorg` vector, no network).
pub fn recv_tx() -> Vec<u8> {
    read_hex_lines("testdata/darkside/recv-tx.txt").remove(0)
}

/// The wire-order (protocol) bytes of a display-order hex txid.
pub fn wire_txid(display_hex: &str) -> Vec<u8> {
    let mut bytes = hex::decode(display_hex).unwrap();
    bytes.reverse();
    bytes
}
