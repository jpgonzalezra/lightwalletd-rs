//! The shape of the HTTP/2 DATA frames a streaming response leaves as.
//!
//! A gRPC message becomes its own DATA frame whenever the handler pends between messages, and a
//! peer that sees a long run of frames below 256 bytes closes the connection with
//! `ENHANCE_YOUR_CALM` (`h2`'s frame-overhead budget). Blocks served from the node pend on every
//! fetch, so this is where that shape is checked, over the transport a deployment actually runs.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use lightwalletd_rs::proto::compact_tx_streamer_client::CompactTxStreamerClient;
use lightwalletd_rs::proto::{BlockId, BlockRange};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_stream::StreamExt;

mod common;

/// Frame type of an HTTP/2 DATA frame (RFC 9113 §6.1).
const DATA: u8 = 0;

/// Payload below which `h2` charges a DATA frame against its connection budget
/// (`DEFAULT_DATA_FRAME_OVERHEAD_THRESHOLD`).
const FRAME_OVERHEAD_THRESHOLD: usize = 256;

/// Forward one connection to `upstream`, recording the length of every DATA frame the server sends.
///
/// Only the 9-byte frame headers are parsed, so no HPACK state is needed: the length and type are
/// in the clear at the front of each frame.
async fn measuring_proxy(upstream: SocketAddr) -> (SocketAddr, Arc<Mutex<Vec<usize>>>) {
    let frames = Arc::new(Mutex::new(Vec::new()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let recorded = frames.clone();
    tokio::spawn(async move {
        let (client, _) = listener.accept().await.unwrap();
        let server = tokio::net::TcpStream::connect(upstream).await.unwrap();
        client.set_nodelay(true).unwrap();
        server.set_nodelay(true).unwrap();
        let (mut client_read, mut client_write) = client.into_split();
        let (mut server_read, mut server_write) = server.into_split();
        tokio::spawn(async move {
            tokio::io::copy(&mut client_read, &mut server_write)
                .await
                .ok();
        });
        let mut header = [0u8; 9];
        while server_read.read_exact(&mut header).await.is_ok() {
            let length = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
            if header[3] == DATA {
                recorded.lock().unwrap().push(length);
            }
            let mut payload = vec![0u8; length];
            if server_read.read_exact(&mut payload).await.is_err()
                || client_write.write_all(&header).await.is_err()
                || client_write.write_all(&payload).await.is_err()
            {
                break;
            }
        }
    });
    (addr, frames)
}

/// Every DATA frame but the last one, which carries whatever the stream had left over.
fn frames_before_the_last(frames: &Arc<Mutex<Vec<usize>>>) -> Vec<usize> {
    let recorded = frames.lock().unwrap();
    recorded[..recorded.len() - 1].to_vec()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_served_range_leaves_as_full_data_frames() {
    const BLOCKS: i32 = 300;

    let mut server = common::TestServer::start().await;
    server.reset(1, "00000000", "main").await;
    server.stage_blocks_create(1, 0, BLOCKS).await;
    server.apply_staged(BLOCKS).await;

    let (proxy, frames) = measuring_proxy(server.addr).await;
    let mut client = CompactTxStreamerClient::connect(format!("http://{proxy}"))
        .await
        .unwrap();
    let mut stream = client
        .get_block_range(BlockRange {
            start: Some(BlockId {
                height: 1,
                hash: vec![],
            }),
            end: Some(BlockId {
                height: BLOCKS as u64,
                hash: vec![],
            }),
            pool_types: vec![],
        })
        .await
        .unwrap()
        .into_inner();

    let mut received = 0;
    while let Some(block) = stream.next().await {
        block.unwrap();
        received += 1;
    }
    assert_eq!(received, BLOCKS);

    let undersized: Vec<usize> = frames_before_the_last(&frames)
        .into_iter()
        .filter(|length| *length < FRAME_OVERHEAD_THRESHOLD)
        .collect();
    assert_eq!(undersized, Vec::<usize>::new());
}
