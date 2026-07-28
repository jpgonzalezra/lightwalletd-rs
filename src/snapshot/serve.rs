//! The HTTP endpoints that publish this instance's cache as a snapshot.
//!
//! Two routes: the manifest, which is a cheap read of the stored epoch digests, and one route per
//! epoch body, streamed straight out of `redb`. Only completed epochs are published, so a body is
//! immutable once it is reachable at all, which is what lets it carry a year-long `immutable`
//! cache directive and sit behind a CDN unchanged.
//!
//! Compression is negotiated per request and is invisible to the format: the digests are always
//! taken over the uncompressed body. If they covered compressed bytes they would depend on the
//! compressor's version and level, two servers holding identical blocks would publish different
//! digests, and the manifest would stop being portable.

use std::io::{self, Write};
use std::sync::Arc;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use tokio::sync::{Semaphore, mpsc};
use tower_http::compression::{CompressionLayer, CompressionLevel};

use crate::cache::Cache;
use crate::config::SnapshotServeConfig;

use super::export;
use super::format::EpochDigest;

/// Bytes buffered before a chunk is handed to the response stream.
const CHUNK_SIZE: usize = 64 * 1024;

/// Chunks that may sit in flight ahead of the client. Small on purpose: the export blocks once the
/// channel is full, so a slow client throttles the disk reads behind it instead of buffering an
/// entire epoch in memory.
const CHUNKS_IN_FLIGHT: usize = 4;

/// A completed epoch never changes, so it can be cached forever by anything in front of us.
const IMMUTABLE: &str = "public, max-age=31536000, immutable";

/// What the routes need to answer a request.
#[derive(Clone)]
struct SnapshotState {
    cache: Arc<Cache>,
    chain: Arc<str>,
    downloads: Arc<Semaphore>,
}

/// Serve the snapshot endpoints on the configured address until the process exits.
pub async fn serve(
    cache: Arc<Cache>,
    chain: String,
    config: SnapshotServeConfig,
) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(config.bind).await?;
    serve_on(listener, cache, chain, config).await
}

/// Serve on an already-bound listener, so a test can bind port 0 and learn where it landed.
async fn serve_on(
    listener: tokio::net::TcpListener,
    cache: Arc<Cache>,
    chain: String,
    config: SnapshotServeConfig,
) -> anyhow::Result<()> {
    let state = SnapshotState {
        cache,
        chain: chain.into(),
        downloads: Arc::new(Semaphore::new(config.max_concurrent_downloads)),
    };

    // Only the epoch route is worth compressing: the manifest is a few hundred bytes per epoch, and
    // the gain in a block stream lives between blocks rather than inside them.
    let mut epoch_route = get(epoch);
    if config.compression_level > 0 {
        epoch_route = epoch_route.layer(
            CompressionLayer::new()
                .zstd(true)
                .gzip(true)
                .quality(CompressionLevel::Precise(config.compression_level)),
        );
    }

    let app = Router::new()
        .route("/snapshot/manifest", get(manifest))
        .route("/snapshot/epoch/{index}", epoch_route)
        .with_state(state);
    axum::serve(listener, app).await?;
    Ok(())
}

/// `GET /snapshot/manifest`: everything this instance can currently serve.
///
/// Never cached: the epoch list grows as the tip crosses boundaries, and a consumer that comes back
/// for more must see the new entries.
async fn manifest(State(state): State<SnapshotState>) -> Response {
    let manifest = match export::manifest(&state.cache, &state.chain) {
        Ok(manifest) => manifest,
        Err(error) => {
            tracing::error!(%error, "building the snapshot manifest failed");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    match serde_json::to_vec(&manifest) {
        Ok(body) => (
            [
                (header::CONTENT_TYPE, "application/json"),
                (header::CACHE_CONTROL, "no-cache"),
            ],
            body,
        )
            .into_response(),
        Err(error) => {
            tracing::error!(%error, "serializing the snapshot manifest failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// `GET /snapshot/epoch/{index}`: one epoch body, streamed out of the cache.
async fn epoch(State(state): State<SnapshotState>, Path(index): Path<u64>) -> Response {
    let stored = match state.cache.epoch_digest(index) {
        Ok(Some(stored)) => stored,
        // Not published: either the epoch is above the tip, below this instance's range, or its
        // digests have not been computed yet.
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(error) => {
            tracing::error!(%error, epoch = index, "reading the epoch digest failed");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    let Some(digest) = EpochDigest::decode(&stored) else {
        tracing::error!(epoch = index, "stored epoch digest is malformed");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };

    // Refuse rather than queue: a queued slow client would hold a slot for as long as it liked.
    let Ok(permit) = Arc::clone(&state.downloads).try_acquire_owned() else {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            "snapshot download slots are all busy; retry shortly\n",
        )
            .into_response();
    };

    let (sender, mut receiver) = mpsc::channel::<Bytes>(CHUNKS_IN_FLIGHT);
    let cache = Arc::clone(&state.cache);
    let chain = state.chain.clone();
    tokio::task::spawn_blocking(move || {
        // Held for the whole export rather than the whole handler: what the cap is protecting is
        // the disk reads, and the client dropping the body ends this task through a send failure.
        let _permit = permit;
        let mut writer = ChunkWriter::new(sender);
        let written = export::write_epoch(&cache, &chain, index, &mut writer)
            .map_err(io::Error::other)
            .and_then(|()| writer.flush());
        if let Err(error) = written {
            // The status line is long gone by now, so this cannot become a 500. The consumer's
            // content digest is what turns a truncated body into a clean rejection.
            tracing::warn!(%error, epoch = index, "serving a snapshot epoch ended early");
        }
    });

    let stream = async_stream::stream! {
        while let Some(chunk) = receiver.recv().await {
            yield Ok::<Bytes, io::Error>(chunk);
        }
    };
    let response = Response::builder()
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::CONTENT_LENGTH, digest.bytes)
        .header(header::CACHE_CONTROL, IMMUTABLE)
        // Set here rather than left to the compression layer, which only adds it to responses it
        // actually compresses: without it on the uncompressed one, a cache in front could serve a
        // stored identity body to a client that asked for zstd, or the reverse.
        .header(header::VARY, header::ACCEPT_ENCODING.as_str())
        .body(Body::from_stream(stream));
    match response {
        Ok(response) => response,
        Err(error) => {
            tracing::error!(%error, epoch = index, "building the epoch response failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// A [`Write`] that hands buffered chunks to the response stream.
///
/// Blocks when the channel is full, which is what makes a slow client slow the export down instead
/// of letting an epoch pile up in memory.
struct ChunkWriter {
    sender: mpsc::Sender<Bytes>,
    buffer: Vec<u8>,
}

impl ChunkWriter {
    fn new(sender: mpsc::Sender<Bytes>) -> Self {
        Self {
            sender,
            buffer: Vec::with_capacity(CHUNK_SIZE),
        }
    }

    fn send(&mut self) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let chunk = Bytes::from(std::mem::replace(
            &mut self.buffer,
            Vec::with_capacity(CHUNK_SIZE),
        ));
        self.sender
            .blocking_send(chunk)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "the client stopped reading"))
    }
}

impl Write for ChunkWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        if self.buffer.len() >= CHUNK_SIZE {
            self.send()?;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.send()
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::ops::RangeInclusive;

    use super::*;
    use crate::proto::{ChainMetadata, CompactBlock, CompactTx, TxOut};
    use crate::snapshot::format::{Manifest, content_digest, parse_epoch};
    use crate::testutil::temp_cache;

    /// Heights 1..=10,000, so epoch 0 is complete and published as the partial range [1, 9999].
    const PUBLISHED_EPOCH: u64 = 0;

    fn block(height: u64, payload: usize) -> CompactBlock {
        let mut hash = vec![0xab; 32];
        hash[..8].copy_from_slice(&height.to_le_bytes());
        let mut prev_hash = vec![0xab; 32];
        prev_hash[..8].copy_from_slice(&(height - 1).to_le_bytes());
        CompactBlock {
            height,
            hash,
            prev_hash,
            chain_metadata: Some(ChainMetadata::default()),
            // Transparent outputs are the cheapest way to give a block a realistic size.
            vtx: if payload > 0 {
                vec![CompactTx {
                    vout: vec![TxOut {
                        value: height,
                        script_pub_key: vec![0x76; payload],
                    }],
                    ..Default::default()
                }]
            } else {
                Vec::new()
            },
            ..Default::default()
        }
    }

    /// A cache holding `heights`, with every complete epoch's digests already stored.
    fn published_cache(heights: RangeInclusive<u64>, payload: usize) -> (tempfile::TempDir, Cache) {
        let (dir, cache) = temp_cache();
        let blocks: Vec<CompactBlock> = heights.map(|height| block(height, payload)).collect();
        cache.add_batch(&blocks).unwrap();
        while export::store_next_epoch_digest(&cache, "main")
            .unwrap()
            .is_some()
        {}
        (dir, cache)
    }

    fn config(max_concurrent_downloads: usize, compression_level: i32) -> SnapshotServeConfig {
        SnapshotServeConfig {
            // Overridden by the ephemeral listener the test binds itself.
            bind: "127.0.0.1:0".parse().unwrap(),
            max_concurrent_downloads,
            compression_level,
        }
    }

    /// Start the endpoints on an ephemeral port and return where they landed.
    async fn start(cache: Cache, config: SnapshotServeConfig) -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(serve_on(
            listener,
            Arc::new(cache),
            "main".to_string(),
            config,
        ));
        address
    }

    /// A client that neither advertises nor transparently decompresses, so these tests see the wire
    /// bytes rather than what reqwest would quietly undo for them.
    ///
    /// Built through [`crate::node::http_client_builder`] like every other client in the crate:
    /// `reqwest::Client::builder()` panics outright when no rustls provider is installed yet, which
    /// makes a test that reaches it first fail on test-ordering rather than on what it asserts.
    fn client() -> reqwest::Client {
        crate::node::http_client_builder()
            .no_gzip()
            .no_zstd()
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn the_manifest_round_trips_over_http() {
        let (_dir, cache) = published_cache(1..=10_000, 0);
        let expected = export::manifest(&cache, "main").unwrap();
        let address = start(cache, config(4, 3)).await;

        let response = client()
            .get(format!("http://{address}/snapshot/manifest"))
            .send()
            .await
            .unwrap();

        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            "no-cache"
        );
        assert_eq!(response.json::<Manifest>().await.unwrap(), expected);
    }

    #[tokio::test]
    async fn an_epoch_body_matches_the_direct_export() {
        let (_dir, cache) = published_cache(1..=10_000, 0);
        let mut exported = Vec::new();
        export::write_epoch(&cache, "main", PUBLISHED_EPOCH, &mut exported).unwrap();
        let address = start(cache, config(4, 3)).await;

        let response = client()
            .get(format!("http://{address}/snapshot/epoch/{PUBLISHED_EPOCH}"))
            .send()
            .await
            .unwrap();

        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            IMMUTABLE
        );
        assert_eq!(
            response.headers().get(header::CONTENT_LENGTH).unwrap(),
            &exported.len().to_string()
        );
        let body = response.bytes().await.unwrap();
        assert_eq!(body.as_ref(), exported.as_slice());
        assert!(parse_epoch(&body).is_ok());
    }

    #[tokio::test]
    async fn an_epoch_that_is_not_published_is_not_found() {
        let (_dir, cache) = published_cache(1..=10_000, 0);
        let address = start(cache, config(4, 3)).await;

        let response = client()
            .get(format!("http://{address}/snapshot/epoch/7"))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn a_download_beyond_the_cap_is_refused_rather_than_queued() {
        // A body far larger than any socket buffer, so the first download is provably still in
        // flight (its export blocked on a full channel) while the second request arrives.
        let (_dir, cache) = published_cache(1..=10_000, 1024);
        let address = start(cache, config(1, 0)).await;

        // Headers have arrived but the body is left unread, so the slot stays taken.
        let holding = client()
            .get(format!("http://{address}/snapshot/epoch/{PUBLISHED_EPOCH}"))
            .send()
            .await
            .unwrap();
        assert_eq!(holding.status(), reqwest::StatusCode::OK);

        let refused = client()
            .get(format!("http://{address}/snapshot/epoch/{PUBLISHED_EPOCH}"))
            .send()
            .await
            .unwrap();

        assert_eq!(refused.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);
        drop(holding);
    }

    #[tokio::test]
    async fn a_client_that_asks_for_zstd_gets_a_body_that_decompresses_to_the_same_bytes() {
        let (_dir, cache) = published_cache(1..=10_000, 0);
        let mut exported = Vec::new();
        export::write_epoch(&cache, "main", PUBLISHED_EPOCH, &mut exported).unwrap();
        let address = start(cache, config(4, 3)).await;

        let response = client()
            .get(format!("http://{address}/snapshot/epoch/{PUBLISHED_EPOCH}"))
            .header(header::ACCEPT_ENCODING, "zstd")
            .send()
            .await
            .unwrap();

        assert_eq!(
            response.headers().get(header::CONTENT_ENCODING).unwrap(),
            "zstd"
        );
        assert_eq!(
            response.headers().get(header::VARY).unwrap(),
            header::ACCEPT_ENCODING.as_str()
        );
        let compressed = response.bytes().await.unwrap();
        let decompressed = zstd::decode_all(compressed.as_ref()).unwrap();

        assert_eq!(decompressed, exported);
        // The digest the manifest publishes is over the uncompressed body, so it holds either way.
        assert!(compressed.len() < exported.len());
        assert_eq!(content_digest(&decompressed), content_digest(&exported));
    }

    #[tokio::test]
    async fn a_client_that_does_not_ask_for_compression_gets_the_uncompressed_body() {
        let (_dir, cache) = published_cache(1..=10_000, 0);
        let mut exported = Vec::new();
        export::write_epoch(&cache, "main", PUBLISHED_EPOCH, &mut exported).unwrap();
        let address = start(cache, config(4, 3)).await;

        let response = client()
            .get(format!("http://{address}/snapshot/epoch/{PUBLISHED_EPOCH}"))
            .send()
            .await
            .unwrap();

        assert_eq!(response.headers().get(header::CONTENT_ENCODING), None);
        // Present on both answers: a cache in front must never hand a stored identity body to a
        // client that asked for zstd, or the reverse.
        assert_eq!(
            response.headers().get(header::VARY).unwrap(),
            header::ACCEPT_ENCODING.as_str()
        );
        assert_eq!(
            response.bytes().await.unwrap().as_ref(),
            exported.as_slice()
        );
    }

    #[tokio::test]
    async fn a_second_instance_bootstraps_from_a_published_snapshot_over_http() {
        // The whole path in one test: publish, download over the wire with compression negotiated,
        // verify against the node, land in a second cache.
        let (_published_dir, published) = published_cache(1..=10_000, 0);
        let blocks: Vec<CompactBlock> = (1..=9_999).map(|height| block(height, 0)).collect();
        let node: Arc<dyn crate::node::NodeRpc> = Arc::new(crate::testutil::FakeNode {
            blockchain_info: Some(
                serde_json::from_value(serde_json::json!({
                    "chain": "main",
                    "blocks": 10_000,
                    "bestblockhash": "00",
                    "consensus": { "chaintip": "00000000" },
                }))
                .unwrap(),
            ),
            hash_by_height: blocks
                .iter()
                .map(|block| {
                    (
                        block.height,
                        crate::encoding::wire_to_display_hex(&block.hash),
                    )
                })
                .collect(),
            ..Default::default()
        });
        let address = start(published, config(4, 3)).await;
        let (_dir, bootstrapped) = temp_cache();

        let source =
            crate::snapshot::import::HttpEpochSource::new(&format!("http://{address}")).unwrap();
        let reached = crate::snapshot::import::import(
            &source,
            &bootstrapped,
            &node,
            &crate::snapshot::import::ImportConfig::default(),
        )
        .await
        .unwrap();

        assert_eq!(reached, Some(9_999));
        for block in &blocks {
            assert_eq!(
                bootstrapped.get(block.height).unwrap().as_ref(),
                Some(block)
            );
        }
        assert!(bootstrapped.validate_light().is_ok());
    }

    #[tokio::test]
    async fn compression_level_zero_serves_the_body_uncompressed_even_when_asked() {
        let (_dir, cache) = published_cache(1..=10_000, 0);
        let address = start(cache, config(4, 0)).await;

        let response = client()
            .get(format!("http://{address}/snapshot/epoch/{PUBLISHED_EPOCH}"))
            .header(header::ACCEPT_ENCODING, "zstd")
            .send()
            .await
            .unwrap();

        assert_eq!(response.headers().get(header::CONTENT_ENCODING), None);
    }
}
