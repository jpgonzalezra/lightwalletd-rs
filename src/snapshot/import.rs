//! Consuming a snapshot: fetch epochs from a source, verify them, and land them in the cache.
//!
//! Four layers run per epoch, in cost order, and every one of them is on:
//!
//! 1. **Content digest.** Proves the bytes arrived intact. Proves nothing about honesty: the
//!    manifest that declared the digest came from the same server as the body.
//! 2. **Chain linkage.** Proves the epoch is internally self-consistent, and that it joins onto what
//!    the cache already holds. On its own it says nothing about the real chain.
//! 3. **Tree sizes.** The cumulative note-commitment tree sizes in `chainMetadata` must grow by
//!    exactly the outputs and actions the later block carries. Costs no RPC and prevents adding,
//!    dropping or reordering commitments across the whole range.
//! 4. **Anchor.** Every block's hash is compared against the operator's own node. This is the layer
//!    that matters, because a compact block's `hash` is a field the publisher chooses rather than
//!    something derivable from the block's contents.
//!
//! Layer 4 is deliberately dense. Checking only the tip would be vacuous: a publisher would pin the
//! real tip hash in the last block, choose that block's `prevHash` freely, and everything below it
//! would be unconstrained while still passing layers 1 to 3.
//!
//! Imports go through the cache's ordinary append, one transaction per epoch, so every existing
//! invariant applies unchanged and a rejected epoch writes nothing. Resumption then needs no state
//! of its own: the importer asks the cache how far it got and carries on from there.
//!
//! An epoch body is held whole to be verified, but its blocks are decoded one at a time rather than
//! materialized together: on mainnet a sandblasting-era epoch is 1.21 GB, so keeping both would
//! double the peak for nothing.

use std::collections::BTreeMap;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use prost::Message;
use tokio::task::JoinSet;
use tokio_stream::StreamExt;

use crate::cache::{Cache, CacheError};
use crate::encoding;
use crate::node::NodeRpc;
use crate::proto::CompactBlock;

use super::SnapshotError;
use super::format::{
    AnchorHasher, EPOCH_SIZE, EpochEntry, FORMAT_VERSION, Manifest, content_digest,
    epoch_first_height, epoch_last_height, parse_epoch,
};

/// Where epoch bodies come from. Implemented over HTTP by the client, and by a fixture source in
/// tests.
#[async_trait]
pub trait EpochSource: Send + Sync {
    /// The publisher's manifest.
    async fn manifest(&self) -> Result<Manifest, SnapshotError>;
    /// One epoch body, uncompressed, refusing anything longer than `max_bytes`.
    ///
    /// The bound is passed in rather than read from the source's own manifest so it comes from the
    /// same validated entry the digest check uses.
    async fn epoch(&self, index: u64, max_bytes: u64) -> Result<Vec<u8>, SnapshotError>;
}

/// Tuning for a snapshot import.
#[derive(Debug, Clone, Copy)]
pub struct ImportConfig {
    /// Concurrent block-hash lookups while recomputing an epoch's anchor. An import's cost is
    /// dominated by these, not by the transfer.
    pub concurrency: usize,
}

impl Default for ImportConfig {
    fn default() -> Self {
        Self { concurrency: 8 }
    }
}

/// Import from `source` into `cache`, verifying every epoch against `node`.
///
/// Resumes from the cache's own tip, so an interrupted import continues where it stopped and a
/// partial import leaves a cache the ordinary ingestor can extend. Returns how far the cache reaches
/// afterwards.
pub async fn import(
    source: &dyn EpochSource,
    cache: &Cache,
    node: &Arc<dyn NodeRpc>,
    config: &ImportConfig,
) -> Result<Option<u64>, SnapshotError> {
    let manifest = source.manifest().await?;
    let chain = node.get_blockchain_info().await?.chain;
    validate_manifest(&manifest, &chain)?;

    let started_from = cache.latest_height()?;
    let mut tip = started_from;
    for entry in &manifest.epochs {
        let next = match tip {
            Some(height) => height + 1,
            // An empty cache takes the snapshot's own base as its floor.
            None => entry.start,
        };
        if entry.end < next {
            continue; // already held, from an earlier run or from native ingestion
        }
        if entry.start > next {
            return Err(SnapshotError::Gap {
                cache_tip: tip,
                snapshot_base: entry.start,
            });
        }

        // The body is the only large allocation an epoch costs: it is verified in place, and the
        // blocks are decoded one at a time on the way into the cache.
        let body = fetch_body(source, entry).await?;
        let records = verify_epoch(&body, entry, &manifest.chain, node, config).await?;
        let arriving = &records[(next - entry.start) as usize..];
        check_junction(cache, entry.index, arriving.first().copied())?;
        cache.add_decoded_batch(
            arriving
                .iter()
                .map(|record| CompactBlock::decode(*record).map_err(CacheError::from)),
            tip.is_none().then_some(next),
        )?;
        tip = Some(entry.end);
        tracing::info!(
            epoch = entry.index,
            from = next,
            to = entry.end,
            "imported snapshot epoch"
        );
    }

    match (started_from, tip) {
        (from, Some(to)) if from != tip => tracing::info!(
            from = from.map(|height| height + 1).unwrap_or(0),
            to,
            "snapshot import finished"
        ),
        _ => tracing::info!("snapshot import had nothing to add"),
    }
    Ok(tip)
}

/// Reject a manifest that cannot describe blocks this instance could use, before any body is
/// fetched.
fn validate_manifest(manifest: &Manifest, chain: &str) -> Result<(), SnapshotError> {
    if manifest.format_version != FORMAT_VERSION {
        return Err(SnapshotError::UnsupportedVersion {
            expected: FORMAT_VERSION,
            found: manifest.format_version,
        });
    }
    if manifest.epoch_size != EPOCH_SIZE {
        return Err(SnapshotError::EpochSizeMismatch {
            expected: EPOCH_SIZE,
            found: manifest.epoch_size,
        });
    }
    if manifest.chain != chain {
        return Err(SnapshotError::ChainMismatch {
            expected: chain.to_string(),
            found: manifest.chain.clone(),
        });
    }

    let malformed = |detail: String| SnapshotError::MalformedManifest(detail);
    let mut previous: Option<&EpochEntry> = None;
    for entry in &manifest.epochs {
        if entry.start > entry.end || entry.end != epoch_last_height(entry.index) {
            return Err(malformed(format!(
                "epoch {} declares [{}, {}], which is not the range of that index",
                entry.index, entry.start, entry.end
            )));
        }
        match previous {
            // Only the lowest published epoch may begin mid-epoch, where the publisher's cache
            // starts; it must then be the base the manifest advertises.
            None => {
                if entry.start < epoch_first_height(entry.index)
                    || manifest.base_height != entry.start
                {
                    return Err(malformed(format!(
                        "first epoch starts at {} but the manifest advertises base {}",
                        entry.start, manifest.base_height
                    )));
                }
            }
            Some(previous) => {
                if entry.index != previous.index + 1
                    || entry.start != epoch_first_height(entry.index)
                {
                    return Err(malformed(format!(
                        "epoch {} does not continue epoch {}",
                        entry.index, previous.index
                    )));
                }
            }
        }
        previous = Some(entry);
    }
    Ok(())
}

/// Download one epoch body, refusing anything that cannot be what the manifest describes.
async fn fetch_body(
    source: &dyn EpochSource,
    entry: &EpochEntry,
) -> Result<Vec<u8>, SnapshotError> {
    let reject = |detail: String| SnapshotError::EpochRejected {
        epoch: entry.index,
        check: "length",
        detail,
    };
    if entry.bytes > MAX_EPOCH_BYTES {
        return Err(reject(format!(
            "manifest declares {} bytes, past the {MAX_EPOCH_BYTES} an epoch may hold",
            entry.bytes
        )));
    }
    let body = source.epoch(entry.index, entry.bytes).await?;
    if body.len() as u64 != entry.bytes {
        return Err(reject(format!(
            "body is {} bytes, manifest declares {}",
            body.len(),
            entry.bytes
        )));
    }
    Ok(body)
}

/// Run every verification layer over `body`, returning its records in height order.
///
/// Blocks are decoded one at a time and dropped again: only the body, the 32-byte hashes the anchor
/// needs, and the block being compared are ever held. A sandblasting-era epoch is over a gigabyte,
/// so materializing all 10,000 of them would double the peak for no reason.
async fn verify_epoch<'a>(
    body: &'a [u8],
    entry: &EpochEntry,
    chain: &str,
    node: &Arc<dyn NodeRpc>,
    config: &ImportConfig,
) -> Result<Vec<&'a [u8]>, SnapshotError> {
    let epoch = entry.index;
    let reject = |check: &'static str, detail: String| SnapshotError::EpochRejected {
        epoch,
        check,
        detail,
    };

    // 1. Content digest.
    let digest = hex::encode(content_digest(body));
    if digest != entry.content_digest {
        return Err(reject(
            "content digest",
            format!(
                "computed {digest}, manifest declares {}",
                entry.content_digest
            ),
        ));
    }

    let (header, records) =
        parse_epoch(body).map_err(|error| reject("framing", error.to_string()))?;
    if header.chain != chain {
        return Err(reject(
            "chain",
            format!("body is for chain {:?}, expected {chain:?}", header.chain),
        ));
    }
    let count = entry.end - entry.start + 1;
    if header.start != entry.start || u64::from(header.count) != count {
        return Err(reject(
            "framing",
            format!(
                "body covers {} blocks from {}, manifest declares {count} from {}",
                header.count, header.start, entry.start
            ),
        ));
    }

    // One pass for layers 2 and 3, keeping only what layer 4 will need.
    let mut hashes: Vec<Vec<u8>> = Vec::with_capacity(records.len());
    let mut previous: Option<CompactBlock> = None;
    for (height, record) in (entry.start..).zip(&records) {
        let block = decode_block(epoch, height, record)?;
        if let Some(previous) = &previous {
            check_linkage(epoch, previous, &block)?;
            check_tree_sizes(epoch, previous, &block)?;
        }
        hashes.push(block.hash.clone());
        previous = Some(block);
    }

    // 4. The one that binds the epoch to the real chain.
    verify_anchor(entry, &hashes, node, config).await?;
    Ok(records)
}

/// Decode one record, holding it to the height its position in the epoch implies.
fn decode_block(epoch: u64, height: u64, record: &[u8]) -> Result<CompactBlock, SnapshotError> {
    let block = CompactBlock::decode(record).map_err(|error| SnapshotError::BlockRejected {
        epoch,
        height,
        check: "decode",
        detail: error.to_string(),
    })?;
    if block.height != height {
        return Err(SnapshotError::BlockRejected {
            epoch,
            height,
            check: "height",
            detail: format!("block claims height {}", block.height),
        });
    }
    Ok(block)
}

/// Check that the first arriving block continues the cache, in both hashes and tree sizes.
///
/// The seam between two epochs is exactly where a snapshot could otherwise splice one chain onto
/// another, so the checks that run inside an epoch have to run across the join too.
fn check_junction(cache: &Cache, epoch: u64, arriving: Option<&[u8]>) -> Result<(), SnapshotError> {
    let Some(record) = arriving else {
        return Ok(());
    };
    let Some(tip_height) = cache.latest_height()? else {
        return Ok(()); // nothing to join onto
    };
    let Some(tip) = cache.get(tip_height)? else {
        return Ok(());
    };
    let first = decode_block(epoch, tip_height + 1, record)?;
    check_linkage(epoch, &tip, &first)?;
    check_tree_sizes(epoch, &tip, &first)
}

/// `later` must name `earlier` as its predecessor.
fn check_linkage(
    epoch: u64,
    earlier: &CompactBlock,
    later: &CompactBlock,
) -> Result<(), SnapshotError> {
    if later.prev_hash != earlier.hash {
        return Err(SnapshotError::BlockRejected {
            epoch,
            height: later.height,
            check: "linkage",
            detail: format!(
                "prevHash {} does not match the hash of block {}, which is {}",
                encoding::wire_to_display_hex(&later.prev_hash),
                earlier.height,
                encoding::wire_to_display_hex(&earlier.hash),
            ),
        });
    }
    Ok(())
}

/// Each pool's cumulative tree size must grow by exactly what `later` adds to it.
///
/// Free to check and hard to fake: it pins the number and placement of commitments across the whole
/// range without a single RPC. It does not pin their values, which is what the anchor and, in a
/// future revision, subtree roots are for.
fn check_tree_sizes(
    epoch: u64,
    earlier: &CompactBlock,
    later: &CompactBlock,
) -> Result<(), SnapshotError> {
    let reject = |detail: String| SnapshotError::BlockRejected {
        epoch,
        height: later.height,
        check: "tree size",
        detail,
    };
    let (Some(before), Some(after)) = (
        earlier.chain_metadata.as_ref(),
        later.chain_metadata.as_ref(),
    ) else {
        return Err(reject("block carries no chainMetadata".to_string()));
    };

    let sapling = later
        .vtx
        .iter()
        .map(|tx| tx.outputs.len() as u64)
        .sum::<u64>();
    let orchard = later
        .vtx
        .iter()
        .map(|tx| tx.actions.len() as u64)
        .sum::<u64>();
    let ironwood = later
        .vtx
        .iter()
        .map(|tx| tx.ironwood_actions.len() as u64)
        .sum::<u64>();

    for (pool, before, after, carried) in [
        (
            "sapling",
            before.sapling_commitment_tree_size,
            after.sapling_commitment_tree_size,
            sapling,
        ),
        (
            "orchard",
            before.orchard_commitment_tree_size,
            after.orchard_commitment_tree_size,
            orchard,
        ),
        (
            "ironwood",
            before.ironwood_commitment_tree_size,
            after.ironwood_commitment_tree_size,
            ironwood,
        ),
    ] {
        let grew = u64::from(after)
            .checked_sub(u64::from(before))
            .ok_or_else(|| reject(format!("{pool} tree shrank from {before} to {after}")))?;
        if grew != carried {
            return Err(reject(format!(
                "{pool} tree grew by {grew} but the block carries {carried}"
            )));
        }
    }
    Ok(())
}

/// Recompute the epoch's anchor from the operator's own node and compare it, height by height.
///
/// The per-height comparison is what localizes a failure; recomputing the digest on top of it is
/// what ties the manifest's published anchor to the body that arrived.
async fn verify_anchor(
    entry: &EpochEntry,
    hashes: &[Vec<u8>],
    node: &Arc<dyn NodeRpc>,
    config: &ImportConfig,
) -> Result<(), SnapshotError> {
    let node_hashes = fetch_block_hashes(
        node,
        entry.index,
        entry.start..=entry.end,
        config.concurrency,
    )
    .await?;
    let mut anchor = AnchorHasher::default();
    for (height, claimed) in (entry.start..).zip(hashes) {
        let reject = |check: &'static str, detail: String| SnapshotError::BlockRejected {
            epoch: entry.index,
            height,
            check,
            detail,
        };
        let display = node_hashes
            .get(&height)
            .ok_or_else(|| reject("anchor", "the node has no block at this height".to_string()))?;
        let wire = encoding::display_hex_to_wire(display)
            .map_err(|error| reject("anchor", format!("node returned {display:?}: {error}")))?;
        if wire != *claimed {
            return Err(reject(
                "anchor",
                format!(
                    "the node has {display}, the snapshot claims {}",
                    encoding::wire_to_display_hex(claimed)
                ),
            ));
        }
        anchor.update(height, &wire);
    }

    let recomputed = hex::encode(anchor.finish());
    if recomputed != entry.anchor {
        return Err(SnapshotError::EpochRejected {
            epoch: entry.index,
            check: "anchor",
            detail: format!(
                "recomputed {recomputed}, manifest declares {}",
                entry.anchor
            ),
        });
    }
    Ok(())
}

/// Largest epoch body this build will hold. Measured on mainnet: 49 MB for an early-2016 epoch,
/// 1.21 GB for an early sandblasting one, which is the era that sets the bar. The ceiling leaves
/// room above that while still stopping a hostile manifest from asking for an allocation instead of
/// a download.
const MAX_EPOCH_BYTES: u64 = 4 * 1024 * 1024 * 1024;

/// Idle timeout between reads from the snapshot peer, which is what detects a stalled transfer.
///
/// Deliberately not a total deadline: an epoch body runs to [`MAX_EPOCH_BYTES`], so any fixed total
/// would silently impose a floor on the link speed a bootstrap needs. At 300 seconds the 1.21 GB
/// sandblasting-era epochs, which are most of a mainnet snapshot and the reason this feature exists,
/// would need over 4 MB/s sustained or fail every retry identically.
const READ_TIMEOUT: Duration = Duration::from_secs(60);
/// TCP connect timeout for the snapshot peer.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Attempts per epoch before giving up.
const FETCH_ATTEMPTS: u32 = 3;
/// First backoff between attempts; doubles from there.
const RETRY_BACKOFF: Duration = Duration::from_secs(2);

/// An [`EpochSource`] over a peer's HTTP endpoints.
pub struct HttpEpochSource {
    client: reqwest::Client,
    base_url: String,
    attempts: u32,
}

impl HttpEpochSource {
    /// Point a source at a peer, e.g. `https://peer.example:9069`.
    pub fn new(url: &str) -> Result<Self, SnapshotError> {
        Ok(Self {
            client: crate::node::http_client_builder()
                .read_timeout(READ_TIMEOUT)
                .connect_timeout(CONNECT_TIMEOUT)
                .build()
                .map_err(SnapshotError::Http)?,
            base_url: url.trim_end_matches('/').to_string(),
            attempts: FETCH_ATTEMPTS,
        })
    }

    /// Fetch one epoch, refusing to hold more than `max_bytes`.
    ///
    /// The cap is on the decompressed stream, which is what `bytes_stream` yields: capping the wire
    /// bytes instead would leave a few compressed megabytes free to expand into many gigabytes.
    async fn fetch_epoch(&self, index: u64, max_bytes: u64) -> Result<Vec<u8>, SnapshotError> {
        let response = self
            .client
            .get(format!("{}/snapshot/epoch/{index}", self.base_url))
            .send()
            .await
            .map_err(SnapshotError::Http)?
            .error_for_status()
            .map_err(SnapshotError::Http)?;

        let mut body: Vec<u8> = Vec::new();
        let mut stream = Box::pin(response.bytes_stream());
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(SnapshotError::Http)?;
            if body.len() as u64 + chunk.len() as u64 > max_bytes {
                return Err(SnapshotError::EpochRejected {
                    epoch: index,
                    check: "length",
                    detail: format!("body ran past the declared {max_bytes} bytes"),
                });
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }
}

#[async_trait]
impl EpochSource for HttpEpochSource {
    async fn manifest(&self) -> Result<Manifest, SnapshotError> {
        Ok(self
            .client
            .get(format!("{}/snapshot/manifest", self.base_url))
            .send()
            .await
            .map_err(SnapshotError::Http)?
            .error_for_status()
            .map_err(SnapshotError::Http)?
            .json()
            .await
            .map_err(SnapshotError::Http)?)
    }

    async fn epoch(&self, index: u64, max_bytes: u64) -> Result<Vec<u8>, SnapshotError> {
        // An epoch is idempotent and verified afterwards, so retrying one is always safe.
        let mut delay = RETRY_BACKOFF;
        let mut attempt = 1;
        loop {
            match self.fetch_epoch(index, max_bytes).await {
                Ok(body) => return Ok(body),
                Err(error) if attempt >= self.attempts => return Err(error),
                Err(error) => {
                    tracing::warn!(%error, epoch = index, attempt, "fetching a snapshot epoch failed; retrying");
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                    attempt += 1;
                }
            }
        }
    }
}

/// Heights per batched lookup.
///
/// Measured against a real node. Co-located, which is the shape a deployment actually has, at the
/// default concurrency of 8: 27.7k heights/s at 100 per batch, 29.6k at 250, 20.7k at 1,000. Larger
/// batches lose because 10,000 heights then split into too few chunks to spread across the workers.
/// Over a high-latency link the ordering reverses, since round trips dominate and bigger batches
/// amortize them, but even there a few hundred per batch keeps the check comfortably affordable.
const LOOKUP_BATCH: usize = 250;

/// Look up every height's block hash, in batches, with at most `concurrency` requests in flight.
///
/// A fixed pool of workers pulling batches off a shared counter. Batch size matters more than
/// concurrency here, since what dominates is round trips rather than the node's own work.
async fn fetch_block_hashes(
    node: &Arc<dyn NodeRpc>,
    epoch: u64,
    heights: RangeInclusive<u64>,
    concurrency: usize,
) -> Result<BTreeMap<u64, String>, SnapshotError> {
    let batches: Arc<Vec<Vec<u64>>> = Arc::new(
        heights
            .collect::<Vec<u64>>()
            .chunks(LOOKUP_BATCH)
            .map(<[u64]>::to_vec)
            .collect(),
    );
    let next = Arc::new(AtomicUsize::new(0));
    let failed = Arc::new(AtomicBool::new(false));

    let mut workers = JoinSet::new();
    for _ in 0..concurrency.max(1) {
        let node = Arc::clone(node);
        let batches = Arc::clone(&batches);
        let next = Arc::clone(&next);
        let failed = Arc::clone(&failed);
        workers.spawn(async move {
            let mut found = Vec::new();
            while !failed.load(Ordering::Relaxed) {
                let Some(batch) = batches.get(next.fetch_add(1, Ordering::Relaxed)) else {
                    break;
                };
                let height = batch[0];
                match node.get_block_hashes(batch).await {
                    Ok(hashes) if hashes.len() == batch.len() => {
                        found.extend(batch.iter().copied().zip(hashes));
                    }
                    Ok(hashes) => {
                        failed.store(true, Ordering::Relaxed);
                        return Err(SnapshotError::EpochRejected {
                            epoch,
                            check: "anchor",
                            detail: format!(
                                "asked the node for {} hashes from height {height}, got {}",
                                batch.len(),
                                hashes.len()
                            ),
                        });
                    }
                    Err(source) => {
                        // Stop the other workers rather than walking the rest of the epoch against
                        // a node that is not answering.
                        failed.store(true, Ordering::Relaxed);
                        return Err(SnapshotError::NodeLookup { height, source });
                    }
                }
            }
            Ok(found)
        });
    }

    let mut hashes = BTreeMap::new();
    while let Some(joined) = workers.join_next().await {
        hashes.extend(joined??);
    }
    Ok(hashes)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::node::GetBlockchainInfo;
    use crate::proto::{ChainMetadata, CompactSaplingOutput, CompactTx};
    use crate::snapshot::format::{EpochDigest, anchor_digest, write_header, write_record};
    use crate::testutil::{FakeNode, temp_cache};

    /// Heights 9,990..=9,999: the tail of epoch 0, which is the smallest range the manifest rules
    /// accept as a complete epoch (a published epoch always ends on its boundary, and only the first
    /// one may start inside it).
    const FIRST: u64 = 9_990;
    const LAST: u64 = 9_999;

    fn block_hash(height: u64) -> Vec<u8> {
        let mut hash = vec![0xab; 32];
        hash[..8].copy_from_slice(&height.to_le_bytes());
        hash
    }

    fn block(height: u64) -> CompactBlock {
        CompactBlock {
            height,
            hash: block_hash(height),
            prev_hash: block_hash(height - 1),
            chain_metadata: Some(ChainMetadata::default()),
            ..Default::default()
        }
    }

    fn chain(heights: RangeInclusive<u64>) -> Vec<CompactBlock> {
        heights.map(block).collect()
    }

    /// Serialize `blocks` as an epoch body, exactly as the export would.
    fn body_of(blocks: &[CompactBlock]) -> Vec<u8> {
        let mut body = Vec::new();
        write_header(&mut body, "main", blocks[0].height, blocks.len() as u32).unwrap();
        for block in blocks {
            write_record(&mut body, &block.encode_to_vec()).unwrap();
        }
        body
    }

    /// The manifest entry describing `body`, with both digests computed over it.
    fn entry_of(index: u64, blocks: &[CompactBlock], body: &[u8]) -> EpochEntry {
        EpochDigest {
            start: blocks[0].height,
            end: blocks[blocks.len() - 1].height,
            bytes: body.len() as u64,
            content: content_digest(body),
            anchor: anchor_digest(
                blocks
                    .iter()
                    .map(|block| (block.height, block.hash.as_slice())),
            ),
        }
        .entry(index)
    }

    /// An in-memory [`EpochSource`] over epochs given as `(index, blocks)`.
    struct FixtureSource {
        manifest: Manifest,
        bodies: HashMap<u64, Vec<u8>>,
    }

    impl FixtureSource {
        fn new(epochs: Vec<(u64, Vec<CompactBlock>)>) -> Self {
            let mut bodies = HashMap::new();
            let mut entries = Vec::new();
            for (index, blocks) in &epochs {
                let body = body_of(blocks);
                entries.push(entry_of(*index, blocks, &body));
                bodies.insert(*index, body);
            }
            let manifest = Manifest {
                format_version: FORMAT_VERSION,
                chain: "main".to_string(),
                epoch_size: EPOCH_SIZE,
                base_height: entries[0].start,
                tip_height: entries[entries.len() - 1].end,
                epochs: entries,
            };
            Self { manifest, bodies }
        }

        /// Replace one epoch's body without touching the manifest, so only the digest can catch it.
        fn corrupt_body(mut self, index: u64, mutate: impl FnOnce(&mut Vec<u8>)) -> Self {
            mutate(self.bodies.get_mut(&index).unwrap());
            self
        }

        /// Replace one epoch's blocks and republish its content digest and length, so the body is
        /// internally consistent and only a later layer can reject it.
        fn republish(mut self, index: u64, blocks: &[CompactBlock]) -> Self {
            let body = body_of(blocks);
            let entry = self
                .manifest
                .epochs
                .iter_mut()
                .find(|entry| entry.index == index)
                .unwrap();
            entry.bytes = body.len() as u64;
            entry.content_digest = hex::encode(content_digest(&body));
            self.bodies.insert(index, body);
            self
        }
    }

    #[async_trait]
    impl EpochSource for FixtureSource {
        async fn manifest(&self) -> Result<Manifest, SnapshotError> {
            Ok(self.manifest.clone())
        }

        async fn epoch(&self, index: u64, _max_bytes: u64) -> Result<Vec<u8>, SnapshotError> {
            self.bodies
                .get(&index)
                .cloned()
                .ok_or(SnapshotError::UnknownEpoch { index })
        }
    }

    /// A node that knows the true hash of every height in `blocks`.
    fn node_over(blocks: &[CompactBlock]) -> Arc<dyn NodeRpc> {
        let blockchain_info: GetBlockchainInfo = serde_json::from_value(serde_json::json!({
            "chain": "main",
            "blocks": blocks[blocks.len() - 1].height,
            "bestblockhash": "00",
            "consensus": { "chaintip": "00000000" },
        }))
        .unwrap();
        Arc::new(FakeNode {
            blockchain_info: Some(blockchain_info),
            hash_by_height: blocks
                .iter()
                .map(|block| (block.height, encoding::wire_to_display_hex(&block.hash)))
                .collect(),
            ..Default::default()
        })
    }

    async fn import_into(
        source: &FixtureSource,
        cache: &Cache,
        node: &Arc<dyn NodeRpc>,
    ) -> Result<Option<u64>, SnapshotError> {
        import(source, cache, node, &ImportConfig::default()).await
    }

    /// An [`EpochSource`] reading straight out of a published cache, so a test drives the real
    /// export rather than a second implementation of it.
    struct ExportedCache {
        cache: Cache,
    }

    #[async_trait]
    impl EpochSource for ExportedCache {
        async fn manifest(&self) -> Result<Manifest, SnapshotError> {
            crate::snapshot::export::manifest(&self.cache, "main")
        }

        async fn epoch(&self, index: u64, _max_bytes: u64) -> Result<Vec<u8>, SnapshotError> {
            let mut body = Vec::new();
            crate::snapshot::export::write_epoch(&self.cache, "main", index, &mut body)?;
            Ok(body)
        }
    }

    #[tokio::test]
    async fn an_exported_epoch_imports_back_block_for_block() {
        // The seam where the two halves meet: bodies and digests produced by the export, verified
        // and stored by the import, with no test-local reimplementation of either side in between.
        // Heights 1..=10,000, so epoch 0 is complete and published as the partial range [1, 9999].
        let blocks = chain(1..=EPOCH_SIZE);
        let (_published_dir, published) = temp_cache();
        published.add_batch(&blocks).unwrap();
        while crate::snapshot::export::store_next_epoch_digest(&published, "main")
            .unwrap()
            .is_some()
        {}
        let node = node_over(&blocks);
        let (_dir, cache) = temp_cache();

        let reached = import(
            &ExportedCache { cache: published },
            &cache,
            &node,
            &ImportConfig::default(),
        )
        .await
        .unwrap();

        assert_eq!(reached, Some(EPOCH_SIZE - 1));
        for block in &blocks[..(EPOCH_SIZE - 1) as usize] {
            assert_eq!(cache.get(block.height).unwrap().as_ref(), Some(block));
        }
        // The epoch holding the publisher's tip is not published, so it does not arrive.
        assert_eq!(cache.get(EPOCH_SIZE).unwrap(), None);
        assert!(cache.validate_light().is_ok());
    }

    /// Serve `body` for every epoch request, with the given `Content-Encoding`, from an ephemeral
    /// port. Stands in for a peer that does not play by the rules.
    async fn hostile_server(body: Vec<u8>, encoding: Option<&'static str>) -> String {
        use axum::http::header;
        use axum::response::IntoResponse;

        let app = axum::Router::new().route(
            "/snapshot/epoch/{index}",
            axum::routing::get(move || {
                let body = body.clone();
                async move {
                    let mut response = body.into_response();
                    if let Some(encoding) = encoding {
                        response.headers_mut().insert(
                            header::CONTENT_ENCODING,
                            axum::http::HeaderValue::from_static(encoding),
                        );
                    }
                    response
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await });
        format!("http://{address}")
    }

    #[tokio::test]
    async fn a_body_longer_than_the_manifest_declared_is_rejected() {
        let url = hostile_server(vec![0u8; 4096], None).await;
        let source = HttpEpochSource::new(&url).unwrap();

        let error = source.epoch(0, 1024).await.unwrap_err();

        assert!(matches!(
            error,
            SnapshotError::EpochRejected {
                check: "length",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn a_decompression_bomb_is_aborted_instead_of_buffered() {
        // 256 MB of zeros in ~8 KB on the wire. Capping the wire bytes would let all of it through;
        // the cap is on the decompressed stream, so this dies on the first chunk past the limit.
        let expanded = 256 * 1024 * 1024;
        let bomb = zstd::encode_all(vec![0u8; expanded].as_slice(), 3).unwrap();
        assert!(
            expanded / bomb.len() > 1_000,
            "expected a real expansion ratio"
        );
        let url = hostile_server(bomb, Some("zstd")).await;
        let source = HttpEpochSource::new(&url).unwrap();

        let error = source.epoch(0, 1024).await.unwrap_err();

        assert!(matches!(
            error,
            SnapshotError::EpochRejected {
                check: "length",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn a_peer_that_cannot_be_reached_is_an_error_rather_than_a_hang() {
        let source = HttpEpochSource::new("http://127.0.0.1:1").unwrap();

        assert!(source.manifest().await.is_err());
    }

    #[tokio::test]
    async fn a_verified_snapshot_lands_block_for_block() {
        let blocks = chain(FIRST..=LAST);
        let source = FixtureSource::new(vec![(0, blocks.clone())]);
        let node = node_over(&blocks);
        let (_dir, cache) = temp_cache();

        let reached = import_into(&source, &cache, &node).await.unwrap();

        assert_eq!(reached, Some(LAST));
        for block in &blocks {
            assert_eq!(cache.get(block.height).unwrap().as_ref(), Some(block));
        }
        assert!(cache.validate_light().is_ok());
    }

    #[tokio::test]
    async fn the_imported_base_height_is_recorded() {
        let blocks = chain(FIRST..=LAST);
        let source = FixtureSource::new(vec![(0, blocks.clone())]);
        let node = node_over(&blocks);
        let (_dir, cache) = temp_cache();

        import_into(&source, &cache, &node).await.unwrap();

        assert_eq!(cache.snapshot_base_height().unwrap(), Some(FIRST));
    }

    #[tokio::test]
    async fn a_corrupted_payload_byte_is_caught_by_the_content_digest() {
        let blocks = chain(FIRST..=LAST);
        let source = FixtureSource::new(vec![(0, blocks.clone())])
            .corrupt_body(0, |body| *body.last_mut().unwrap() ^= 0xff);
        let node = node_over(&blocks);
        let (_dir, cache) = temp_cache();

        let error = import_into(&source, &cache, &node).await.unwrap_err();

        assert!(matches!(
            error,
            SnapshotError::EpochRejected {
                check: "content digest",
                ..
            }
        ));
        assert_eq!(cache.latest_height().unwrap(), None);
    }

    #[tokio::test]
    async fn a_rewritten_prev_hash_is_caught_by_the_linkage_check() {
        let mut blocks = chain(FIRST..=LAST);
        blocks[5].prev_hash = vec![0xee; 32];
        let source = FixtureSource::new(vec![(0, chain(FIRST..=LAST))]).republish(0, &blocks);
        let node = node_over(&blocks);
        let (_dir, cache) = temp_cache();

        let error = import_into(&source, &cache, &node).await.unwrap_err();

        assert!(matches!(
            error,
            SnapshotError::BlockRejected {
                check: "linkage",
                height,
                ..
            } if height == FIRST + 5
        ));
    }

    #[tokio::test]
    async fn a_dropped_sapling_output_is_caught_by_the_tree_size_check() {
        // A block that adds one Sapling commitment, and the blocks after it carrying the bumped
        // cumulative size. Removing the output without adjusting the sizes is what the layer exists
        // to catch: the hashes still agree with the node, so the anchor would not notice.
        let mut blocks = chain(FIRST..=LAST);
        blocks[5].vtx = vec![CompactTx {
            outputs: vec![CompactSaplingOutput::default()],
            ..Default::default()
        }];
        for block in &mut blocks[5..] {
            block.chain_metadata = Some(ChainMetadata {
                sapling_commitment_tree_size: 1,
                ..Default::default()
            });
        }
        let honest = blocks.clone();
        blocks[5].vtx.clear();

        let source = FixtureSource::new(vec![(0, honest)]).republish(0, &blocks);
        let node = node_over(&blocks);
        let (_dir, cache) = temp_cache();

        let error = import_into(&source, &cache, &node).await.unwrap_err();

        assert!(matches!(
            error,
            SnapshotError::BlockRejected {
                check: "tree size",
                height,
                ..
            } if height == FIRST + 5
        ));
    }

    #[tokio::test]
    async fn a_block_hash_the_node_disagrees_with_is_caught_by_the_anchor() {
        // The last block, so the epoch's internal linkage still holds and only the anchor can reject
        // it.
        let honest = chain(FIRST..=LAST);
        let mut blocks = honest.clone();
        blocks[9].hash = vec![0xcc; 32];

        let source = FixtureSource::new(vec![(0, honest.clone())]).republish(0, &blocks);
        let node = node_over(&honest);
        let (_dir, cache) = temp_cache();

        let error = import_into(&source, &cache, &node).await.unwrap_err();

        assert!(matches!(
            error,
            SnapshotError::BlockRejected {
                check: "anchor",
                height,
                ..
            } if height == LAST
        ));
    }

    #[tokio::test]
    async fn a_forged_chain_carrying_the_true_tip_hash_is_rejected() {
        // The attack a tip-only anchor would let through: every block below the tip is fabricated,
        // the chain is internally consistent, the tree sizes add up, and the final block carries the
        // real tip hash. Only checking every height catches it.
        let honest = chain(FIRST..=LAST);
        let mut forged: Vec<CompactBlock> = (FIRST..=LAST)
            .map(|height| CompactBlock {
                height,
                hash: vec![height as u8; 32],
                prev_hash: vec![(height - 1) as u8; 32],
                chain_metadata: Some(ChainMetadata::default()),
                ..Default::default()
            })
            .collect();
        let last = forged.len() - 1;
        forged[last].hash = honest[last].hash.clone();

        let source = FixtureSource::new(vec![(0, honest.clone())]).republish(0, &forged);
        let node = node_over(&honest);
        let (_dir, cache) = temp_cache();

        let error = import_into(&source, &cache, &node).await.unwrap_err();

        assert!(matches!(
            error,
            SnapshotError::BlockRejected {
                check: "anchor",
                height: FIRST,
                ..
            }
        ));
        assert_eq!(cache.latest_height().unwrap(), None);
    }

    #[tokio::test]
    async fn a_rejected_epoch_writes_nothing_and_leaves_the_cache_consistent() {
        let honest = chain(FIRST..=LAST);
        let mut blocks = honest.clone();
        blocks[9].hash = vec![0xcc; 32];
        let source = FixtureSource::new(vec![(0, honest.clone())]).republish(0, &blocks);
        let node = node_over(&honest);
        let (_dir, cache) = temp_cache();
        cache.add_batch(&chain(FIRST..=FIRST + 2)).unwrap();

        assert!(import_into(&source, &cache, &node).await.is_err());

        assert_eq!(cache.latest_height().unwrap(), Some(FIRST + 2));
        assert!(cache.validate_light().is_ok());
    }

    #[tokio::test]
    async fn an_import_resumes_from_the_cached_tip() {
        // A cache already holding a prefix of the snapshot's range takes only the suffix, and the
        // junction between the two is checked like any other block boundary.
        let blocks = chain(FIRST..=LAST);
        let source = FixtureSource::new(vec![(0, blocks.clone())]);
        let node = node_over(&blocks);
        let (_dir, cache) = temp_cache();
        cache.add_batch(&blocks[..4]).unwrap();

        let reached = import_into(&source, &cache, &node).await.unwrap();

        assert_eq!(reached, Some(LAST));
        for block in &blocks {
            assert_eq!(cache.get(block.height).unwrap().as_ref(), Some(block));
        }
    }

    /// Two epochs: the tail of epoch 0 and the whole of epoch 1. The second one is full size
    /// because only the lowest published epoch may begin mid-epoch.
    fn two_epochs() -> (Vec<CompactBlock>, Vec<CompactBlock>) {
        (chain(FIRST..=LAST), chain(LAST + 1..=2 * EPOCH_SIZE - 1))
    }

    #[tokio::test]
    async fn an_interrupted_import_resumes_at_the_next_epoch() {
        let (first_epoch, second_epoch) = two_epochs();
        let all: Vec<CompactBlock> = first_epoch.iter().chain(&second_epoch).cloned().collect();
        let node = node_over(&all);

        // A source that cannot serve the second epoch: the first one still commits.
        let mut truncated =
            FixtureSource::new(vec![(0, first_epoch.clone()), (1, second_epoch.clone())]);
        truncated.bodies.remove(&1);
        let (_dir, interrupted) = temp_cache();
        assert!(import_into(&truncated, &interrupted, &node).await.is_err());
        assert_eq!(interrupted.latest_height().unwrap(), Some(LAST));

        // Re-run against a complete source: it picks up at the epoch boundary it reached.
        let complete = FixtureSource::new(vec![(0, first_epoch), (1, second_epoch)]);
        let reached = import_into(&complete, &interrupted, &node).await.unwrap();

        let (_uninterrupted_dir, uninterrupted) = temp_cache();
        import_into(&complete, &uninterrupted, &node).await.unwrap();

        assert_eq!(reached, Some(2 * EPOCH_SIZE - 1));
        assert_eq!(
            interrupted.latest_height().unwrap(),
            uninterrupted.latest_height().unwrap()
        );
        assert_eq!(
            interrupted.snapshot_base_height().unwrap(),
            uninterrupted.snapshot_base_height().unwrap()
        );
        for block in &all {
            assert_eq!(
                interrupted.get(block.height).unwrap(),
                uninterrupted.get(block.height).unwrap()
            );
        }
    }

    #[tokio::test]
    async fn a_second_epoch_that_does_not_join_the_first_is_rejected() {
        // The seam between epochs is where a snapshot could splice one chain onto another, so the
        // junction is checked like any block boundary inside an epoch.
        let (first_epoch, second_epoch) = two_epochs();
        let all: Vec<CompactBlock> = first_epoch.iter().chain(&second_epoch).cloned().collect();
        let node = node_over(&all);
        let mut spliced = second_epoch.clone();
        spliced[0].prev_hash = vec![0xee; 32];

        let source =
            FixtureSource::new(vec![(0, first_epoch), (1, second_epoch)]).republish(1, &spliced);
        let (_dir, cache) = temp_cache();

        let error = import_into(&source, &cache, &node).await.unwrap_err();

        assert!(matches!(
            error,
            SnapshotError::BlockRejected {
                epoch: 1,
                check: "linkage",
                height,
                ..
            } if height == EPOCH_SIZE
        ));
        assert_eq!(cache.latest_height().unwrap(), Some(LAST)); // the good epoch stands
    }

    #[tokio::test]
    async fn importing_twice_is_a_no_op_the_second_time() {
        let blocks = chain(FIRST..=LAST);
        let source = FixtureSource::new(vec![(0, blocks.clone())]);
        let node = node_over(&blocks);
        let (_dir, cache) = temp_cache();

        import_into(&source, &cache, &node).await.unwrap();
        let reached = import_into(&source, &cache, &node).await.unwrap();

        assert_eq!(reached, Some(LAST));
        assert_eq!(cache.latest_height().unwrap(), Some(LAST));
    }

    #[tokio::test]
    async fn a_cache_above_the_snapshot_range_is_left_untouched() {
        let blocks = chain(FIRST..=LAST);
        let source = FixtureSource::new(vec![(0, blocks.clone())]);
        let node = node_over(&blocks);
        let (_dir, cache) = temp_cache();
        cache.add_batch(&chain(LAST + 1..=LAST + 5)).unwrap();

        let reached = import_into(&source, &cache, &node).await.unwrap();

        assert_eq!(reached, Some(LAST + 5));
        assert_eq!(cache.get(FIRST).unwrap(), None); // no backfill, no truncation
    }

    #[tokio::test]
    async fn a_snapshot_that_would_leave_a_gap_is_rejected() {
        let blocks = chain(FIRST..=LAST);
        let source = FixtureSource::new(vec![(0, blocks.clone())]);
        let node = node_over(&blocks);
        let (_dir, cache) = temp_cache();
        cache.add_batch(&chain(FIRST - 20..=FIRST - 15)).unwrap();

        let error = import_into(&source, &cache, &node).await.unwrap_err();

        assert!(matches!(
            error,
            SnapshotError::Gap {
                snapshot_base: FIRST,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn a_snapshot_for_another_chain_is_rejected_before_any_epoch_is_fetched() {
        let blocks = chain(FIRST..=LAST);
        let mut source = FixtureSource::new(vec![(0, blocks.clone())]);
        source.manifest.chain = "test".to_string();
        source.bodies.clear(); // fetching any body would now panic on unwrap in the fixture
        let node = node_over(&blocks);
        let (_dir, cache) = temp_cache();

        let error = import_into(&source, &cache, &node).await.unwrap_err();

        assert!(matches!(error, SnapshotError::ChainMismatch { .. }));
    }

    #[tokio::test]
    async fn an_unknown_format_version_is_rejected() {
        let blocks = chain(FIRST..=LAST);
        let mut source = FixtureSource::new(vec![(0, blocks.clone())]);
        source.manifest.format_version = FORMAT_VERSION + 1;
        let node = node_over(&blocks);
        let (_dir, cache) = temp_cache();

        let error = import_into(&source, &cache, &node).await.unwrap_err();

        assert!(matches!(error, SnapshotError::UnsupportedVersion { .. }));
    }

    #[tokio::test]
    async fn a_different_epoch_size_is_rejected() {
        let blocks = chain(FIRST..=LAST);
        let mut source = FixtureSource::new(vec![(0, blocks.clone())]);
        source.manifest.epoch_size = EPOCH_SIZE * 2;
        let node = node_over(&blocks);
        let (_dir, cache) = temp_cache();

        let error = import_into(&source, &cache, &node).await.unwrap_err();

        assert!(matches!(error, SnapshotError::EpochSizeMismatch { .. }));
    }

    #[tokio::test]
    async fn an_epoch_whose_bounds_disagree_with_its_index_is_rejected() {
        let blocks = chain(FIRST..=LAST);
        let mut source = FixtureSource::new(vec![(0, blocks.clone())]);
        source.manifest.epochs[0].end = LAST - 1;
        let node = node_over(&blocks);
        let (_dir, cache) = temp_cache();

        let error = import_into(&source, &cache, &node).await.unwrap_err();

        assert!(matches!(error, SnapshotError::MalformedManifest(_)));
    }

    #[tokio::test]
    async fn a_manifest_advertising_a_base_it_does_not_serve_is_rejected() {
        let blocks = chain(FIRST..=LAST);
        let mut source = FixtureSource::new(vec![(0, blocks.clone())]);
        source.manifest.base_height = FIRST - 100;
        let node = node_over(&blocks);
        let (_dir, cache) = temp_cache();

        let error = import_into(&source, &cache, &node).await.unwrap_err();

        assert!(matches!(error, SnapshotError::MalformedManifest(_)));
    }
}
