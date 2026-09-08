//! Subtree-roots method: `GetSubtreeRoots`.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use async_stream::try_stream;
use tokio::time::Instant;
use tonic::{Request, Response, Status};

use crate::cache::Cache;
use crate::encoding;
use crate::node::{NodeError, NodeRpc, Subtree};
use crate::proto::{BoxStream, GetSubtreeRootsArg, ShieldedProtocol, SubtreeRoot};

use super::{Streamer, decode_hex, framing, with_deadline};

/// Substring zebrad's `z_get_subtrees_by_index` puts in its JSON-RPC error message when asked for a
/// pool it doesn't recognize (`zebra-rpc` `methods.rs`: `"invalid pool name, must be one of: [...]"`).
/// A pre-NU6.3 node doesn't know the `"ironwood"` pool and answers with exactly this error (that's
/// not a server failure; the Ironwood subtree literally cannot exist yet on that node), so it is
/// matched here and turned into a clean empty stream instead of propagating as an RPC error. Matching
/// on the message (rather than the JSON-RPC error code, which zebrad reports as the generic `Misc`
/// code shared by many unrelated errors) keeps this narrowly scoped to the unrecognized-pool case.
const INVALID_POOL_NAME: &str = "invalid pool name";

/// Highest subtree index the backend node can represent. zebrad's `z_get_subtrees_by_index` takes a
/// `NoteCommitmentSubtreeIndex` (`zebra-chain` `subtree.rs`: a `u16`) for both the start index and
/// the limit, while the protocol declares each of them a `uint32`. Anything above this is rejected by
/// the node as `Invalid params`, which carries the generic `Misc` code and so would reach the client
/// as a retryable `Unavailable`, telling a wallet to back off and try again over input that can
/// never succeed. The range is therefore enforced here, before a round-trip that could only fail.
const MAX_SUBTREE_INDEX: u32 = u16::MAX as u32;

/// Overall deadline for the node work one `GetSubtreeRoots` request can trigger: the subtree query
/// plus the completing-block hash lookups it fans out into. Same value as the transparent-address
/// scan, which bounds the same shape of fan-out (ADR 0042).
const SUBTREE_SCAN_DEADLINE: Duration = Duration::from_secs(30);

/// Heights per batched `getblockhash` request when the cache does not hold a completing block.
///
/// One request covers the whole subtree set of any chain today, so the usual cold-cache cost is a
/// single round trip. The chunking keeps one request bounded once that stops being true. The
/// snapshot check batches smaller because it spreads its batches across a worker pool, where
/// splitting into too few chunks starves the workers. Here the calls are sequential and round trips
/// are the whole cost, so larger is better.
const HASH_LOOKUP_BATCH: usize = 1_000;

pub(super) async fn get_subtree_roots(
    streamer: &Streamer,
    request: Request<GetSubtreeRootsArg>,
) -> Result<Response<BoxStream<SubtreeRoot>>, Status> {
    let arg = request.into_inner();
    let protocol = match ShieldedProtocol::try_from(arg.shielded_protocol) {
        Ok(ShieldedProtocol::Sapling) => "sapling",
        Ok(ShieldedProtocol::Orchard) => "orchard",
        Ok(ShieldedProtocol::Ironwood) => "ironwood",
        Err(_) => return Err(Status::invalid_argument("unrecognized shielded protocol")),
    };
    if arg.start_index > MAX_SUBTREE_INDEX {
        return Err(Status::invalid_argument(format!(
            "start_index {} is above the highest subtree index the node can address ({})",
            arg.start_index, MAX_SUBTREE_INDEX
        )));
    }
    // Unlike the start index, a limit above the range is not an error: a limit is a ceiling, so one
    // past the range means "all of them". That request is already expressible as the unlimited `0`,
    // so it maps there. Clamping to `MAX_SUBTREE_INDEX` would instead cap the count one short of the
    // `MAX_SUBTREE_INDEX + 1` subtrees a full pool can hold (indexes `0..=MAX_SUBTREE_INDEX`).
    let max_entries = if arg.max_entries > MAX_SUBTREE_INDEX {
        0
    } else {
        arg.max_entries
    };

    // In darkside mode the roots are staged complete (with their completing block already set),
    // so they are served verbatim rather than computed from the cached blocks. Both backends are
    // bounded by the checks above, so they answer the same request identically.
    if let Some(state) = &streamer.darkside {
        let roots = state.lock().await.subtree_roots_for(
            arg.shielded_protocol,
            arg.start_index,
            max_entries,
        );
        let stream = tokio_stream::iter(roots.into_iter().map(Ok::<_, Status>));
        return Ok(Response::new(Box::pin(stream)));
    }

    let deadline = Instant::now() + SUBTREE_SCAN_DEADLINE;
    let subtrees = match with_deadline(
        deadline,
        "get_subtree_roots",
        streamer
            .node
            .get_subtrees(protocol, arg.start_index, max_entries),
    )
    .await?
    {
        Ok(subtrees) => subtrees,
        Err(NodeError::Rpc { ref message, .. }) if message.contains(INVALID_POOL_NAME) => {
            return Ok(Response::new(Box::pin(tokio_stream::empty())));
        }
        // A genuine node failure (transport, decode, or any other RPC error) still propagates.
        Err(other) => return Err(other.into()),
    };
    let mut subtrees = subtrees.subtrees;
    // Indexes run `start_index..=MAX_SUBTREE_INDEX`, so that is how many subtrees the node can
    // legitimately answer with. Anything past it is a subtree nobody could ask for again, and
    // dropping it keeps a broken or hostile node from setting how much work this request does.
    let addressable = usize::try_from(MAX_SUBTREE_INDEX - arg.start_index).unwrap_or(usize::MAX);
    subtrees.truncate(addressable.saturating_add(1));

    let completing_hashes =
        completing_block_hashes(&streamer.cache, streamer.node.as_ref(), &subtrees, deadline)
            .await?;

    // A root is under 100 bytes, so they leave in batches rather than one undersized DATA frame
    // each (ADR 0037).
    let stream = framing::coalesce(try_stream! {
        for (subtree, completing_block_hash) in subtrees.into_iter().zip(completing_hashes) {
            let root_hash = decode_hex(&subtree.root, "subtree root")?;
            yield SubtreeRoot {
                root_hash,
                completing_block_hash,
                completing_block_height: subtree.end_height,
            };
        }
    });
    Ok(Response::new(Box::pin(stream)))
}

/// The hash of the block that completed each subtree, in display order, one entry per subtree and
/// in the same order they came in.
///
/// The node's answer already carries the completing height, so what is missing is one hash per
/// height. Reading the block back for it would decode every transaction in a historical block to
/// keep 32 bytes, and on a cache miss would cost two node round trips and a full parse per subtree,
/// against a reply of about 100 bytes. The cache answers from its stored bytes without decoding the
/// transactions, and whatever it does not hold is resolved in batched height lookups (ADR 0042).
async fn completing_block_hashes(
    cache: &Cache,
    node: &dyn NodeRpc,
    subtrees: &[Subtree],
    deadline: Instant,
) -> Result<Vec<Vec<u8>>, Status> {
    let heights: Vec<u64> = subtrees.iter().map(|subtree| subtree.end_height).collect();
    let cached = cache.hashes_at(&heights)?;
    let missing: Vec<u64> = heights
        .iter()
        .copied()
        .filter(|height| !cached.contains_key(height))
        .collect::<BTreeSet<u64>>()
        .into_iter()
        .collect();

    let mut fetched: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
    for batch in missing.chunks(HASH_LOOKUP_BATCH) {
        let hashes = with_deadline(deadline, "get_subtree_roots", node.get_block_hashes(batch))
            .await?
            .map_err(Status::from)?;
        if hashes.len() != batch.len() {
            return Err(Status::internal(format!(
                "asked the node for {} block hashes, got {}",
                batch.len(),
                hashes.len()
            )));
        }
        for (height, hash) in batch.iter().zip(hashes) {
            fetched.insert(*height, decode_hex(&hash, "completing block hash")?);
        }
    }

    heights
        .iter()
        .map(|height| match cached.get(height) {
            // The cache holds the hash in protocol order. This field carries it in display order.
            Some(hash) => Ok(encoding::wire_to_display_bytes(hash)),
            // The node reports hashes in display order already.
            None => fetched.get(height).cloned().ok_or_else(|| {
                Status::internal(format!("no block hash resolved for height {height}"))
            }),
        })
        .collect()
}
