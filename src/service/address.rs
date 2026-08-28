//! Transparent-address methods: `GetTaddressTxids`/`GetTaddressTransactions`, `GetTaddressBalance`
//! (and its streaming variant), and `GetAddressUtxos` (and its streaming variant).

use std::time::Duration;

use async_stream::try_stream;
use tokio::time::{Instant, timeout_at};
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};

use crate::encoding;
use crate::proto::{
    Address, AddressList, Balance, BoxStream, GetAddressUtxosArg, GetAddressUtxosReply,
    GetAddressUtxosReplyList, RawTransaction, TransparentAddressBlockFilter,
};

use super::{Streamer, decode_hex, framing, mined_height};

/// Max addresses a single transparent-address request may carry before the server rejects it,
/// bounding the per-request accumulation across `GetTaddressBalance`, its streaming variant, and
/// `GetAddressUtxos`.
pub(super) const MAX_STREAMED_ADDRESSES: usize = 10_000;

/// Max matching txids a single `GetTaddressTransactions`/`GetTaddressTxids` request may have before
/// the server rejects it, bounding the per-txid node fetches one request can trigger.
pub(super) const MAX_TADDRESS_TXIDS: usize = 10_000;

/// Max unspent outputs a single `GetAddressUtxos`/`GetAddressUtxosStream` request may return before
/// the server rejects it, bounding the result one request keeps resident.
///
/// `maxEntries` cannot do this job: the protocol reads zero as unlimited and that is what
/// light-client SDKs send, so without a server-side cap the reply count is set by how many outputs
/// the named addresses hold. Results are ordered by height, so a client past the cap pages with
/// `startHeight`/`maxEntries` (ADR 0038).
///
/// The value has to clear [`MAX_INDEXED_OUTPUTS_PER_BLOCK`]. `startHeight` selects a block and not
/// an output, so a height whose outputs do not fit in one reply could never be read in full.
pub(super) const MAX_ADDRESS_UTXOS: usize = 100_000;

/// Upper bound on the transparent outputs one block can add to the address index, and so on the
/// largest group of unspent outputs that can share a single height.
///
/// Consensus caps a block at 2,000,000 bytes, and the smallest output the index can hold is 32
/// bytes: 8 for the value, 1 for the script length, 23 for a P2SH script. This ignores the header,
/// the transaction overhead and the inputs every transaction carries, so the real limit is lower.
pub(super) const MAX_INDEXED_OUTPUTS_PER_BLOCK: usize = 2_000_000 / 32;

const _: () = assert!(
    MAX_ADDRESS_UTXOS > MAX_INDEXED_OUTPUTS_PER_BLOCK,
    "the reply cap has to clear one block's worth of outputs: a height that does not fit in one \
     reply can never be read, and paging by startHeight stalls on it"
);

/// Max height span a single `GetTaddressTransactions`/`GetTaddressTxids` request may scan.
/// Deliberately generous (beyond a full-history scan of the current chain), so it never rejects a
/// legitimate wallet request, while still rejecting an `end` near `u64::MAX`.
pub(super) const MAX_TADDRESS_BLOCK_SPAN: u64 = 10_000_000;

/// Overall deadline for the node work one `GetTaddressTransactions`/`GetTaddressTxids` request can
/// trigger: the address-index scan plus the per-txid fetches it fans out into. Without it, an
/// abandoned scan keeps a node connection pinned for as long as the node keeps working on it.
const TADDRESS_SCAN_DEADLINE: Duration = Duration::from_secs(30);

/// Append `address` to `addresses`, rejecting once it would exceed [`MAX_STREAMED_ADDRESSES`]
/// so a single client stream cannot accumulate without bound.
fn push_bounded(addresses: &mut Vec<String>, address: String) -> Result<(), Status> {
    if addresses.len() >= MAX_STREAMED_ADDRESSES {
        return Err(Status::resource_exhausted(
            "get_taddress_balance_stream: too many addresses submitted",
        ));
    }
    addresses.push(address);
    Ok(())
}

/// Validate that `address` has the transparent-address shape: a `t` followed by exactly 34
/// alphanumeric characters (equivalent to the `\At[a-zA-Z0-9]{34}\z` check upstream).
fn check_taddress(address: &str) -> Result<(), Status> {
    let bytes = address.as_bytes();
    let well_formed =
        bytes.len() == 35 && bytes[0] == b't' && bytes[1..].iter().all(u8::is_ascii_alphanumeric);
    if !well_formed {
        return Err(Status::invalid_argument(format!(
            "transparent address {address} contains invalid characters"
        )));
    }
    Ok(())
}

pub(super) async fn get_taddress_txids(
    streamer: &Streamer,
    request: Request<TransparentAddressBlockFilter>,
) -> Result<Response<BoxStream<RawTransaction>>, Status> {
    Ok(Response::new(
        taddress_transactions(streamer, request.into_inner()).await?,
    ))
}

pub(super) async fn get_taddress_transactions(
    streamer: &Streamer,
    request: Request<TransparentAddressBlockFilter>,
) -> Result<Response<BoxStream<RawTransaction>>, Status> {
    Ok(Response::new(
        taddress_transactions(streamer, request.into_inner()).await?,
    ))
}

pub(super) async fn get_taddress_balance(
    streamer: &Streamer,
    request: Request<AddressList>,
) -> Result<Response<Balance>, Status> {
    let address_list = request.into_inner();
    if address_list.addresses.len() > MAX_STREAMED_ADDRESSES {
        return Err(Status::resource_exhausted(
            "get_taddress_balance: too many addresses submitted",
        ));
    }
    for address in &address_list.addresses {
        check_taddress(address)?;
    }
    let balance = streamer
        .node
        .get_address_balance(&address_list.addresses)
        .await
        .map_err(super::errors::address_query_to_status)?;
    Ok(Response::new(Balance {
        value_zat: balance.balance,
    }))
}

/// Accumulate the addresses a client streams in, validating and bounding each one as it arrives.
/// Rejecting mid-stream means a malformed or over-long stream is refused at the offending message
/// instead of after the whole stream has been received.
async fn collect_streamed_addresses(
    incoming: impl Stream<Item = Result<Address, Status>>,
) -> Result<Vec<String>, Status> {
    let mut incoming = std::pin::pin!(incoming);
    let mut addresses = Vec::new();
    while let Some(address) = incoming.next().await {
        let address = address?.address;
        check_taddress(&address)?;
        push_bounded(&mut addresses, address)?;
    }
    Ok(addresses)
}

pub(super) async fn get_taddress_balance_stream(
    streamer: &Streamer,
    request: Request<tonic::Streaming<Address>>,
) -> Result<Response<Balance>, Status> {
    let addresses = collect_streamed_addresses(request.into_inner()).await?;
    let balance = streamer
        .node
        .get_address_balance(&addresses)
        .await
        .map_err(super::errors::address_query_to_status)?;
    Ok(Response::new(Balance {
        value_zat: balance.balance,
    }))
}

pub(super) async fn get_address_utxos(
    streamer: &Streamer,
    request: Request<GetAddressUtxosArg>,
) -> Result<Response<GetAddressUtxosReplyList>, Status> {
    let address_utxos = collect_utxos(streamer, &request.into_inner()).await?;
    Ok(Response::new(GetAddressUtxosReplyList { address_utxos }))
}

pub(super) async fn get_address_utxos_stream(
    streamer: &Streamer,
    request: Request<GetAddressUtxosArg>,
) -> Result<Response<BoxStream<GetAddressUtxosReply>>, Status> {
    let replies = collect_utxos(streamer, &request.into_inner()).await?;
    let stream = tokio_stream::iter(replies.into_iter().map(Ok));
    Ok(Response::new(Box::pin(stream)))
}

/// Fetch the UTXOs for the requested addresses, apply the `startHeight`/`maxEntries` filters, and
/// convert them into the gRPC reply shape.
///
/// The address count is capped before the node call: `getaddressutxos` cannot push down
/// `startHeight`/`maxEntries`, so the whole backend result is materialized before those filters
/// apply, and an uncapped address list would turn one request into unbounded backend work.
///
/// The reply count is capped at [`MAX_ADDRESS_UTXOS`] as the replies are built. Both the unary and
/// the streaming method go through here, so one check covers both.
pub(super) async fn collect_utxos(
    streamer: &Streamer,
    arg: &GetAddressUtxosArg,
) -> Result<Vec<GetAddressUtxosReply>, Status> {
    if arg.addresses.len() > MAX_STREAMED_ADDRESSES {
        return Err(Status::resource_exhausted(format!(
            "get_address_utxos: too many addresses submitted (limit {MAX_STREAMED_ADDRESSES})"
        )));
    }
    for address in &arg.addresses {
        check_taddress(address)?;
    }
    let utxos = streamer
        .node
        .get_address_utxos(&arg.addresses)
        .await
        .map_err(super::errors::address_query_to_status)?;
    let mut replies = Vec::new();
    for utxo in utxos {
        if utxo.height < arg.start_height {
            continue;
        }
        if arg.max_entries > 0 && replies.len() as u32 >= arg.max_entries {
            break;
        }
        // After the `maxEntries` break, so a request that states its own bound within the cap is
        // always served, however many outputs the addresses hold.
        if replies.len() >= MAX_ADDRESS_UTXOS {
            return Err(Status::resource_exhausted(format!(
                "get_address_utxos: more than {MAX_ADDRESS_UTXOS} unspent outputs match; raise startHeight or set maxEntries to read them in pages"
            )));
        }
        let txid = encoding::display_hex_to_wire(&utxo.txid)
            .map_err(|e| Status::internal(format!("decoding utxo txid: {e}")))?;
        let script = decode_hex(&utxo.script, "utxo script")?;
        replies.push(GetAddressUtxosReply {
            address: utxo.address,
            txid,
            index: utxo.output_index as i32,
            script,
            value_zat: utxo.satoshis as i64,
            height: utxo.height,
        });
    }
    Ok(replies)
}

/// Stream one full `RawTransaction` per txid that touches the filter's address within its block
/// range. Shared by `GetTaddressTxids` (a deprecated alias) and `GetTaddressTransactions`. The
/// matching txids are fetched up front so a request whose range matches more than
/// [`MAX_TADDRESS_TXIDS`] transactions is rejected before any per-txid fetch hits the node.
///
/// The range itself is bounded first: an open-ended request is pinned to the chain tip
/// ([`resolve_range_end`]), a span wider than [`MAX_TADDRESS_BLOCK_SPAN`] is rejected, and
/// [`TADDRESS_SCAN_DEADLINE`] caps the node work the whole request can trigger.
async fn taddress_transactions(
    streamer: &Streamer,
    filter: TransparentAddressBlockFilter,
) -> Result<BoxStream<RawTransaction>, Status> {
    check_taddress(&filter.address)?;
    let range = filter.range.ok_or_else(|| {
        Status::invalid_argument("get_taddress_transactions: must specify block range")
    })?;
    let start = range
        .start
        .ok_or_else(|| {
            Status::invalid_argument("get_taddress_transactions: must specify a start block height")
        })?
        .height;
    let node = streamer.node.clone();
    let deadline = Instant::now() + TADDRESS_SCAN_DEADLINE;

    let end = resolve_range_end(streamer, range.end.map(|block| block.height), start).await?;

    let addresses = [filter.address];
    let txids = with_deadline(deadline, node.get_address_txids(&addresses, start, end))
        .await?
        .map_err(super::errors::address_query_to_status)?;
    if txids.len() > MAX_TADDRESS_TXIDS {
        return Err(Status::resource_exhausted(format!(
            "get_taddress_transactions: more than {MAX_TADDRESS_TXIDS} matching transactions; narrow the block range"
        )));
    }

    // One node fetch per txid, so the transactions leave in batches rather than one undersized
    // DATA frame each (ADR 0037).
    Ok(Box::pin(framing::coalesce(try_stream! {
        for txid in txids {
            let raw = with_deadline(deadline, node.get_raw_transaction(&txid))
                .await?
                .map_err(super::errors::transaction_lookup_to_status)?;
            let data = decode_hex(&raw.hex, "transaction hex")?;
            yield RawTransaction { data, height: mined_height(raw.height) };
        }
    })))
}

/// Resolve the upper bound of a `GetTaddressTransactions` range, rejecting an over-wide span.
///
/// `end` is optional in the protocol, and a zero `end` means unset rather than height zero (the
/// backends read it that way too). Either way it is pinned to the chain tip at request time, so the
/// address-index scan always runs to a concrete height instead of open-endedly to a tip that keeps
/// growing. An explicit `end` is taken as given, but only within [`MAX_TADDRESS_BLOCK_SPAN`].
async fn resolve_range_end(
    streamer: &Streamer,
    requested_end: Option<u64>,
    start: u64,
) -> Result<u64, Status> {
    let end = match requested_end {
        Some(end) if end > 0 => end,
        _ => streamer.node.get_blockchain_info().await?.blocks,
    };
    if end > start && end - start > MAX_TADDRESS_BLOCK_SPAN {
        return Err(Status::invalid_argument(format!(
            "get_taddress_transactions: block range too wide ({} blocks, limit {MAX_TADDRESS_BLOCK_SPAN})",
            end - start
        )));
    }
    Ok(end)
}

/// Run a node call under the request's overall `deadline`, mapping expiry to `DeadlineExceeded`.
async fn with_deadline<T, E>(
    deadline: Instant,
    call: impl Future<Output = Result<T, E>>,
) -> Result<Result<T, E>, Status> {
    timeout_at(deadline, call).await.map_err(|_| {
        Status::deadline_exceeded("get_taddress_transactions: timed out waiting for the node")
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio_stream::StreamExt;
    use tonic::Code;

    use super::{Address, MAX_STREAMED_ADDRESSES, collect_streamed_addresses, push_bounded};
    use crate::testutil::example_taddress;

    /// A client stream over `addresses` that counts how many messages the server pulled from it.
    fn counted_stream(
        addresses: Vec<String>,
    ) -> (
        impl tokio_stream::Stream<Item = Result<Address, tonic::Status>>,
        Arc<AtomicUsize>,
    ) {
        let received = Arc::new(AtomicUsize::new(0));
        let counter = received.clone();
        let stream = tokio_stream::iter(addresses).map(move |address| {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(Address { address })
        });
        (stream, received)
    }

    #[test]
    fn push_bounded_accepts_up_to_the_cap() {
        let mut addresses = Vec::new();
        for _ in 0..MAX_STREAMED_ADDRESSES {
            push_bounded(&mut addresses, "t".to_string()).unwrap();
        }
        assert_eq!(addresses.len(), MAX_STREAMED_ADDRESSES);
    }

    #[test]
    fn push_bounded_rejects_over_the_cap() {
        let mut addresses = vec!["t".to_string(); MAX_STREAMED_ADDRESSES];
        let status = push_bounded(&mut addresses, "t".to_string()).unwrap_err();
        assert_eq!(status.code(), Code::ResourceExhausted);
    }

    #[tokio::test]
    async fn collect_streamed_addresses_accumulates_a_well_formed_stream() {
        let (stream, _received) = counted_stream(vec![example_taddress(); 3]);

        let addresses = collect_streamed_addresses(stream).await.unwrap();

        assert_eq!(addresses, vec![example_taddress(); 3]);
    }

    #[tokio::test]
    async fn collect_streamed_addresses_rejects_an_invalid_address_without_draining_the_stream() {
        let (stream, received) = counted_stream(vec![
            example_taddress(),
            "not_a_real_address".to_string(),
            example_taddress(),
        ]);

        let status = collect_streamed_addresses(stream).await.unwrap_err();

        assert_eq!(status.code(), Code::InvalidArgument);
        assert_eq!(received.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn collect_streamed_addresses_rejects_over_the_cap_without_draining_the_stream() {
        let (stream, received) =
            counted_stream(vec![example_taddress(); MAX_STREAMED_ADDRESSES + 2]);

        let status = collect_streamed_addresses(stream).await.unwrap_err();

        assert_eq!(status.code(), Code::ResourceExhausted);
        assert_eq!(received.load(Ordering::SeqCst), MAX_STREAMED_ADDRESSES + 1);
    }
}
