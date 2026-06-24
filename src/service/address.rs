//! Transparent-address methods: `GetTaddressTxids`/`GetTaddressTransactions`, `GetTaddressBalance`
//! (and its streaming variant), and `GetAddressUtxos` (and its streaming variant).

use async_stream::try_stream;
use tonic::{Request, Response, Status};

use crate::encoding;
use crate::proto::{
    Address, AddressList, Balance, BoxStream, GetAddressUtxosArg, GetAddressUtxosReply,
    GetAddressUtxosReplyList, RawTransaction, TransparentAddressBlockFilter,
};

use super::{Streamer, decode_hex, mined_height};

/// Max addresses a single `GetTaddressBalanceStream` request may submit before the
/// server rejects it, bounding the per-request accumulation.
const MAX_STREAMED_ADDRESSES: usize = 10_000;

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
    Ok(Response::new(taddress_transactions(
        streamer,
        request.into_inner(),
    )?))
}

pub(super) async fn get_taddress_transactions(
    streamer: &Streamer,
    request: Request<TransparentAddressBlockFilter>,
) -> Result<Response<BoxStream<RawTransaction>>, Status> {
    Ok(Response::new(taddress_transactions(
        streamer,
        request.into_inner(),
    )?))
}

pub(super) async fn get_taddress_balance(
    streamer: &Streamer,
    request: Request<AddressList>,
) -> Result<Response<Balance>, Status> {
    let address_list = request.into_inner();
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

pub(super) async fn get_taddress_balance_stream(
    streamer: &Streamer,
    request: Request<tonic::Streaming<Address>>,
) -> Result<Response<Balance>, Status> {
    let mut incoming = request.into_inner();
    let mut addresses = Vec::new();
    while let Some(address) = incoming.message().await? {
        push_bounded(&mut addresses, address.address)?;
    }
    for address in &addresses {
        check_taddress(address)?;
    }
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
pub(super) async fn collect_utxos(
    streamer: &Streamer,
    arg: &GetAddressUtxosArg,
) -> Result<Vec<GetAddressUtxosReply>, Status> {
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
/// range. Shared by `GetTaddressTxids` (a deprecated alias) and `GetTaddressTransactions`.
fn taddress_transactions(
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
    let end = range.end.map(|block| block.height).unwrap_or(0);
    let address = filter.address;
    let node = streamer.node.clone();
    Ok(Box::pin(try_stream! {
        let addresses = [address];
        let txids = node
            .get_address_txids(&addresses, start, end)
            .await
            .map_err(super::errors::address_query_to_status)?;
        for txid in txids {
            let raw = node
                .get_raw_transaction(&txid)
                .await
                .map_err(super::errors::transaction_lookup_to_status)?;
            let data = decode_hex(&raw.hex, "transaction hex")?;
            yield RawTransaction { data, height: mined_height(raw.height) };
        }
    }))
}

#[cfg(test)]
mod tests {
    use tonic::Code;

    use super::{MAX_STREAMED_ADDRESSES, push_bounded};

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
}
