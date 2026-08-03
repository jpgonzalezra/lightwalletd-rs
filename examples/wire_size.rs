//! How many bytes a `GetBlockRange` response puts on the wire, how many fixed-size packets that
//! costs a transport billing in them, and whether compressing compact blocks buys anything.
//!
//! Usage: `cargo run --example wire_size -- <cache.redb> [--from H] [--to H] [--dump PATH]`
//!
//! Opens the cache read-write, so point it at a copy, never at a cache a server has open.
//!
//! Blocks are compressed as they are read rather than accumulated, so the only cost that grows with
//! the range is one size per block. A full mainnet cache is tens of gigabytes and would not fit
//! otherwise.

use std::io::Write;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use lightwalletd_rs::cache::Cache;

/// Payload of one mixnet packet, which is billed whole however full it is.
const PACKET_PAYLOAD: usize = 2048;

/// gRPC length-prefixed framing per message: a compression flag plus a four-byte length.
const GRPC_FRAMING: usize = 5;

/// Range widths a syncing wallet actually asks for, used to translate bytes into packet counts.
const RANGE_WIDTHS: [usize; 4] = [100, 1_000, 10_000, 100_000];

#[derive(Parser)]
#[command(about = "Measure what a GetBlockRange response costs on the wire")]
struct Arguments {
    /// Cache to measure. Point it at a copy: it is opened read-write.
    cache: PathBuf,

    /// First height to include. Defaults to the start of the cache.
    #[arg(long)]
    from: Option<u64>,

    /// Last height to include. Defaults to the end of the cache. Bounding the range is how one
    /// historical era is measured on its own.
    #[arg(long)]
    to: Option<u64>,

    /// Write the concatenated payload here, streamed rather than held in memory.
    #[arg(long)]
    dump: Option<PathBuf>,
}

/// Counts what a writer is given and keeps none of it, so the whole-stream compression figure
/// costs no memory.
#[derive(Default)]
struct ByteCounter(usize);

impl Write for ByteCounter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.0 += buffer.len();
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn main() -> Result<()> {
    let arguments = Arguments::parse();

    let cache = Cache::open(&arguments.cache).context("opening the cache")?;
    let (cached_first, cached_last) = cache
        .range()
        .context("reading the cached range")?
        .context("the cache is empty")?;
    let first = arguments.from.unwrap_or(cached_first).max(cached_first);
    let last = arguments.to.unwrap_or(cached_last).min(cached_last);
    anyhow::ensure!(
        first <= last,
        "empty range: {first} to {last}, cache holds {cached_first} to {cached_last}"
    );

    let mut block_sizes = Vec::new();
    let mut per_message_total = 0usize;
    let mut stream_compressor =
        zstd::Encoder::new(ByteCounter::default(), 3).context("starting the stream compressor")?;
    let mut dump = arguments
        .dump
        .as_ref()
        .map(std::fs::File::create)
        .transpose()
        .context("creating the payload dump")?;

    cache.for_each_raw::<anyhow::Error>(first..=last, |_height, bytes| {
        block_sizes.push(bytes.len());
        per_message_total += zstd::encode_all(bytes, 3)
            .context("zstd per message")?
            .len();
        stream_compressor
            .write_all(bytes)
            .context("zstd over the stream")?;
        if let Some(file) = &mut dump {
            file.write_all(bytes).context("writing the payload dump")?;
        }
        Ok(())
    })?;

    let stream_total = stream_compressor
        .finish()
        .context("finishing the stream compressor")?
        .0;

    let block_count = block_sizes.len();
    anyhow::ensure!(block_count > 0, "no blocks in {first} to {last}");
    let payload_total: usize = block_sizes.iter().sum();
    let wire_total = payload_total + block_count * GRPC_FRAMING;

    let mut sorted = block_sizes;
    sorted.sort_unstable();

    println!("# Wire size of compact blocks\n");
    println!(
        "Cache `{}`, heights {first} to {last}, {block_count} blocks.\n",
        arguments.cache.display()
    );

    println!("## Serialized bytes per block\n");
    println!("| metric | bytes |");
    println!("|---|---|");
    println!("| min | {} |", sorted.first().copied().unwrap_or(0));
    println!("| p50 | {} |", percentile(&sorted, 0.50));
    println!("| mean | {} |", payload_total / block_count);
    println!("| p90 | {} |", percentile(&sorted, 0.90));
    println!("| p99 | {} |", percentile(&sorted, 0.99));
    println!("| max | {} |", sorted.last().copied().unwrap_or(0));
    println!(
        "| mean on the wire (+{GRPC_FRAMING} gRPC framing) | {} |\n",
        wire_total / block_count
    );

    println!("## Fixed-size packets\n");
    println!(
        "A mixnet exposes a byte stream, so packets fill contiguously and there is no alignment to"
    );
    println!("tune. What matters is the count, since every reply packet consumes one reply block");
    println!("against a default budget of 10.\n");
    let mean_wire = wire_total as f64 / block_count as f64;
    println!("| range width | wire bytes | packets |");
    println!("|---|---|---|");
    for width in RANGE_WIDTHS {
        let bytes = (mean_wire * width as f64) as usize;
        println!(
            "| {} blocks | {} | {} |",
            width,
            human_bytes(bytes),
            bytes.div_ceil(PACKET_PAYLOAD)
        );
    }
    println!();

    println!("## Compression\n");
    println!("| scheme | bytes | ratio |");
    println!("|---|---|---|");
    println!("| uncompressed | {} | 1.00x |", human_bytes(payload_total));
    println!(
        "| zstd-3 per message (what gRPC compression does) | {} | {:.2}x |",
        human_bytes(per_message_total),
        payload_total as f64 / per_message_total.max(1) as f64
    );
    println!(
        "| zstd-3 over the whole stream (what a transport could do) | {} | {:.2}x |",
        human_bytes(stream_total),
        payload_total as f64 / stream_total.max(1) as f64
    );

    if let Some(path) = &arguments.dump {
        println!("\nPayload dumped to `{}`.", path.display());
    }

    Ok(())
}

fn percentile(sorted: &[usize], fraction: f64) -> usize {
    if sorted.is_empty() {
        return 0;
    }
    let index = (((sorted.len() - 1) as f64) * fraction).round() as usize;
    sorted[index]
}

fn human_bytes(bytes: usize) -> String {
    const UNITS: [&str; 4] = ["B", "KiB", "MiB", "GiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}
