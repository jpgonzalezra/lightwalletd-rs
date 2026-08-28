//! Minimal reproduction for the stream-establishment failures seen through the proxy rig.
//!
//! Two mixnet clients in one process: one accepts streams and echoes what it reads, the other opens
//! a stream per trial and waits for its bytes back. No gRPC, no HTTP/2, no proxies, no shared client
//! behind a mutex, so a failure here belongs to the SDK or the network and not to the rig.
//!
//! The echo side counts three stages separately, which is what turns a failure into a direction:
//!
//! - accepted but never read: the open arrived and the payload behind it did not (outbound loss).
//! - read but never written: the echo side failed writing its reply (return path, at the source).
//! - read and written, yet the dialler still timed out: the reply was lost in transit (return path).
//!
//! Those stages are global counters sampled around each trial, and the echo side advances them from
//! spawned tasks. A stage landing after a trial's deadline is therefore credited to the next trial:
//! the failure count is exact, the direction it is attributed to is not.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::Parser;
use nym_sdk::DebugConfig;
use nym_sdk::mixnet::{MixnetClient, MixnetClientBuilder};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Small enough to ride in a single packet, so payload size is never a variable.
const PAYLOAD: usize = 64;

#[derive(Parser)]
#[command(about = "Count stream failures with nothing but the SDK in the path")]
struct Arguments {
    #[arg(long, default_value_t = 200)]
    trials: usize,

    /// Reply-block budgets to compare, rotated one per trial. Interleaving them inside a single
    /// process is what keeps a drifting network from being mistaken for an effect of the budget.
    #[arg(long, value_delimiter = ',', default_value = "1,20,100,400")]
    budgets: Vec<u32>,

    /// How long one trial may wait for its echo before counting as a failure. Successful round
    /// trips measured 2 to 4 seconds, so this is ample.
    #[arg(long, default_value_t = 20)]
    timeout_secs: u64,

    /// `minimum_reply_surb_storage_threshold`, the reply blocks a receiver holds back before it will
    /// spend any on a reply. Unset leaves the SDK default of 10.
    #[arg(long)]
    surb_threshold: Option<usize>,

    /// Build a new dialling client for every trial, so no reply blocks carry over between them.
    ///
    /// The reserve threshold only binds while the receiver's store is small, and the store is keyed
    /// by sender tag, which a client keeps for the life of the process. Rotating the dialler is what
    /// makes each trial a first exchange; it costs one registration per trial.
    #[arg(long)]
    fresh_dialler: bool,
}

#[derive(Default)]
struct EchoStages {
    accepted: AtomicUsize,
    read: AtomicUsize,
    written: AtomicUsize,
    last_error: Mutex<Option<String>>,
}

impl EchoStages {
    fn snapshot(&self) -> (usize, usize, usize) {
        (
            self.accepted.load(Ordering::Relaxed),
            self.read.load(Ordering::Relaxed),
            self.written.load(Ordering::Relaxed),
        )
    }
}

/// A client with the SDK defaults, except for the reply-block reserve when one is asked for.
///
/// `connect_new` is this without the `debug_config` call, so an unset threshold reproduces it
/// exactly rather than merely closely.
async fn connect(surb_threshold: Option<usize>) -> Result<MixnetClient> {
    let mut debug = DebugConfig::default();
    if let Some(threshold) = surb_threshold {
        debug.reply_surbs.minimum_reply_surb_storage_threshold = threshold;
    }
    Ok(MixnetClientBuilder::new_ephemeral()
        .debug_config(debug)
        .build()?
        .connect_to_mixnet()
        .await?)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let arguments = Arguments::parse();
    let timeout = Duration::from_secs(arguments.timeout_secs);

    let mut echo_client = connect(arguments.surb_threshold)
        .await
        .context("connecting the echo client")?;
    let echo_address = *echo_client.nym_address();
    let mut listener = echo_client.listener().context("taking the listener")?;

    let stages = Arc::new(EchoStages::default());
    let stages_task = Arc::clone(&stages);

    tokio::spawn(async move {
        while let Some(mut stream) = listener.accept().await {
            stages_task.accepted.fetch_add(1, Ordering::Relaxed);
            let stages_inner = Arc::clone(&stages_task);
            tokio::spawn(async move {
                let mut buffer = vec![0u8; PAYLOAD];
                if let Err(error) = stream.read_exact(&mut buffer).await {
                    *stages_inner
                        .last_error
                        .lock()
                        .unwrap_or_else(|e| e.into_inner()) = Some(format!("read: {error}"));
                    return;
                }
                stages_inner.read.fetch_add(1, Ordering::Relaxed);

                if let Err(error) = stream.write_all(&buffer).await {
                    *stages_inner
                        .last_error
                        .lock()
                        .unwrap_or_else(|e| e.into_inner()) = Some(format!("write: {error}"));
                    return;
                }
                if let Err(error) = stream.flush().await {
                    *stages_inner
                        .last_error
                        .lock()
                        .unwrap_or_else(|e| e.into_inner()) = Some(format!("flush: {error}"));
                    return;
                }
                stages_inner.written.fetch_add(1, Ordering::Relaxed);
            });
        }
    });

    let mut dial_client = connect(arguments.surb_threshold)
        .await
        .context("connecting the dialling client")?;

    println!("echo   {echo_address}");
    println!("dial   {}", dial_client.nym_address());
    println!(
        "trials {} | budgets {:?} interleaved | timeout {}s\n",
        arguments.trials, arguments.budgets, arguments.timeout_secs
    );

    let payload = vec![0xA5u8; PAYLOAD];
    let mut per_budget: std::collections::BTreeMap<u32, (usize, usize, Vec<u128>)> = arguments
        .budgets
        .iter()
        .map(|b| (*b, (0, 0, Vec::new())))
        .collect();
    let mut ok = 0usize;
    let mut outbound_loss = 0usize;
    let mut echo_write_failed = 0usize;
    let mut return_loss = 0usize;
    let mut dial_errors = 0usize;
    let mut registration_retries = 0usize;
    let mut latencies = Vec::new();

    for trial in 1..=arguments.trials {
        let budget = arguments.budgets[(trial - 1) % arguments.budgets.len()];
        if arguments.fresh_dialler && trial > 1 {
            // Registering a fresh client is itself flaky: gateways time out during authentication
            // often enough to end a run on the second trial. Those failures are noise here, so they
            // are retried and counted rather than fatal, and the count is reported at the end.
            let mut attempt = 1;
            dial_client = loop {
                match connect(arguments.surb_threshold).await {
                    Ok(client) => break client,
                    Err(error) if attempt < 5 => {
                        registration_retries += 1;
                        println!("      registration attempt {attempt} failed: {error}");
                        attempt += 1;
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                    Err(error) => {
                        return Err(error).context("reconnecting the dialling client, five attempts");
                    }
                }
            };
        }
        let before = stages.snapshot();
        let started = Instant::now();

        let attempt = tokio::time::timeout(timeout, async {
            let mut stream = dial_client.open_stream(echo_address, Some(budget)).await?;
            stream.write_all(&payload).await?;
            stream.flush().await?;
            let mut back = vec![0u8; PAYLOAD];
            stream.read_exact(&mut back).await?;
            anyhow::Ok(back)
        })
        .await;

        let elapsed = started.elapsed().as_millis();
        let after = stages.snapshot();
        let accepted = after.0 > before.0;
        let read = after.1 > before.1;
        let written = after.2 > before.2;

        let entry = per_budget.entry(budget).or_insert((0, 0, Vec::new()));
        if matches!(&attempt, Ok(Ok(back)) if *back == payload) {
            entry.0 += 1;
            entry.2.push(elapsed);
        } else {
            entry.1 += 1;
        }

        match attempt {
            Ok(Ok(back)) if back == payload => {
                ok += 1;
                latencies.push(elapsed);
                println!("{trial:>4}  b{budget:<4} ok         {elapsed:>6} ms");
            }
            Ok(Ok(_)) => {
                dial_errors += 1;
                println!("{trial:>4}  b{budget:<4} CORRUPT    {elapsed:>6} ms");
            }
            Ok(Err(error)) => {
                dial_errors += 1;
                println!("{trial:>4}  b{budget:<4} ERROR      {elapsed:>6} ms  {error}");
            }
            Err(_) if accepted && !read => {
                outbound_loss += 1;
                println!("{trial:>4}  b{budget:<4} LOST-OUT   {elapsed:>6} ms");
            }
            Err(_) if read && !written => {
                echo_write_failed += 1;
                println!("{trial:>4}  b{budget:<4} ECHO-WRITE {elapsed:>6} ms");
            }
            Err(_) if written => {
                return_loss += 1;
                println!("{trial:>4}  b{budget:<4} LOST-BACK  {elapsed:>6} ms");
            }
            Err(_) => {
                outbound_loss += 1;
                println!("{trial:>4}  b{budget:<4} NO-ACCEPT  {elapsed:>6} ms");
            }
        }
    }

    latencies.sort_unstable();

    let failures = outbound_loss + echo_write_failed + return_loss + dial_errors;
    println!("\n--- summary ---");
    println!("trials                       {}", arguments.trials);
    if arguments.fresh_dialler {
        println!("registration retries         {registration_retries}");
    }
    println!("ok                           {ok}");
    println!("lost outbound                {outbound_loss}");
    println!("echo failed to write         {echo_write_failed}");
    println!("lost on the way back         {return_loss}");
    println!("dialler errors               {dial_errors}");
    println!(
        "failure rate                 {:.1}%  ({failures}/{})",
        100.0 * failures as f64 / arguments.trials.max(1) as f64,
        arguments.trials
    );
    println!(
        "ok latency   p50 {} ms | p90 {} ms | p99 {} ms",
        percentile(&latencies, 0.50),
        percentile(&latencies, 0.90),
        percentile(&latencies, 0.99)
    );
    println!("\n--- by reply-block budget (interleaved) ---");
    println!(
        "{:>6}  {:>8}  {:>3}  {:>6}  {:>6}  {:>9}",
        "budget", "trials", "ok", "fails", "rate", "p50"
    );
    for (budget, (good, bad, mut samples)) in per_budget {
        samples.sort_unstable();
        let p50 = percentile(&samples, 0.50);
        let total = good + bad;
        println!(
            "{budget:>6}  {total:>8}  {good:>3}  {bad:>6}  {:>5.1}%  {p50:>6} ms",
            100.0 * bad as f64 / total.max(1) as f64
        );
    }

    let stage_totals = stages.snapshot();
    println!(
        "echo stages: accepted {} | read {} | written {}",
        stage_totals.0, stage_totals.1, stage_totals.2
    );
    if let Some(error) = stages
        .last_error
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_ref()
    {
        println!("last echo error: {error}");
    }

    Ok(())
}

/// Nearest-rank percentile over an already sorted slice.
fn percentile(sorted: &[u128], fraction: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[(((sorted.len() - 1) as f64) * fraction).round() as usize]
}
