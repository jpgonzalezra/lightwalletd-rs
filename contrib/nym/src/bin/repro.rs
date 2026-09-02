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
//!
//! With `--wait-established` the dialler also waits for the SDK's establishment acknowledgement
//! before writing anything, and every trial is scored twice: whether the peer acknowledged, and
//! whether the echo came back. A layer that discards unacknowledged streams is only safe if those
//! two agree.

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
    /// `default` leaves the count to the SDK, which is the only way to exercise its own numbers.
    #[arg(long, value_delimiter = ',', default_value = "1,20,100,400")]
    budgets: Vec<String>,

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

    /// Wait for the peer's establishment acknowledgement before writing the payload.
    #[arg(long)]
    wait_established: bool,

    /// How long to wait for that acknowledgement. The SDK's own default is 15 seconds.
    #[arg(long, default_value_t = 15)]
    establish_timeout_secs: u64,

    /// Before the main run, dial this many streams at an address whose client registered with a
    /// live gateway and then disconnected. Nothing is listening, and nothing below the stream layer
    /// can tell: the gateway accepts and stores for an absent client either way.
    #[arg(long, default_value_t = 0)]
    dead_peer_trials: usize,
}

/// A reply-block budget for one trial, or the SDK's own choice.
type Budget = Option<u32>;

fn parse_budget(token: &str) -> Result<Budget> {
    match token.trim() {
        "default" | "d" => Ok(None),
        number => Ok(Some(number.parse().context("parsing a reply-block budget")?)),
    }
}

fn budget_label(budget: Budget) -> String {
    match budget {
        Some(count) => format!("b{count}"),
        None => "bdef".to_owned(),
    }
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
    let establish_timeout = Duration::from_secs(arguments.establish_timeout_secs);
    let budgets = arguments
        .budgets
        .iter()
        .map(|token| parse_budget(token))
        .collect::<Result<Vec<_>>>()?;

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
        "trials {} | budgets {:?} interleaved | timeout {}s",
        arguments.trials, arguments.budgets, arguments.timeout_secs
    );
    if arguments.wait_established {
        println!(
            "establishment ack awaited before each write | ack timeout {}s",
            arguments.establish_timeout_secs
        );
    }
    println!();

    if arguments.dead_peer_trials > 0 {
        dead_peer_arm(
            &mut dial_client,
            arguments.surb_threshold,
            arguments.dead_peer_trials,
            establish_timeout,
        )
        .await?;
    }

    let payload = vec![0xA5u8; PAYLOAD];
    let mut per_budget: std::collections::BTreeMap<String, (usize, usize, Vec<u128>)> = budgets
        .iter()
        .map(|budget| (budget_label(*budget), (0, 0, Vec::new())))
        .collect();
    let mut ok = 0usize;
    let mut outbound_loss = 0usize;
    let mut echo_write_failed = 0usize;
    let mut return_loss = 0usize;
    let mut dial_errors = 0usize;
    let mut registration_retries = 0usize;
    let mut latencies = Vec::new();
    let mut establish_latencies = Vec::new();
    // established/not against echoed/not, in that order.
    let mut establishment_matrix = [[0usize; 2]; 2];

    for trial in 1..=arguments.trials {
        let budget = budgets[(trial - 1) % budgets.len()];
        let label = budget_label(budget);
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
        let mut establishment: Option<std::result::Result<u128, String>> = None;

        let attempt = tokio::time::timeout(timeout, async {
            let mut stream = dial_client.open_stream(echo_address, budget).await?;
            if arguments.wait_established {
                let waited = Instant::now();
                establishment = Some(
                    match stream.wait_established_with_timeout(establish_timeout).await {
                        Ok(()) => Ok(waited.elapsed().as_millis()),
                        Err(error) => Err(error.to_string()),
                    },
                );
            }
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

        let echoed = matches!(&attempt, Ok(Ok(back)) if *back == payload);
        let entry = per_budget
            .entry(label.clone())
            .or_insert((0, 0, Vec::new()));
        if echoed {
            entry.0 += 1;
            entry.2.push(elapsed);
        } else {
            entry.1 += 1;
        }

        let mark = match &establishment {
            None => String::new(),
            Some(Ok(waited)) => {
                establish_latencies.push(*waited);
                establishment_matrix[0][usize::from(!echoed)] += 1;
                format!("  ack {waited:>5} ms")
            }
            Some(Err(_)) => {
                establishment_matrix[1][usize::from(!echoed)] += 1;
                "  NO-ACK".to_owned()
            }
        };

        match attempt {
            Ok(Ok(back)) if back == payload => {
                ok += 1;
                latencies.push(elapsed);
                println!("{trial:>4}  {label:<5} ok         {elapsed:>6} ms{mark}");
            }
            Ok(Ok(_)) => {
                dial_errors += 1;
                println!("{trial:>4}  {label:<5} CORRUPT    {elapsed:>6} ms{mark}");
            }
            Ok(Err(error)) => {
                dial_errors += 1;
                println!("{trial:>4}  {label:<5} ERROR      {elapsed:>6} ms{mark}  {error}");
            }
            Err(_) if accepted && !read => {
                outbound_loss += 1;
                println!("{trial:>4}  {label:<5} LOST-OUT   {elapsed:>6} ms{mark}");
            }
            Err(_) if read && !written => {
                echo_write_failed += 1;
                println!("{trial:>4}  {label:<5} ECHO-WRITE {elapsed:>6} ms{mark}");
            }
            Err(_) if written => {
                return_loss += 1;
                println!("{trial:>4}  {label:<5} LOST-BACK  {elapsed:>6} ms{mark}");
            }
            Err(_) => {
                outbound_loss += 1;
                println!("{trial:>4}  {label:<5} NO-ACCEPT  {elapsed:>6} ms{mark}");
            }
        }
    }

    latencies.sort_unstable();
    establish_latencies.sort_unstable();

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

    if arguments.wait_established {
        println!("\n--- establishment ack against outcome ---");
        println!("{:>17}  {:>8}  {:>11}", "", "echo ok", "echo failed");
        println!(
            "{:>17}  {:>8}  {:>11}",
            "acknowledged", establishment_matrix[0][0], establishment_matrix[0][1]
        );
        println!(
            "{:>17}  {:>8}  {:>11}",
            "no ack", establishment_matrix[1][0], establishment_matrix[1][1]
        );
        println!(
            "ack latency  p50 {} ms | p90 {} ms | p99 {} ms | max {} ms",
            percentile(&establish_latencies, 0.50),
            percentile(&establish_latencies, 0.90),
            percentile(&establish_latencies, 0.99),
            establish_latencies.last().copied().unwrap_or(0)
        );
    }

    println!("\n--- by reply-block budget (interleaved) ---");
    println!(
        "{:>6}  {:>8}  {:>3}  {:>6}  {:>6}  {:>9}",
        "budget", "trials", "ok", "fails", "rate", "p50"
    );
    for (label, (good, bad, mut samples)) in per_budget {
        samples.sort_unstable();
        let p50 = percentile(&samples, 0.50);
        let total = good + bad;
        println!(
            "{label:>6}  {total:>8}  {good:>3}  {bad:>6}  {:>5.1}%  {p50:>6} ms",
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

/// Dial an address that routes but has nobody behind it, and see what the caller is told.
///
/// The client is registered and then disconnected, so its gateway stays in the topology and keeps
/// accepting for it. This is the failure the dialling side could not detect at all: an address
/// whose owner is not running.
async fn dead_peer_arm(
    dial_client: &mut MixnetClient,
    surb_threshold: Option<usize>,
    trials: usize,
    establish_timeout: Duration,
) -> Result<()> {
    let absent = connect(surb_threshold)
        .await
        .context("connecting the client that is about to leave")?;
    let absent_address = *absent.nym_address();
    absent.disconnect().await;
    println!("--- dead peer arm ---");
    println!("dead   {absent_address}  (registered, then disconnected)");

    let mut acknowledged = 0usize;
    let mut refused = 0usize;
    for trial in 1..=trials {
        let started = Instant::now();
        match dial_client.open_stream(absent_address, None).await {
            Ok(mut stream) => {
                let outcome = stream.wait_established_with_timeout(establish_timeout).await;
                let elapsed = started.elapsed().as_millis();
                match outcome {
                    Ok(()) => {
                        acknowledged += 1;
                        println!("{trial:>4}  dead  ACKNOWLEDGED {elapsed:>6} ms");
                    }
                    Err(error) => println!("{trial:>4}  dead  no ack      {elapsed:>6} ms  {error}"),
                }
            }
            Err(error) => {
                refused += 1;
                let elapsed = started.elapsed().as_millis();
                println!("{trial:>4}  dead  OPEN-REFUSED {elapsed:>6} ms  {error}");
            }
        }
    }
    println!(
        "dead peer: {trials} dials | opens refused {refused} | acknowledged {acknowledged}\n"
    );
    Ok(())
}

/// Percentile over an already sorted slice, at index `round((n - 1) * fraction)`.
fn percentile(sorted: &[u128], fraction: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[(((sorted.len() - 1) as f64) * fraction).round() as usize]
}
