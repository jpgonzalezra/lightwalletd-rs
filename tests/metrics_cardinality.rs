//! What a client can add to the metrics registry (ADR 0035): the label values are the server's, so
//! inventing paths and verbs buys one shared bucket rather than a series per request.
//!
//! The registry is process-global, so this file keeps a single test. A second one running next to
//! it would record its own traffic into the same series.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;

use common::TestServer;
use lightwalletd_rs::config::GrpcWebOrigins;
use lightwalletd_rs::proto::Empty;
use reqwest::Method;
use tokio::net::TcpListener;

const GET_LIGHTD_INFO: &str = "/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLightdInfo";
const UNKNOWN_PATH: &str = "/unknown/unknown";
const GRPC_WEB_CONTENT_TYPE: &str = "application/grpc-web+proto";
const INVENTED_PATHS: usize = 50;
const INVENTED_VERBS: usize = 10;

#[tokio::test]
async fn invented_paths_and_verbs_share_one_bucket_instead_of_minting_series() {
    let mut server = TestServer::start_with_grpc_web(Some(GrpcWebOrigins::Any)).await;
    let metrics_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let metrics_addr = metrics_listener.local_addr().unwrap();
    tokio::spawn(lightwalletd_rs::metrics::serve_on(metrics_listener));

    // One real call, so the test also pins that a served method keeps its own labels. It fails on a
    // mock chain with no blocks staged, and a failed call is recorded like a successful one.
    let _ = server.compact.get_lightd_info(Empty {}).await;

    let _ = rustls::crypto::ring::default_provider().install_default();
    let client = reqwest::Client::builder().build().unwrap();
    for path in 0..INVENTED_PATHS {
        let response = client
            .post(format!("http://{}/A/{path}", server.addr))
            .header("content-type", GRPC_WEB_CONTENT_TYPE)
            .send()
            .await;
        assert!(response.is_ok(), "the server answered every invented path");
    }
    for verb in 0..INVENTED_VERBS {
        let verb = Method::from_bytes(format!("VERB{verb}").as_bytes()).unwrap();
        let response = client
            .request(verb, format!("http://{}{GET_LIGHTD_INFO}", server.addr))
            .send()
            .await;
        assert!(response.is_ok(), "the server answered every invented verb");
    }

    let scraped = scrape(metrics_addr).await;
    let series = series(&scraped);
    let invented_in_the_registry: Vec<_> = scraped
        .lines()
        .filter(|line| line.contains("/A/") || line.contains("VERB"))
        .collect();
    // Collapsed into one bucket, but still counted: an operator watching someone probe paths sees
    // the volume.
    let unrouted_calls = counter(&scraped, "grpc_server_started_total", "unknown");

    assert_eq!(
        (
            invented_in_the_registry,
            label_values(&series, "grpc_method"),
            label_values(&series, "path"),
            label_values(&series, "method"),
            unrouted_calls,
        ),
        (
            Vec::new(),
            BTreeSet::from(["GetLightdInfo".to_owned(), "unknown".to_owned()]),
            BTreeSet::from([GET_LIGHTD_INFO.to_owned(), UNKNOWN_PATH.to_owned()]),
            BTreeSet::from(["OTHER".to_owned(), "POST".to_owned()]),
            (INVENTED_PATHS + INVENTED_VERBS) as f64,
        )
    );
}

async fn scrape(addr: SocketAddr) -> String {
    reqwest::get(format!("http://{addr}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap()
}

/// Every `(name, labels)` pair the scrape reports, comments dropped.
fn series(body: &str) -> Vec<(String, BTreeMap<String, String>)> {
    body.lines()
        .filter(|line| !line.starts_with('#'))
        .filter_map(|line| {
            let (head, _value) = line.rsplit_once(' ')?;
            let (name, labels) = head.split_once('{').map_or((head, ""), |(name, labels)| {
                (name, labels.trim_end_matches('}'))
            });
            Some((name.to_owned(), labels_of(labels)))
        })
        .collect()
}

fn labels_of(labels: &str) -> BTreeMap<String, String> {
    labels
        .split("\",")
        .filter_map(|pair| pair.split_once("=\""))
        .map(|(name, value)| (name.to_owned(), value.trim_end_matches('"').to_owned()))
        .collect()
}

/// Every value the scrape carries for one label name, across all series that use it.
fn label_values(series: &[(String, BTreeMap<String, String>)], label: &str) -> BTreeSet<String> {
    series
        .iter()
        .filter_map(|(_, labels)| labels.get(label).cloned())
        .collect()
}

/// The value of a counter, picked by the `grpc_method` label its series carries.
fn counter(body: &str, name: &str, grpc_method: &str) -> f64 {
    body.lines()
        .filter(|line| {
            line.starts_with(name) && line.contains(&format!("grpc_method=\"{grpc_method}\""))
        })
        .filter_map(|line| line.rsplit_once(' '))
        .filter_map(|(_, value)| value.parse::<f64>().ok())
        .sum()
}
