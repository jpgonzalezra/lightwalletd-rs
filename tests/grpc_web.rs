//! gRPC-web transport (ADR 0026): the requests a browser actually sends, driven over HTTP/1.1 with
//! a plain HTTP client rather than a generated gRPC client.
//!
//! The framing is asserted by hand on purpose. A generated client would hide exactly the parts a
//! browser depends on: the length-prefixed frames, the trailer frame carrying `grpc-status`, and
//! the CORS headers without which a browser refuses to hand the response to JavaScript.

mod common;

use std::net::SocketAddr;

use common::{TestServer, testdata_blocks};
use lightwalletd_rs::config::GrpcWebOrigins;
use lightwalletd_rs::proto::{BlockId, BlockRange, CompactBlock, LightdInfo};
use prost::Message;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::{Response, StatusCode};

/// `Empty`, encoded: a protobuf message with no fields is zero bytes on the wire.
const EMPTY_MESSAGE: &[u8] = &[];
const WALLET_ORIGIN: &str = "http://localhost:3000";
const OTHER_ORIGIN: &str = "http://evil.example";
const GRPC_WEB_CONTENT_TYPE: &str = "application/grpc-web+proto";
const STREAMER: &str = "cash.z.wallet.sdk.rpc.CompactTxStreamer";

/// A plaintext HTTP/1.1 client, built like every other client in the crate (a rustls provider must
/// be installed before `reqwest::Client::builder()`, even for a connection that never uses TLS).
fn browser_like_client() -> reqwest::Client {
    let _ = rustls::crypto::ring::default_provider().install_default();
    reqwest::Client::builder().build().unwrap()
}

/// Frame a protobuf message the way a gRPC-web client does: one flags byte, four big-endian length
/// bytes, then the message.
fn frame(message: &[u8]) -> Vec<u8> {
    let mut framed = vec![0u8];
    framed.extend_from_slice(&(message.len() as u32).to_be_bytes());
    framed.extend_from_slice(message);
    framed
}

/// Split a gRPC-web response body into its message frames and its trailer frame, which carries the
/// call's `grpc-status` as `key:value\r\n` lines.
fn decode_frames(body: &[u8]) -> (Vec<Vec<u8>>, String) {
    let mut messages = Vec::new();
    let mut trailers = String::new();
    let mut rest = body;
    while !rest.is_empty() {
        assert!(rest.len() >= 5, "truncated frame header in {rest:?}");
        let flags = rest[0];
        let length = u32::from_be_bytes([rest[1], rest[2], rest[3], rest[4]]) as usize;
        assert!(
            rest.len() >= 5 + length,
            "frame length {length} overruns body"
        );
        let (payload, remainder) = rest[5..].split_at(length);
        // Bit 7 of the flags marks the trailer frame; everything else is a message.
        if flags & 0x80 == 0 {
            messages.push(payload.to_vec());
        } else {
            trailers = String::from_utf8_lossy(payload).into_owned();
        }
        rest = remainder;
    }
    (messages, trailers)
}

/// POST a gRPC-web call, with the headers the browser client library sends.
async fn call(
    client: &reqwest::Client,
    addr: SocketAddr,
    method: &str,
    message: &[u8],
    origin: &str,
) -> Response {
    client
        .post(format!("http://{addr}/{STREAMER}/{method}"))
        .header("content-type", GRPC_WEB_CONTENT_TYPE)
        .header("x-grpc-web", "1")
        .header("origin", origin)
        .body(frame(message))
        .send()
        .await
        .unwrap()
}

/// The `OPTIONS` request a browser sends before the call above.
async fn preflight(
    client: &reqwest::Client,
    addr: SocketAddr,
    method: &str,
    origin: &str,
) -> Response {
    client
        .request(
            reqwest::Method::OPTIONS,
            format!("http://{addr}/{STREAMER}/{method}"),
        )
        .header("origin", origin)
        .header("access-control-request-method", "POST")
        .header("access-control-request-headers", "content-type,x-grpc-web")
        .send()
        .await
        .unwrap()
}

/// A header's value as a comma-separated lowercase list, for asserting on CORS headers whose order
/// is not part of the contract.
fn header_list(headers: &HeaderMap, name: &str) -> Vec<String> {
    headers
        .get(name)
        .map(HeaderValue::to_str)
        .transpose()
        .unwrap()
        .unwrap_or_default()
        .split(',')
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .collect()
}

/// A server with the four testdata blocks staged and the gRPC-web transport restricted to one origin.
async fn server_with_blocks() -> TestServer {
    let mut server = TestServer::start_with_grpc_web(Some(GrpcWebOrigins::Only(vec![
        WALLET_ORIGIN.to_string(),
    ])))
    .await;
    server.reset(380640, "2bb40e60", "main").await;
    server.stage_blocks(testdata_blocks()).await;
    server.apply_staged(380643).await;
    server
}

#[tokio::test]
async fn a_unary_call_over_grpc_web_returns_a_decodable_message_and_an_ok_status() {
    let server = server_with_blocks().await;
    let client = browser_like_client();

    let response = call(
        &client,
        server.addr,
        "GetLightdInfo",
        EMPTY_MESSAGE,
        WALLET_ORIGIN,
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        GRPC_WEB_CONTENT_TYPE
    );
    let body = response.bytes().await.unwrap();
    let (messages, trailers) = decode_frames(&body);
    let info = LightdInfo::decode(messages[0].as_slice()).unwrap();
    assert_eq!(
        (info.chain_name.as_str(), info.block_height),
        ("main", 380643)
    );
    assert!(
        trailers.contains("grpc-status:0"),
        "expected an OK trailer frame, got {trailers:?}"
    );
}

#[tokio::test]
async fn a_server_streaming_call_over_grpc_web_returns_one_frame_per_block() {
    let server = server_with_blocks().await;
    let client = browser_like_client();
    let range = BlockRange {
        start: Some(BlockId {
            height: 380640,
            hash: vec![],
        }),
        end: Some(BlockId {
            height: 380643,
            hash: vec![],
        }),
        pool_types: vec![],
    };

    let response = call(
        &client,
        server.addr,
        "GetBlockRange",
        &range.encode_to_vec(),
        WALLET_ORIGIN,
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = response.bytes().await.unwrap();
    let (messages, trailers) = decode_frames(&body);
    let heights: Vec<u64> = messages
        .iter()
        .map(|message| CompactBlock::decode(message.as_slice()).unwrap().height)
        .collect();
    assert_eq!(heights, vec![380640, 380641, 380642, 380643]);
    assert!(
        trailers.contains("grpc-status:0"),
        "expected an OK trailer frame, got {trailers:?}"
    );
}

#[tokio::test]
async fn a_preflight_from_an_allowed_origin_permits_every_header_a_grpc_web_client_sends() {
    let server = server_with_blocks().await;
    let client = browser_like_client();

    let response = preflight(&client, server.addr, "GetLightdInfo", WALLET_ORIGIN).await;

    assert!(response.status().is_success());
    let headers = response.headers().clone();
    assert_eq!(
        headers.get("access-control-allow-origin").unwrap(),
        WALLET_ORIGIN
    );
    assert!(header_list(&headers, "access-control-allow-methods").contains(&"post".to_string()));
    // A header missing here fails the preflight, and the call never leaves the browser.
    for header in [
        "content-type",
        "authorization",
        "x-grpc-web",
        "x-user-agent",
        "grpc-timeout",
        "grpc-encoding",
        "grpc-accept-encoding",
    ] {
        assert!(
            header_list(&headers, "access-control-allow-headers").contains(&header.to_string()),
            "{header} must be allowed on the request"
        );
    }
}

/// `Access-Control-Expose-Headers` rides the *answer*, not the preflight (the preflight only
/// negotiates what may be sent), so this is asserted on a real call.
#[tokio::test]
async fn a_call_exposes_the_grpc_status_headers_to_javascript() {
    let server = server_with_blocks().await;
    let client = browser_like_client();

    let response = call(
        &client,
        server.addr,
        "GetLightdInfo",
        EMPTY_MESSAGE,
        WALLET_ORIGIN,
    )
    .await;

    let headers = response.headers().clone();
    // Without these a trailers-only error (every early rejection) reaches the browser with its
    // reason stripped off, and nothing in the symptom mentions CORS.
    for header in ["grpc-status", "grpc-message", "grpc-status-details-bin"] {
        assert!(
            header_list(&headers, "access-control-expose-headers").contains(&header.to_string()),
            "{header} must be readable from JavaScript"
        );
    }
}

/// The case the exposed headers exist for. A call rejected before any message is written answers
/// trailers-only, which gRPC-web carries as HTTP headers rather than as a trailer frame in the body:
/// a browser reads the outcome off the response itself, and only because those headers are exposed.
#[tokio::test]
async fn a_rejected_call_over_grpc_web_carries_its_status_in_the_headers_and_sends_no_frames() {
    let server = server_with_blocks().await;
    let client = browser_like_client();

    // What a client built against a newer `service.proto` sends: a method this server does not have.
    let response = call(
        &client,
        server.addr,
        "NoSuchMethod",
        EMPTY_MESSAGE,
        WALLET_ORIGIN,
    )
    .await;

    let status = response.status();
    let grpc_status = response
        .headers()
        .get("grpc-status")
        .map(|value| value.to_str().unwrap().to_string());
    let body = response.bytes().await.unwrap();
    // `Unimplemented` (12) in the headers, and nothing in the body to decode it from.
    assert_eq!(
        (status, grpc_status.as_deref(), body.len()),
        (StatusCode::OK, Some("12"), 0)
    );
}

#[tokio::test]
async fn a_preflight_from_an_origin_outside_the_allowlist_is_not_granted() {
    let server = server_with_blocks().await;
    let client = browser_like_client();

    let response = preflight(&client, server.addr, "GetLightdInfo", OTHER_ORIGIN).await;

    // The browser is the enforcement point: with no `access-control-allow-origin` in the answer it
    // refuses to send the call at all.
    assert_eq!(response.headers().get("access-control-allow-origin"), None);
}

#[tokio::test]
async fn a_call_from_an_origin_outside_the_allowlist_is_not_granted() {
    let server = server_with_blocks().await;
    let client = browser_like_client();

    let response = call(
        &client,
        server.addr,
        "GetLightdInfo",
        EMPTY_MESSAGE,
        OTHER_ORIGIN,
    )
    .await;

    assert_eq!(response.headers().get("access-control-allow-origin"), None);
}

#[tokio::test]
async fn any_origin_is_granted_when_no_allowlist_is_configured() {
    let mut server = TestServer::start_with_grpc_web(Some(GrpcWebOrigins::Any)).await;
    server.reset(380640, "2bb40e60", "main").await;
    let client = browser_like_client();

    let response = call(
        &client,
        server.addr,
        "GetLightdInfo",
        EMPTY_MESSAGE,
        OTHER_ORIGIN,
    )
    .await;

    assert_eq!(
        response
            .headers()
            .get("access-control-allow-origin")
            .unwrap(),
        "*"
    );
}

#[tokio::test]
async fn with_the_transport_off_the_server_speaks_only_grpc_over_http2() {
    let mut server = TestServer::start().await;
    server.reset(380640, "2bb40e60", "main").await;
    server.stage_blocks(testdata_blocks()).await;
    server.apply_staged(380643).await;
    let client = browser_like_client();

    let attempt = client
        .post(format!("http://{}/{STREAMER}/GetLightdInfo", server.addr))
        .header("content-type", GRPC_WEB_CONTENT_TYPE)
        .header("x-grpc-web", "1")
        .header("origin", WALLET_ORIGIN)
        .body(frame(EMPTY_MESSAGE))
        .send()
        .await;

    // Not a 4xx: without `--grpc-web` the listener never negotiates HTTP/1.1, so the request does
    // not survive the connection.
    assert!(
        attempt.is_err(),
        "expected no HTTP/1.1 answer, got {attempt:?}"
    );
    // ... while the HTTP/2 gRPC path is untouched.
    let info = server
        .compact
        .get_lightd_info(lightwalletd_rs::proto::Empty {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(info.block_height, 380643);
}
