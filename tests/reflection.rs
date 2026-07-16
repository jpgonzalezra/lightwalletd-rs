//! Verifies gRPC Server Reflection (S10): a running server advertises `CompactTxStreamer` and
//! `DarksideStreamer` over the standard reflection service, so tools like `grpcurl -plaintext
//! <addr> list` can discover the API without a local `.proto` checkout.

mod common;

use common::TestServer;
use tonic_reflection::pb::v1::ServerReflectionRequest;
use tonic_reflection::pb::v1::server_reflection_request::MessageRequest;
use tonic_reflection::pb::v1::server_reflection_response::MessageResponse;

/// `ListServices` over the reflection API returns every service the in-process darkside server
/// registers, matching what `grpcurl -plaintext <addr> list` would show against a real binary.
#[tokio::test]
async fn list_services_advertises_compact_tx_streamer_and_darkside_streamer() {
    let mut server = TestServer::start().await;

    let request = ServerReflectionRequest {
        host: String::new(),
        message_request: Some(MessageRequest::ListServices(String::new())),
    };
    let response = server
        .reflection
        .server_reflection_info(tokio_stream::once(request))
        .await
        .unwrap();

    let message = response
        .into_inner()
        .message()
        .await
        .unwrap()
        .expect("one ListServices response");

    let names: Vec<String> = match message.message_response {
        Some(MessageResponse::ListServicesResponse(list)) => {
            list.service.into_iter().map(|s| s.name).collect()
        }
        other => panic!("expected ListServicesResponse, got {other:?}"),
    };

    assert!(
        names.contains(&"cash.z.wallet.sdk.rpc.CompactTxStreamer".to_string()),
        "expected CompactTxStreamer in {names:?}"
    );
    assert!(
        names.contains(&"cash.z.wallet.sdk.rpc.DarksideStreamer".to_string()),
        "expected DarksideStreamer in {names:?}"
    );
}
