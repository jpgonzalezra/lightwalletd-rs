//! Unit tests for the chain methods (`GetLatestBlock`, `GetLightdInfo`).

use std::sync::Arc;

use serde_json::json;
use tonic::Request;

use crate::proto::compact_tx_streamer_server::CompactTxStreamer;
use crate::proto::{BlockId, ChainSpec, Empty};
use crate::testutil::FakeNode;

use super::streamer_with;

/// A valid mainnet unified address, used to exercise the donation-address passthrough.
const DONATION_UA: &str = "u1scrubbedbeforepublicationplan001000000000000000000";

/// A node answering the two RPCs `get_lightd_info` issues (`getinfo` + `getblockchaininfo`).
fn lightd_info_node() -> Arc<FakeNode> {
    Arc::new(FakeNode {
        info: Some(
            serde_json::from_value(
                json!({ "build": "v1.2.3", "subversion": "/MagicBean:5.10.0/" }),
            )
            .unwrap(),
        ),
        blockchain_info: Some(
            serde_json::from_value(json!({
                "chain": "main",
                "blocks": 100,
                "bestblockhash": "00",
                "consensus": { "chaintip": "00000000" },
            }))
            .unwrap(),
        ),
        ..Default::default()
    })
}

#[tokio::test]
async fn get_latest_block_reverses_display_hash_to_wire() {
    let display_hash = "0011223344556677889900aabbccddeeff00112233445566778899aabbccddee";
    let fake = Arc::new(FakeNode {
        blockchain_info: Some(
            serde_json::from_value(json!({
                "chain": "main",
                "blocks": 1000,
                "bestblockhash": display_hash,
                "consensus": { "chaintip": "00000000" },
            }))
            .unwrap(),
        ),
        ..Default::default()
    });
    let (_dir, streamer) = streamer_with(fake);

    let response = streamer
        .get_latest_block(Request::new(ChainSpec::default()))
        .await
        .unwrap()
        .into_inner();

    let mut wire = hex::decode(display_hash).unwrap();
    wire.reverse();
    assert_eq!(
        response,
        BlockId {
            height: 1000,
            hash: wire
        }
    );
}

#[tokio::test]
async fn get_lightd_info_advertises_configured_donation_address() {
    let (_dir, streamer) = streamer_with(lightd_info_node());
    let streamer = streamer.with_donation_address(Some(DONATION_UA.to_string()));

    let response = streamer
        .get_lightd_info(Request::new(Empty {}))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.donation_address, DONATION_UA);
}

#[tokio::test]
async fn get_lightd_info_donation_address_empty_when_unset() {
    let (_dir, streamer) = streamer_with(lightd_info_node());

    let response = streamer
        .get_lightd_info(Request::new(Empty {}))
        .await
        .unwrap()
        .into_inner();

    assert!(response.donation_address.is_empty());
}

#[tokio::test]
async fn get_lightd_info_reports_sapling_by_branch_id_and_next_pending_upgrade() {
    let fake = Arc::new(FakeNode {
        info: Some(
            serde_json::from_value(json!({ "build": "v1.2.3", "subversion": "/Zebra:5.1.1/" }))
                .unwrap(),
        ),
        blockchain_info: Some(
            serde_json::from_value(json!({
                "chain": "main",
                "blocks": 100,
                "bestblockhash": "00",
                "consensus": { "chaintip": "5437f330" },
                "upgrades": {
                    "76b809bb": { "name": "Sapling", "activationheight": 419200, "status": "active" },
                    "c8e71055": { "name": "NU6", "activationheight": 2726400, "status": "active" },
                    "aaaaaaaa": { "name": "NU7", "activationheight": 9000000, "status": "pending" },
                    "bbbbbbbb": { "name": "NU8", "activationheight": 9999999, "status": "pending" },
                },
            }))
            .unwrap(),
        ),
        ..Default::default()
    });
    let (_dir, streamer) = streamer_with(fake);

    let info = streamer
        .get_lightd_info(Request::new(Empty {}))
        .await
        .unwrap()
        .into_inner();

    // Sapling is found by branch ID; the next upgrade is the lowest-height pending one (NU7, not NU8).
    assert_eq!(info.sapling_activation_height, 419200);
    assert_eq!(info.upgrade_name, "NU7");
    assert_eq!(info.upgrade_height, 9000000);
}
