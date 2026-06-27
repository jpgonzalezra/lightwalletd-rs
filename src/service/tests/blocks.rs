//! Unit tests for the block-serving methods (`GetBlock`, `GetBlockRange`).

use std::sync::Arc;

use tonic::{Code, Request};

use crate::proto::compact_tx_streamer_server::CompactTxStreamer;
use crate::proto::{BlockId, BlockRange};
use crate::testutil::FakeNode;

use super::streamer_with;

#[tokio::test]
async fn get_block_past_the_tip_maps_to_out_of_range() {
    let fake = Arc::new(FakeNode {
        block_verbose_err: Some((-8, "block height not in best chain".to_string())),
        ..Default::default()
    });
    let (_dir, streamer) = streamer_with(fake);

    let status = streamer
        .get_block(Request::new(BlockId {
            height: 99_999_999,
            hash: vec![],
        }))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::OutOfRange);
}

#[tokio::test]
async fn get_block_with_unclassified_node_error_maps_to_unavailable() {
    let fake = Arc::new(FakeNode {
        block_verbose_err: Some((-99, "something unexpected".to_string())),
        ..Default::default()
    });
    let (_dir, streamer) = streamer_with(fake);

    let status = streamer
        .get_block(Request::new(BlockId {
            height: 1,
            hash: vec![],
        }))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::Unavailable);
}

#[tokio::test]
async fn get_block_unspecified_identifier_is_invalid_argument() {
    let (_dir, streamer) = streamer_with(Arc::new(FakeNode::default()));

    let status = streamer
        .get_block(Request::new(BlockId {
            height: 0,
            hash: vec![],
        }))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn get_block_range_without_start_is_invalid_argument() {
    let (_dir, streamer) = streamer_with(Arc::new(FakeNode::default()));

    let status = streamer
        .get_block_range(Request::new(BlockRange {
            start: None,
            end: Some(BlockId {
                height: 2,
                hash: vec![],
            }),
            ..Default::default()
        }))
        .await
        .err()
        .unwrap();

    assert_eq!(status.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn get_block_range_without_end_is_invalid_argument() {
    let (_dir, streamer) = streamer_with(Arc::new(FakeNode::default()));

    let status = streamer
        .get_block_range(Request::new(BlockRange {
            start: Some(BlockId {
                height: 1,
                hash: vec![],
            }),
            end: None,
            ..Default::default()
        }))
        .await
        .err()
        .unwrap();

    assert_eq!(status.code(), Code::InvalidArgument);
}
