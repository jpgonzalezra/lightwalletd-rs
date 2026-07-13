//! gRPC types and service stubs generated from the `.proto` files by `tonic-build`.
//!
//! The package is `cash.z.wallet.sdk.rpc`; the generated module is re-exported here so the rest of
//! the crate can refer to it as `crate::proto`.

use std::pin::Pin;

use tokio_stream::Stream;
use tonic::Status;

tonic::include_proto!("cash.z.wallet.sdk.rpc");

/// Boxed server-streaming response, shared by every streaming method's associated type.
pub(crate) type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

/// Encoded `prost_types::FileDescriptorSet` for `service.proto` + `darkside.proto`, emitted by
/// `build.rs`. Registered with `tonic-reflection` at startup so `grpcurl`/`grpcui` and similar
/// tools can discover and describe the server's gRPC services without a local `.proto` checkout.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.bin"));
