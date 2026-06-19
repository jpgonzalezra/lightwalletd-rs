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
