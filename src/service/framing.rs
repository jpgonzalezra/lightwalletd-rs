//! Batching that keeps a server-streaming response from turning into one tiny HTTP/2 DATA frame
//! per message.
//!
//! Tonic's encoder packs consecutive messages into one body chunk and cuts it when the source
//! stream returns `Poll::Pending`. A range served from the cache never pends between blocks, so it
//! already leaves as full-sized frames. A range served from the node pends on every fetch, so each
//! block leaves alone, and a compact block with no shielded output is under 100 bytes. `h2` charges
//! a connection budget for every DATA frame below 256 bytes and closes the connection with
//! `ENHANCE_YOUR_CALM` once it runs out. A run of empty blocks gets there in around 150 messages.
//!
//! [`coalesce`] holds messages back until they are worth a frame, so the wire shape is the same
//! whichever side the messages came from. See ADR 0037.

use async_stream::stream;
use tokio_stream::Stream;
use tonic::Status;

/// Encoded bytes a batch holds before it is released. Well clear of the 256-byte threshold `h2`
/// charges a frame against, and small next to what one fetch costs to produce.
const BATCH_BYTES: usize = 4 * 1024;

/// Batch `source` so its messages leave in groups that are worth a DATA frame each.
///
/// The batch goes out with nothing awaited in between, which is what lands it in one chunk.
/// Whatever is still held is flushed when the source ends, and ahead of an error, so a stream that
/// fails partway still delivers the prefix it had already produced.
///
/// For streams that end. A batch is held until it fills or the source runs out, so an open-ended
/// stream like `GetMempoolStream` would sit on its last messages waiting for a batch to complete.
pub(super) fn coalesce<T, S>(source: S) -> impl Stream<Item = Result<T, Status>> + Send
where
    T: prost::Message,
    S: Stream<Item = Result<T, Status>> + Send,
{
    stream! {
        let mut batch = FrameBatch::new();
        for await message in source {
            match message {
                Ok(message) => {
                    if let Some(ready) = batch.push(message) {
                        for message in ready {
                            yield Ok(message);
                        }
                    }
                }
                Err(status) => {
                    for message in batch.flush() {
                        yield Ok(message);
                    }
                    yield Err(status);
                    return;
                }
            }
        }
        for message in batch.flush() {
            yield Ok(message);
        }
    }
}

/// Messages held back until they add up to a frame worth sending.
struct FrameBatch<T> {
    pending: Vec<T>,
    bytes: usize,
}

impl<T: prost::Message> FrameBatch<T> {
    fn new() -> Self {
        Self {
            pending: Vec::new(),
            bytes: 0,
        }
    }

    /// Add `message`, returning everything held once it is worth a frame.
    fn push(&mut self, message: T) -> Option<Vec<T>> {
        self.bytes += message.encoded_len();
        self.pending.push(message);
        (self.bytes >= BATCH_BYTES).then(|| self.take())
    }

    /// Whatever is still held.
    fn flush(&mut self) -> Vec<T> {
        self.take()
    }

    fn take(&mut self) -> Vec<T> {
        self.bytes = 0;
        std::mem::take(&mut self.pending)
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::{Pin, pin};
    use std::task::{Context, Poll, Waker};

    use async_stream::stream;
    use prost::Message;
    use tokio_stream::{Stream, StreamExt};
    use tonic::Status;

    use super::{BATCH_BYTES, coalesce};
    use crate::proto::CompactBlock;

    /// A block whose encoded form is a little over `size` bytes.
    fn block(size: usize) -> CompactBlock {
        CompactBlock {
            height: 1,
            hash: vec![0; size],
            ..Default::default()
        }
    }

    /// How many blocks of `size` a batch holds before it is released.
    fn batch_len(size: usize) -> usize {
        BATCH_BYTES.div_ceil(block(size).encoded_len())
    }

    /// Pends once, standing in for the node fetch a cache miss awaits.
    struct PendOnce(bool);

    impl Future for PendOnce {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
            if self.0 {
                return Poll::Ready(());
            }
            self.0 = true;
            context.waker().wake_by_ref();
            Poll::Pending
        }
    }

    /// `count` blocks of `size`, pending between them the way a node-served range does.
    fn node_served(count: usize, size: usize) -> impl Stream<Item = Result<CompactBlock, Status>> {
        stream! {
            for _ in 0..count {
                PendOnce(false).await;
                yield Ok(block(size));
            }
        }
    }

    /// Poll until the stream hands something over, then count what follows without pending again.
    /// That run is what the encoder packs into one chunk.
    fn ready_run<S>(stream: &mut Pin<&mut S>) -> usize
    where
        S: Stream<Item = Result<CompactBlock, Status>>,
    {
        let mut context = Context::from_waker(Waker::noop());
        loop {
            match stream.as_mut().poll_next(&mut context) {
                Poll::Pending => continue,
                Poll::Ready(None) => return 0,
                Poll::Ready(Some(_)) => break,
            }
        }
        let mut count = 1;
        while let Poll::Ready(Some(_)) = stream.as_mut().poll_next(&mut context) {
            count += 1;
        }
        count
    }

    #[test]
    fn hands_over_a_whole_batch_without_pending() {
        let held = batch_len(64);
        let mut batched = pin!(coalesce(node_served(held * 2, 64)));
        assert_eq!(ready_run(&mut batched), held);
    }

    #[test]
    fn hands_over_a_message_that_is_already_worth_a_frame_on_its_own() {
        let mut batched = pin!(coalesce(node_served(2, BATCH_BYTES)));
        assert_eq!(ready_run(&mut batched), 1);
    }

    #[tokio::test]
    async fn delivers_every_message_including_the_last_partial_batch() {
        let batched = coalesce(node_served(batch_len(64) + 3, 64));
        assert_eq!(batched.collect::<Vec<_>>().await.len(), batch_len(64) + 3);
    }

    #[tokio::test]
    async fn delivers_what_it_holds_before_forwarding_an_error() {
        let messages = vec![
            Ok(block(64)),
            Ok(block(64)),
            Err(Status::aborted("chain discontinuity")),
        ];
        let delivered = coalesce(tokio_stream::iter(messages))
            .collect::<Vec<_>>()
            .await;
        assert_eq!(
            (delivered.len(), delivered.last().map(Result::is_err)),
            (3, Some(true))
        );
    }

    #[tokio::test]
    async fn ends_at_the_first_error() {
        let messages = vec![Err(Status::aborted("first")), Ok(block(64))];
        let delivered = coalesce(tokio_stream::iter(messages))
            .collect::<Vec<_>>()
            .await;
        assert_eq!(delivered.len(), 1);
    }
}
