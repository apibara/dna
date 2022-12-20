//! Group messages into batches.

use std::{
    pin::Pin,
    task::{self, Poll},
    time::Duration,
};

use futures::Stream;
use pin_project::pin_project;

use crate::core::pb::{starknet, stream};

/// One item to be batched.
#[derive(Debug)]
pub struct BatchItem {
    pub is_live: bool,
    pub cursor: stream::v1alpha2::Cursor,
    pub data: Option<starknet::v1alpha2::Block>,
}

/// Message from data stream to batcher.
#[derive(Debug)]
pub enum BatchMessage {
    Finalized(BatchItem),
    Accepted(BatchItem),
    Pending(BatchItem),
}

pub trait BatchDataStreamExt: Stream {
    /// Returns a new data stream that batches messages into one.
    ///
    /// The resulting stream will have batches of size `batch_size`,
    /// unless data is produced too slowly, in which case the stream
    /// produces smaller batches every `interval`.
    fn batch<E>(self, batch_size: usize, interval: Duration) -> BatchDataStream<Self, E>
    where
        Self: Stream<Item = Result<BatchMessage, E>> + Sized,
        E: std::error::Error,
    {
        BatchDataStream::new(self, batch_size, interval)
    }
}

#[pin_project]
pub struct BatchDataStream<S, E>
where
    S: Stream<Item = Result<BatchMessage, E>>,
    E: std::error::Error,
{
    #[pin]
    inner: S,
    interval: Duration,
    state: BatchState,
}

struct BatchState {
    items: Vec<starknet::v1alpha2::Block>,
    latest_cursor: stream::v1alpha2::Cursor,
    data_finality: stream::v1alpha2::DataFinality,
    batch_size: usize,
}

impl BatchMessage {
    pub fn is_live(&self) -> bool {
        match self {
            BatchMessage::Finalized(item) => item.is_live,
            BatchMessage::Accepted(item) => item.is_live,
            BatchMessage::Pending(item) => item.is_live,
        }
    }
}

impl<S, E> BatchDataStream<S, E>
where
    S: Stream<Item = Result<BatchMessage, E>>,
    E: std::error::Error,
{
    pub fn new(inner: S, batch_size: usize, interval: Duration) -> Self {
        let state = BatchState::new(batch_size);
        BatchDataStream {
            inner,
            interval,
            state,
        }
    }
}

impl BatchState {
    pub fn new(batch_size: usize) -> Self {
        BatchState {
            latest_cursor: stream::v1alpha2::Cursor::default(),
            items: Vec::with_capacity(batch_size),
            data_finality: stream::v1alpha2::DataFinality::DataStatusUnknown,
            batch_size,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn should_drain(&self) -> bool {
        self.items.len() >= self.batch_size
    }

    pub fn push_data(&mut self, cursor: stream::v1alpha2::Cursor, data: starknet::v1alpha2::Block) {
        self.latest_cursor = cursor;
        self.items.push(data);
    }

    pub fn drain_data(&mut self) -> stream::v1alpha2::Data {
        let items = std::mem::take(&mut self.items);
        let end_cursor = std::mem::take(&mut self.latest_cursor);
        let data = stream::v1alpha2::Data {
            end_cursor: Some(end_cursor),
            finality: self.data_finality as i32,
            data: items,
        };
        self.items = Vec::with_capacity(self.batch_size);
        data
    }
}

impl<S, E> Stream for BatchDataStream<S, E>
where
    S: Stream<Item = Result<BatchMessage, E>>,
    E: std::error::Error,
{
    type Item = Result<stream::v1alpha2::Data, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            match this.inner.as_mut().poll_next(cx) {
                Poll::Pending => {
                    // TODO: check interval clock
                    return Poll::Pending;
                }
                Poll::Ready(None) => {
                    // inner stream complete. drain this one
                    if this.state.is_empty() {
                        return Poll::Ready(None);
                    }
                    let data = this.state.drain_data();
                    return Poll::Ready(Some(Ok(data)));
                }
                Poll::Ready(Some(Ok(message))) => {
                    // TODO:
                    // - update finality data (if first message)
                    // - update end cursor
                    // - insert into items
                    // - maybe drain data
                    let is_live = message.is_live();
                    match message {
                        BatchMessage::Finalized(item) => {
                            if let Some(data) = item.data {
                                this.state.push_data(item.cursor, data);
                            }
                        }
                        _ => {
                            // TODO: update state based on message
                            todo!()
                        }
                    }

                    if this.state.should_drain() || is_live {
                        let data = this.state.drain_data();
                        return Poll::Ready(Some(Ok(data)));
                    }

                    // continue loop
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
            }
        }
    }
}
