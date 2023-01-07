//! Group messages into batches.

use std::{
    pin::Pin,
    task::{self, Poll},
    time::Duration,
};

use futures::Stream;
use pin_project::pin_project;

use crate::core::pb::{starknet, stream};

use super::StreamError;

/// One item to be batched.
#[derive(Debug)]
pub struct BatchItem {
    pub is_live: bool,
    pub stream_id: u64,
    pub cursor: Option<stream::v1alpha2::Cursor>,
    pub end_cursor: stream::v1alpha2::Cursor,
    pub data_finality: stream::v1alpha2::DataFinality,
    pub data: Option<starknet::v1alpha2::Block>,
}

/// Message from data stream to batcher.
#[derive(Debug)]
pub enum BatchMessage {
    Finalized(BatchItem),
    Accepted(BatchItem),
    Pending(BatchItem),
    Invalidate(BatchItem),
}

pub trait BatchDataStreamExt: Stream {
    /// Returns a new data stream that batches messages into one.
    ///
    /// The resulting stream will have batches of size `batch_size`,
    /// unless data is produced too slowly, in which case the stream
    /// produces smaller batches every `interval`.
    fn batch(self, batch_size: usize, interval: Duration) -> BatchDataStream<Self>
    where
        Self: Stream<Item = Result<BatchMessage, StreamError>> + Sized,
    {
        BatchDataStream::new(self, batch_size, interval)
    }
}

#[pin_project]
pub struct BatchDataStream<S>
where
    S: Stream<Item = Result<BatchMessage, StreamError>>,
{
    #[pin]
    inner: S,
    interval: Duration,
    state: BatchState,
}

struct BatchState {
    items: Vec<starknet::v1alpha2::Block>,
    starting_cursor: Option<stream::v1alpha2::Cursor>,
    latest_cursor: stream::v1alpha2::Cursor,
    data_finality: stream::v1alpha2::DataFinality,
    batch_size: usize,
    stream_id: Option<u64>,
}

impl BatchMessage {
    pub fn is_live(&self) -> bool {
        match self {
            BatchMessage::Finalized(item) => item.is_live,
            BatchMessage::Accepted(item) => item.is_live,
            BatchMessage::Pending(item) => item.is_live,
            BatchMessage::Invalidate(item) => item.is_live,
        }
    }

    pub fn stream_id(&self) -> u64 {
        match self {
            BatchMessage::Finalized(item) => item.stream_id,
            BatchMessage::Accepted(item) => item.stream_id,
            BatchMessage::Pending(item) => item.stream_id,
            BatchMessage::Invalidate(item) => item.stream_id,
        }
    }

    pub fn data_finality(&self) -> stream::v1alpha2::DataFinality {
        match self {
            BatchMessage::Finalized(item) => item.data_finality,
            BatchMessage::Accepted(item) => item.data_finality,
            BatchMessage::Pending(item) => item.data_finality,
            BatchMessage::Invalidate(item) => item.data_finality,
        }
    }
}

impl<S> BatchDataStream<S>
where
    S: Stream<Item = Result<BatchMessage, StreamError>>,
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
            starting_cursor: None,
            latest_cursor: stream::v1alpha2::Cursor::default(),
            items: Vec::with_capacity(batch_size),
            data_finality: stream::v1alpha2::DataFinality::DataStatusUnknown,
            batch_size,
            stream_id: None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn should_drain(&self) -> bool {
        self.items.len() >= self.batch_size
    }

    pub fn push_data(
        &mut self,
        starting_cursor: Option<stream::v1alpha2::Cursor>,
        cursor: stream::v1alpha2::Cursor,
        stream_id: u64,
        finality: stream::v1alpha2::DataFinality,
        data: starknet::v1alpha2::Block,
    ) {
        self.latest_cursor = cursor;
        self.stream_id = Some(stream_id);
        self.data_finality = finality;
        if self.items.is_empty() {
            self.starting_cursor = starting_cursor;
        }
        self.items.push(data);
    }

    pub fn drain_data(&mut self) -> stream::v1alpha2::StreamDataResponse {
        use stream::v1alpha2::stream_data_response::Message;

        let items = std::mem::take(&mut self.items);
        let cursor = std::mem::take(&mut self.starting_cursor);
        let end_cursor = std::mem::take(&mut self.latest_cursor);
        let data = stream::v1alpha2::Data {
            cursor,
            end_cursor: Some(end_cursor),
            finality: self.data_finality as i32,
            data: items,
        };
        self.items = Vec::with_capacity(self.batch_size);

        stream::v1alpha2::StreamDataResponse {
            stream_id: self.stream_id.unwrap_or_default(),
            message: Some(Message::Data(data)),
        }
    }
}

impl<S> Stream for BatchDataStream<S>
where
    S: Stream<Item = Result<BatchMessage, StreamError>>,
{
    type Item = Result<stream::v1alpha2::StreamDataResponse, StreamError>;

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
                    // drain conditions:
                    // - new stream id
                    // - new data finality
                    // - batch size
                    // - invalidate message
                    //
                    // in all other cases there is no need to drain
                    let has_different_stream_id = this
                        .state
                        .stream_id
                        .map(|id| id != message.stream_id())
                        .unwrap_or(true);

                    let has_different_data_finality = {
                        // don't drain on first message, which will have different finality.
                        use stream::v1alpha2::DataFinality;

                        this.state.data_finality != DataFinality::DataStatusUnknown
                            && this.state.data_finality != message.data_finality()
                    };

                    let is_live = message.is_live();

                    let should_drain = has_different_data_finality
                        || has_different_stream_id
                        || is_live
                        || this.state.should_drain();

                    let data_to_drain = match message {
                        BatchMessage::Invalidate(_item) => {
                            // TODO: drain existing data, then prepare to
                            // return invalidate message on next poll
                            todo!()
                        }
                        BatchMessage::Finalized(item) => {
                            let mut data = None;
                            if should_drain {
                                data = Some(this.state.drain_data());
                            }
                            if let Some(data) = item.data {
                                this.state.push_data(
                                    item.cursor,
                                    item.end_cursor,
                                    item.stream_id,
                                    item.data_finality,
                                    data,
                                );
                            }
                            data
                        }
                        BatchMessage::Accepted(_item) => {
                            // TODO: same condition as finalized
                            todo!()
                        }
                        BatchMessage::Pending(_item) => {
                            // TODO: always drain
                            todo!()
                        }
                    };

                    if let Some(data) = data_to_drain {
                        return Poll::Ready(Some(Ok(data)));
                    }

                    // continue loop
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
            }
        }
    }
}
