//! Stream finalized data.

use std::{sync::Arc, task::Poll};

use futures::Stream;
use pin_project::pin_project;
use tracing::{debug, warn};

use crate::{
    core::{
        pb::{
            starknet::v1alpha2,
            stream::v1alpha2::{DataFinality, StreamDataRequest},
        },
        GlobalBlockId, IngestionMessage, InvalidBlockHashSize,
    },
    db::StorageReader,
};

use super::{
    batch::{BatchDataStream, BatchItem, BatchMessage},
    BatchDataStreamExt, BlockDataAggregator, DatabaseBlockDataAggregator, StreamError,
};

#[pin_project]
pub struct FinalizedBlockStream<R, C, L, CE, LE>
where
    R: StorageReader,
    C: Stream<Item = Result<StreamDataRequest, CE>>,
    L: Stream<Item = Result<IngestionMessage, LE>>,
    CE: std::error::Error,
    LE: std::error::Error,
{
    stream_id: u64,
    previous_cursor: Option<GlobalBlockId>,
    finalized_cursor: GlobalBlockId,
    storage: Arc<R>,
    aggregator: DatabaseBlockDataAggregator<R>,
    #[pin]
    client_stream: C,
    #[pin]
    ingestion: L,
}

#[derive(Debug, thiserror::Error)]
pub enum FinalizedBlockStreamError {
    #[error(transparent)]
    IngestionStream(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("ingestion stream stopped")]
    IngestionStreamStopped,
    #[error(transparent)]
    Aggregate(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("cursor is not valid")]
    InvalidCursor(#[from] InvalidBlockHashSize),
    #[error(transparent)]
    Storage(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl<R, C, L, CE, LE> FinalizedBlockStream<R, C, L, CE, LE>
where
    R: StorageReader,
    C: Stream<Item = Result<StreamDataRequest, CE>>,
    L: Stream<Item = Result<IngestionMessage, LE>>,
    CE: std::error::Error,
    LE: std::error::Error,
{
    pub fn new(
        starting_cursor: Option<GlobalBlockId>,
        finalized_cursor: GlobalBlockId,
        filter: v1alpha2::Filter,
        stream_id: u64,
        storage: Arc<R>,
        client_stream: C,
        ingestion: L,
    ) -> Result<Self, FinalizedBlockStreamError> {
        let aggregator = DatabaseBlockDataAggregator::new(storage.clone(), filter);
        Ok(FinalizedBlockStream {
            stream_id,
            previous_cursor: starting_cursor,
            finalized_cursor,
            storage,
            aggregator,
            client_stream,
            ingestion,
        })
    }
}

impl<R, C, L, CE, LE> Stream for FinalizedBlockStream<R, C, L, CE, LE>
where
    R: StorageReader,
    C: Stream<Item = Result<StreamDataRequest, CE>>,
    L: Stream<Item = Result<IngestionMessage, LE>>,
    CE: std::error::Error + Send + Sync + 'static,
    LE: std::error::Error + Send + Sync + 'static,
{
    type Item = Result<BatchMessage, StreamError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // refresh current finalized block from ingestion events
        let this = self.project();
        match this.ingestion.poll_next(cx) {
            Poll::Pending => {}
            Poll::Ready(None) => {
                let err = StreamError::internal(FinalizedBlockStreamError::IngestionStreamStopped);
                return Poll::Ready(Some(Err(err)));
            }
            Poll::Ready(Some(Err(err))) => {
                let err = FinalizedBlockStreamError::IngestionStream(Box::new(err));
                let err = StreamError::internal(err);
                return Poll::Ready(Some(Err(err)));
            }
            Poll::Ready(Some(Ok(message))) => match message {
                IngestionMessage::Finalized(block_id) => {
                    debug!(block_id = %block_id, "got new finalized block");
                    *this.finalized_cursor = block_id;
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                _ => {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
            },
        }

        match this.client_stream.poll_next(cx) {
            Poll::Pending => {}
            Poll::Ready(None) => {
                // client closed connection. close stream.
                return Poll::Ready(None);
            }
            Poll::Ready(Some(Err(err))) => {
                // client stream error. close stream.
                warn!(err = ?err, "client stream error");
                return Poll::Ready(None);
            }
            Poll::Ready(Some(Ok(request))) => {
                debug!(req = ?request, "client request");
                // ignore batch_size and finality

                if let Some(new_stream_id) = request.stream_id {
                    *this.stream_id = new_stream_id;
                }

                if let Some(new_filter) = request.filter {
                    let aggregator =
                        DatabaseBlockDataAggregator::new(this.storage.clone(), new_filter);
                    *this.aggregator = aggregator;
                }

                if let Some(new_starting_cursor) = request.starting_cursor {
                    let new_previous_cursor = match GlobalBlockId::from_cursor(&new_starting_cursor)
                    {
                        Ok(id) => id,
                        Err(_err) => {
                            let err = StreamError::client("invalid stream cursor");
                            return Poll::Ready(Some(Err(err)));
                        }
                    };
                    *this.previous_cursor = Some(new_previous_cursor);
                }

                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        }

        let current_cursor_block_number = this.previous_cursor.map(|c| c.number() + 1).unwrap_or(0);
        let current_cursor = match this.storage.canonical_block_id(current_cursor_block_number) {
            Err(err) => {
                let err = FinalizedBlockStreamError::Storage(Box::new(err));
                let err = StreamError::internal(err);
                return Poll::Ready(Some(Err(err)));
            }
            Ok(None) => {
                // still waiting on data for the next cursor
                return Poll::Pending;
            }
            Ok(Some(block_id)) => block_id,
        };

        let is_after_finalized_block = current_cursor.number() > this.finalized_cursor.number();

        // don't return any more data until finalized block changes
        if is_after_finalized_block {
            return Poll::Pending;
        }
        let aggregate_result = match this.aggregator.aggregate_for_block(&current_cursor) {
            Ok(result) => result,
            Err(err) => {
                let err = FinalizedBlockStreamError::Aggregate(Box::new(err));
                let err = StreamError::internal(err);
                return Poll::Ready(Some(Err(err)));
            }
        };

        let is_live = current_cursor.number() == this.finalized_cursor.number();
        let current_cursor_cursor = current_cursor.to_cursor();
        let previous_cursor = this.previous_cursor.map(|c| c.to_cursor());
        *this.previous_cursor = Some(current_cursor);

        let message = BatchItem {
            is_live,
            stream_id: *this.stream_id,
            data_finality: DataFinality::DataStatusFinalized,
            cursor: previous_cursor,
            end_cursor: current_cursor_cursor,
            data: aggregate_result,
        };
        Poll::Ready(Some(Ok(BatchMessage::Finalized(message))))
    }
}

impl<R, C, L, CE, LE> BatchDataStreamExt for FinalizedBlockStream<R, C, L, CE, LE>
where
    R: StorageReader,
    C: Stream<Item = Result<StreamDataRequest, CE>>,
    L: Stream<Item = Result<IngestionMessage, LE>>,
    CE: std::error::Error + Send + Sync + 'static,
    LE: std::error::Error + Send + Sync + 'static,
{
    fn batch(
        self,
        batch_size: usize,
        interval: std::time::Duration,
    ) -> super::batch::BatchDataStream<Self>
    where
        Self: Stream<Item = Result<BatchMessage, StreamError>> + Sized,
    {
        BatchDataStream::new(self, batch_size, interval)
    }
}
