//! Stream finalized data.

use std::{sync::Arc, task::Poll};

use futures::Stream;
use pin_project::pin_project;
use tracing::debug;

use crate::{
    core::{pb::starknet::v1alpha2, GlobalBlockId, IngestionMessage, InvalidBlockHashSize},
    db::StorageReader,
};

use super::{
    batch::{BatchDataStream, BatchItem, BatchMessage},
    BatchDataStreamExt, BlockDataAggregator, DatabaseBlockDataAggregator,
};

#[pin_project]
pub struct FinalizedBlockStream<R, L, LE>
where
    R: StorageReader,
    L: Stream<Item = Result<IngestionMessage, LE>>,
    LE: std::error::Error,
{
    current_cursor: Option<GlobalBlockId>,
    finalized_cursor: GlobalBlockId,
    storage: Arc<R>,
    aggregator: DatabaseBlockDataAggregator<R>,
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
}

impl<R, L, LE> FinalizedBlockStream<R, L, LE>
where
    R: StorageReader,
    L: Stream<Item = Result<IngestionMessage, LE>>,
    LE: std::error::Error,
{
    pub fn new(
        starting_cursor: GlobalBlockId,
        finalized_cursor: GlobalBlockId,
        filter: v1alpha2::Filter,
        storage: Arc<R>,
        ingestion: L,
    ) -> Result<Self, FinalizedBlockStreamError> {
        let aggregator = DatabaseBlockDataAggregator::new(storage.clone(), filter);
        Ok(FinalizedBlockStream {
            current_cursor: Some(starting_cursor),
            finalized_cursor,
            storage,
            aggregator,
            ingestion,
        })
    }
}

impl<R, L, LE> Stream for FinalizedBlockStream<R, L, LE>
where
    R: StorageReader,
    L: Stream<Item = Result<IngestionMessage, LE>>,
    LE: std::error::Error + Send + Sync + 'static,
{
    type Item = Result<BatchMessage, FinalizedBlockStreamError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // refresh current finalized block from ingestion events
        let this = self.project();
        match this.ingestion.poll_next(cx) {
            Poll::Pending => {}
            Poll::Ready(None) => {
                return Poll::Ready(Some(Err(FinalizedBlockStreamError::IngestionStreamStopped)))
            }
            Poll::Ready(Some(Err(err))) => {
                let err = FinalizedBlockStreamError::IngestionStream(Box::new(err));
                return Poll::Ready(Some(Err(err)));
            }
            Poll::Ready(Some(Ok(message))) => match message {
                IngestionMessage::Finalized(block_id) => {
                    debug!(block_id = %block_id, "got new finalized block");
                    *this.finalized_cursor = block_id;
                    cx.waker().wake_by_ref();
                }
                _ => {
                    cx.waker().wake_by_ref();
                }
            },
        }

        let current_cursor = match this.current_cursor {
            None => {
                // stream sent data up to the current finalized cursor.
                // the cursor for the next block is still unknown, so sleep
                // until it receives a message from the finalized stream.
                return Poll::Pending;
            }
            Some(cursor) => cursor,
        };

        let is_after_finalized_block = current_cursor.number() > this.finalized_cursor.number();

        // don't return any more data until finalized block changes
        if is_after_finalized_block {
            return Poll::Pending;
        }

        let aggregate_result = match this.aggregator.aggregate_for_block(current_cursor) {
            Ok(result) => result,
            Err(err) => {
                let err = FinalizedBlockStreamError::Aggregate(Box::new(err));
                return Poll::Ready(Some(Err(err)));
            }
        };

        let is_live = current_cursor.number() == this.finalized_cursor.number();
        let current_cursor = current_cursor.to_cursor();
        *this.current_cursor = aggregate_result.next;

        let message = BatchItem {
            is_live,
            cursor: current_cursor,
            data: aggregate_result.data,
        };
        Poll::Ready(Some(Ok(BatchMessage::Finalized(message))))
    }
}

impl<R, L, LE> BatchDataStreamExt for FinalizedBlockStream<R, L, LE>
where
    R: StorageReader,
    L: Stream<Item = Result<IngestionMessage, LE>>,
    LE: std::error::Error + Send + Sync + 'static,
{
    fn batch<E>(
        self,
        batch_size: usize,
        interval: std::time::Duration,
    ) -> super::batch::BatchDataStream<Self, E>
    where
        Self: Stream<Item = Result<BatchMessage, E>> + Sized,
        E: std::error::Error,
    {
        BatchDataStream::new(self, batch_size, interval)
    }
}
