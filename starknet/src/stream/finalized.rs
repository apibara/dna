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
    previous_cursor: Option<GlobalBlockId>,
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
    #[error(transparent)]
    Storage(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl<R, L, LE> FinalizedBlockStream<R, L, LE>
where
    R: StorageReader,
    L: Stream<Item = Result<IngestionMessage, LE>>,
    LE: std::error::Error,
{
    pub fn new(
        starting_cursor: Option<GlobalBlockId>,
        finalized_cursor: GlobalBlockId,
        filter: v1alpha2::Filter,
        storage: Arc<R>,
        ingestion: L,
    ) -> Result<Self, FinalizedBlockStreamError> {
        let aggregator = DatabaseBlockDataAggregator::new(storage.clone(), filter);
        Ok(FinalizedBlockStream {
            previous_cursor: starting_cursor,
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

        let current_cursor_block_number = this.previous_cursor.map(|c| c.number() + 1).unwrap_or(0);
        let current_cursor = match this.storage.canonical_block_id(current_cursor_block_number) {
            Err(err) => {
                let err = FinalizedBlockStreamError::Storage(Box::new(err));
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
                return Poll::Ready(Some(Err(err)));
            }
        };

        let is_live = current_cursor.number() == this.finalized_cursor.number();
        let current_cursor_cursor = current_cursor.to_cursor();
        *this.previous_cursor = Some(current_cursor);

        let message = BatchItem {
            is_live,
            cursor: current_cursor_cursor,
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
