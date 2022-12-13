//! Stream finalized data.

use std::{pin::Pin, task::Poll};

use futures::Stream;
use pin_project::pin_project;
use tracing::info;

use crate::core::{
    pb::{self, starknet::v1alpha2},
    GlobalBlockId, IngestionMessage,
};

use super::aggregate::BlockDataAggregator;

#[pin_project]
pub struct FinalizedBlockStream<A, L, LE>
where
    A: BlockDataAggregator,
    L: Stream<Item = Result<IngestionMessage, LE>>,
    LE: std::error::Error,
{
    current_block: GlobalBlockId,
    highest_block: GlobalBlockId,
    aggregator: A,
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
}

impl<A, L, LE> FinalizedBlockStream<A, L, LE>
where
    A: BlockDataAggregator,
    L: Stream<Item = Result<IngestionMessage, LE>>,
    LE: std::error::Error,
{
    pub fn new(
        starting_block: GlobalBlockId,
        highest_block: GlobalBlockId,
        aggregator: A,
        ingestion: L,
    ) -> Self {
        FinalizedBlockStream {
            current_block: starting_block,
            highest_block,
            aggregator,
            ingestion,
        }
    }
}

impl<A, L, LE> Stream for FinalizedBlockStream<A, L, LE>
where
    A: BlockDataAggregator,
    L: Stream<Item = Result<IngestionMessage, LE>>,
    LE: std::error::Error + Send + Sync + 'static,
{
    type Item = Result<pb::stream::v1alpha2::Data, FinalizedBlockStreamError>;

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
                    info!(block_id = %block_id, "got new finalized block");
                    *this.highest_block = block_id;
                    cx.waker().wake_by_ref();
                }
                _ => {
                    cx.waker().wake_by_ref();
                }
            },
        }

        // don't return any more data until finalized block changes
        if this.current_block.number() > this.highest_block.number() {
            return Poll::Pending;
        }

        let (data, next_block) = match this.aggregator.aggregate_batch(
            this.current_block,
            50,
            v1alpha2::BlockStatus::AcceptedOnL1,
        ) {
            Ok(result) => result,
            Err(err) => {
                let err = FinalizedBlockStreamError::Aggregate(Box::new(err));
                return Poll::Ready(Some(Err(err)));
            }
        };

        *this.current_block = next_block;

        let data = pb::stream::v1alpha2::Data {
            end_cursor: vec![],
            finality: pb::stream::v1alpha2::DataFinality::DataStatusFinalized as i32,
            data,
        };

        Poll::Ready(Some(Ok(data)))
    }
}
