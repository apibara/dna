//! Stream finalized data.

use std::task::Poll;

use futures::Stream;
use pin_project::pin_project;
use tokio_util::sync::CancellationToken;

use crate::core::{GlobalBlockId, IngestionMessage};

use super::aggregate::BlockDataAggregator;

#[pin_project]
pub struct FinalizedBlockStream<A, L>
where
    A: BlockDataAggregator,
    L: Stream<Item = IngestionMessage>,
{
    current_block: GlobalBlockId,
    aggregator: A,
    #[pin]
    ingestion: L,
}

impl<A, L> FinalizedBlockStream<A, L>
where
    A: BlockDataAggregator,
    L: Stream<Item = IngestionMessage>,
{
    pub fn new(starting_block: GlobalBlockId, aggregator: A, ingestion: L) -> Self {
        FinalizedBlockStream {
            current_block: starting_block,
            aggregator,
            ingestion,
        }
    }
}

impl<A, L> Stream for FinalizedBlockStream<A, L>
where
    A: BlockDataAggregator,
    L: Stream<Item = IngestionMessage>,
{
    type Item = IngestionMessage;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
