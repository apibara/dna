//! Stream finalized data.

use std::task::Poll;

use futures::Stream;
use pin_project::pin_project;
use tracing::info;

use crate::core::{pb::starknet::v1alpha2, GlobalBlockId, IngestionMessage};

use super::aggregate::BlockDataAggregator;

#[pin_project]
pub struct FinalizedBlockStream<A, L, LE>
where
    A: BlockDataAggregator,
    L: Stream<Item = Result<IngestionMessage, LE>>,
    LE: std::error::Error,
{
    current_block: GlobalBlockId,
    aggregator: A,
    #[pin]
    ingestion: L,
}

impl<A, L, LE> FinalizedBlockStream<A, L, LE>
where
    A: BlockDataAggregator,
    L: Stream<Item = Result<IngestionMessage, LE>>,
    LE: std::error::Error,
{
    pub fn new(starting_block: GlobalBlockId, aggregator: A, ingestion: L) -> Self {
        FinalizedBlockStream {
            current_block: starting_block,
            aggregator,
            ingestion,
        }
    }
}

impl<A, L, LE> Stream for FinalizedBlockStream<A, L, LE>
where
    A: BlockDataAggregator,
    L: Stream<Item = Result<IngestionMessage, LE>>,
    LE: std::error::Error,
{
    type Item = Vec<v1alpha2::Block>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut blocks = Vec::with_capacity(100);
        let mut current_block = self.current_block;
        let this = self.project();
        for _ in 0..100 {
            let block = this
                .aggregator
                .aggregate_for_block(&current_block)
                .unwrap()
                .unwrap();
            current_block = this.aggregator.next_block(&current_block).unwrap().unwrap();
            blocks.push(block)
        }
        *this.current_block = current_block;
        Poll::Ready(Some(blocks))
    }
}
