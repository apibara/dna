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
    ct: CancellationToken,
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
        // gracefully shutdown
        if self.ct.is_cancelled() {
            return Poll::Ready(None);
        }

        todo!()
    }
}
