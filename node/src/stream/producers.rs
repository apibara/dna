use async_trait::async_trait;
use futures::Stream;
use prost::Message;

use super::{configuration::StreamConfiguration, error::StreamError, ingestion::IngestionMessage};
use crate::core::Cursor;

pub enum IngestionResponse<C: Cursor> {
    Invalidate(C),
    Ok,
}

pub enum BatchCursor<C: Cursor> {
    Finalized(Option<C>, Vec<C>),
    Accepted(Option<C>, C),
    Pending(Option<C>, C),
}

/// An object that produces cursors.
#[async_trait]
pub trait CursorProducer: Stream<Item = Result<BatchCursor<Self::Cursor>, StreamError>> {
    type Cursor: Cursor;
    type Filter: Message + Default + Clone;

    fn reconfigure(
        &mut self,
        configuration: &StreamConfiguration<Self::Cursor, Self::Filter>,
    ) -> Result<(), StreamError>;

    /// Handles an ingestion message.
    ///
    /// This handler should be used to resync the producer's current state with the chain state.
    async fn handle_ingestion_message(
        &mut self,
        message: &IngestionMessage<Self::Cursor>,
    ) -> Result<IngestionResponse<Self::Cursor>, StreamError>;
}

#[async_trait]
pub trait BatchProducer {
    type Cursor: Cursor;
    type Filter: Message + Default + Clone;
    type Block;

    fn reconfigure(
        &mut self,
        configuration: &StreamConfiguration<Self::Cursor, Self::Filter>,
    ) -> Result<(), StreamError>;

    async fn next_batch(
        &mut self,
        cursors: impl Iterator<Item = Self::Cursor> + Send + Sync,
    ) -> Result<Vec<Self::Block>, StreamError>;
}
