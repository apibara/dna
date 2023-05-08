use async_trait::async_trait;
use futures::Stream;

use super::ingestion::IngestionMessage;
use crate::core::Cursor as CursorT;

/// An object that produces cursors.
#[async_trait]
pub trait CursorProducer: Stream<Item = Result<Self::Cursor, Self::Error>> {
    type Cursor: CursorT;
    type Error;

    /// Handles an ingestion message.
    ///
    /// This handler should be used to resync the producer's current state with the chain state.
    async fn handle_ingestion_message(
        &mut self,
        message: &IngestionMessage<Self::Cursor>,
    ) -> Result<(), Self::Error>;
}

#[async_trait]
pub trait BatchProducer {
    type Cursor: CursorT;
    type Error;

    async fn next_batch(
        &mut self,
        cursors: impl Iterator<Item = Self::Cursor>,
    ) -> Result<Option<()>, Self::Error>;
}
