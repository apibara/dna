use async_trait::async_trait;
use futures::Stream;
use prost::Message;

use super::{configuration::StreamConfiguration, error::StreamError, ingestion::IngestionMessage};
use crate::core::Cursor;

/// The response to an ingestion message.
#[derive(Debug)]
pub enum IngestionResponse<C: Cursor> {
    /// Invalidate all data after the given cursor.
    Invalidate(C),
    /// No invalidation is required.
    Ok,
}

/// The response to a call to reconfigure.
#[derive(Debug)]
pub enum ReconfigureResponse<C: Cursor> {
    /// Invalidate all data after the given cursor.
    Invalidate(C),
    /// No invalidation is required.
    Ok,
    /// The specified starting cursor doesn't exists.
    MissingStartingCursor,
}

/// A batch cursor.
#[derive(Debug)]
pub enum BatchCursor<C: Cursor> {
    /// A bunch of finalized data.
    Finalized(Option<C>, Vec<C>),
    /// A single accepted cursor.
    Accepted(Option<C>, C),
    /// A single pending cursor.
    Pending(Option<C>, C),
}

/// An object that produces cursors.
#[async_trait]
pub trait CursorProducer: Stream<Item = Result<BatchCursor<Self::Cursor>, StreamError>> {
    type Cursor: Cursor;
    type Filter: Message + Default + Clone;

    /// Reconfigure the cursor producer.
    ///
    /// Since the user specifies a starting cursor, this function can signal if the specified
    /// cursor has been invalidated while the client was offline.
    async fn reconfigure(
        &mut self,
        configuration: &StreamConfiguration<Self::Cursor, Self::Filter>,
    ) -> Result<ReconfigureResponse<Self::Cursor>, StreamError>;

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

impl<C: Cursor> BatchCursor<C> {
    /// Creates a new finalized batch cursor.
    ///
    /// Panics if `cursors` is empty.
    pub fn new_finalized(start_cursor: Option<C>, cursors: Vec<C>) -> Self {
        BatchCursor::Finalized(start_cursor, cursors)
    }

    /// Creates a new accepted batch cursor.
    pub fn new_accepted(start_cursor: Option<C>, cursor: C) -> Self {
        BatchCursor::Accepted(start_cursor, cursor)
    }

    /// Creates a new pending batch cursor.
    pub fn new_pending(start_cursor: Option<C>, cursor: C) -> Self {
        BatchCursor::Pending(start_cursor, cursor)
    }

    /// Returns the start cursor, that is the cursor immediately before the first cursor in the
    /// batch.
    pub fn start_cursor(&self) -> Option<&C> {
        match self {
            BatchCursor::Finalized(start_cursor, _) => start_cursor.as_ref(),
            BatchCursor::Accepted(start_cursor, _) => start_cursor.as_ref(),
            BatchCursor::Pending(start_cursor, _) => start_cursor.as_ref(),
        }
    }

    /// Returns the last cursor in the batch.
    pub fn end_cursor(&self) -> &C {
        match self {
            BatchCursor::Finalized(_, cursors) => cursors.last().expect("empty batch"),
            BatchCursor::Accepted(_, ref cursor) => cursor,
            BatchCursor::Pending(_, ref cursor) => cursor,
        }
    }

    /// Returns the finalized cursors.
    pub fn as_finalized(&self) -> Option<&[C]> {
        match self {
            BatchCursor::Finalized(_, ref cursors) => Some(cursors),
            _ => None,
        }
    }

    /// Returns the accepted cursor.
    pub fn as_accepted(&self) -> Option<&C> {
        match self {
            BatchCursor::Accepted(_, ref cursor) => Some(cursor),
            _ => None,
        }
    }

    /// Returns the pending cursor.
    pub fn as_pending(&self) -> Option<&C> {
        match self {
            BatchCursor::Pending(_, ref cursor) => Some(cursor),
            _ => None,
        }
    }
}
