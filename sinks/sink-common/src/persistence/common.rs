use apibara_core::node::v1alpha2::Cursor;
use async_trait::async_trait;
use error_stack::Result;

use crate::SinkError;

/// Client used to interact with the persistence backend.
#[async_trait]
pub trait PersistenceClient {
    /// Attempts to acquire a lock on the sink.
    async fn lock(&mut self) -> Result<(), SinkError>;

    /// Unlock the previously acquired lock.
    async fn unlock(&mut self) -> Result<(), SinkError>;

    /// Reads the currently stored cursor value.
    async fn get_cursor(&mut self) -> Result<Option<Cursor>, SinkError>;

    /// Updates the sink cursor value.
    async fn put_cursor(&mut self, cursor: Cursor) -> Result<(), SinkError>;

    /// Deletes any stored value for the sink cursor.
    async fn delete_cursor(&mut self) -> Result<(), SinkError>;
}

#[async_trait]
impl<P> PersistenceClient for Box<P>
where
    P: PersistenceClient + ?Sized + Send,
{
    async fn lock(&mut self) -> Result<(), SinkError> {
        (**self).lock().await
    }

    async fn unlock(&mut self) -> Result<(), SinkError> {
        (**self).unlock().await
    }

    async fn get_cursor(&mut self) -> Result<Option<Cursor>, SinkError> {
        (**self).get_cursor().await
    }

    async fn put_cursor(&mut self, cursor: Cursor) -> Result<(), SinkError> {
        (**self).put_cursor(cursor).await
    }

    async fn delete_cursor(&mut self) -> Result<(), SinkError> {
        (**self).delete_cursor().await
    }
}
