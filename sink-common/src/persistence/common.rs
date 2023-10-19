use std::fmt;

use apibara_core::node::v1alpha2::Cursor;
use async_trait::async_trait;
use error_stack::Result;

/// Client used to interact with the persistence backend.
#[async_trait]
pub trait PersistenceClient {
    /// Attempts to acquire a lock on the sink.
    async fn lock(&mut self) -> Result<(), PersistenceClientError>;

    /// Unlock the previously acquired lock.
    async fn unlock(&mut self) -> Result<(), PersistenceClientError>;

    /// Reads the currently stored cursor value.
    async fn get_cursor(&mut self) -> Result<Option<Cursor>, PersistenceClientError>;

    /// Updates the sink cursor value.
    async fn put_cursor(&mut self, cursor: Cursor) -> Result<(), PersistenceClientError>;

    /// Deletes any stored value for the sink cursor.
    async fn delete_cursor(&mut self) -> Result<(), PersistenceClientError>;
}

/// Error returned by the [PersitenceClient].
#[derive(Debug)]
pub struct PersistenceClientError;
impl error_stack::Context for PersistenceClientError {}

impl fmt::Display for PersistenceClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("persistence client operation failed")
    }
}

#[async_trait]
impl<P> PersistenceClient for Box<P>
where
    P: PersistenceClient + ?Sized + Send,
{
    async fn lock(&mut self) -> Result<(), PersistenceClientError> {
        (**self).lock().await
    }

    async fn unlock(&mut self) -> Result<(), PersistenceClientError> {
        (**self).unlock().await
    }

    async fn get_cursor(&mut self) -> Result<Option<Cursor>, PersistenceClientError> {
        (**self).get_cursor().await
    }

    async fn put_cursor(&mut self, cursor: Cursor) -> Result<(), PersistenceClientError> {
        (**self).put_cursor(cursor).await
    }

    async fn delete_cursor(&mut self) -> Result<(), PersistenceClientError> {
        (**self).delete_cursor().await
    }
}

