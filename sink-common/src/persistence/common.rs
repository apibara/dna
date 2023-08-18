use apibara_core::node::v1alpha2::Cursor;
use async_trait::async_trait;

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
#[derive(Debug, thiserror::Error)]
pub enum PersistenceClientError {
    #[error("Failed to acquire lock: {0}")]
    LockFailed(Box<dyn std::error::Error + Send + Sync>),
    #[error("Failed to release lock: {0}")]
    UnlockFailed(Box<dyn std::error::Error + Send + Sync>),
    #[error("Failed to get cursor: {0}")]
    GetCursorFailed(Box<dyn std::error::Error + Send + Sync>),
    #[error("Failed to put cursor: {0}")]
    PutCursorFailed(Box<dyn std::error::Error + Send + Sync>),
    #[error("Failed to delete cursor: {0}")]
    DeleteCursorFailed(Box<dyn std::error::Error + Send + Sync>),
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

impl PersistenceClientError {
    pub fn lock<E: std::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Self::LockFailed(Box::new(err))
    }

    pub fn unlock<E: std::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Self::UnlockFailed(Box::new(err))
    }

    pub fn get_cursor<E: std::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Self::GetCursorFailed(Box::new(err))
    }

    pub fn put_cursor<E: std::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Self::PutCursorFailed(Box::new(err))
    }

    pub fn delete_cursor<E: std::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Self::DeleteCursorFailed(Box::new(err))
    }
}
