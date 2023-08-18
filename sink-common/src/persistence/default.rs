use apibara_core::node::v1alpha2::Cursor;
use async_trait::async_trait;

use super::common::{PersistenceClient, PersistenceClientError};

/// A [PersistenceClient] that does not persist anything.
pub struct NoPersistence;

#[async_trait]
impl PersistenceClient for NoPersistence {
    async fn lock(&mut self) -> Result<(), PersistenceClientError> {
        Ok(())
    }

    async fn unlock(&mut self) -> Result<(), PersistenceClientError> {
        Ok(())
    }

    async fn get_cursor(&mut self) -> Result<Option<Cursor>, PersistenceClientError> {
        Ok(None)
    }

    async fn put_cursor(&mut self, _cursor: Cursor) -> Result<(), PersistenceClientError> {
        Ok(())
    }

    async fn delete_cursor(&mut self) -> Result<(), PersistenceClientError> {
        Ok(())
    }
}
