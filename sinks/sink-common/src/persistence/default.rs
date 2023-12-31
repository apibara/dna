use apibara_core::node::v1alpha2::Cursor;
use async_trait::async_trait;
use error_stack::Result;

use crate::SinkError;

use super::common::PersistenceClient;

/// A [PersistenceClient] that does not persist anything.
pub struct NoPersistence;

#[async_trait]
impl PersistenceClient for NoPersistence {
    async fn lock(&mut self) -> Result<(), SinkError> {
        Ok(())
    }

    async fn unlock(&mut self) -> Result<(), SinkError> {
        Ok(())
    }

    async fn get_cursor(&mut self) -> Result<Option<Cursor>, SinkError> {
        Ok(None)
    }

    async fn put_cursor(&mut self, _cursor: Cursor) -> Result<(), SinkError> {
        Ok(())
    }

    async fn delete_cursor(&mut self) -> Result<(), SinkError> {
        Ok(())
    }
}
