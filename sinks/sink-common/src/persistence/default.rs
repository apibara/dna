use async_trait::async_trait;
use error_stack::Result;

use crate::{PersistedState, SinkError};

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

    async fn get_state(&mut self) -> Result<PersistedState, SinkError> {
        Ok(PersistedState::default())
    }

    async fn put_state(&mut self, _state: PersistedState) -> Result<(), SinkError> {
        Ok(())
    }

    async fn delete_state(&mut self) -> Result<(), SinkError> {
        Ok(())
    }
}
