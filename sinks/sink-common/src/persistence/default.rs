use apibara_dna_protocol::dna::common::Cursor;
use async_trait::async_trait;
use error_stack::{Result, ResultExt};

use crate::{filter::Filter, PersistedState, SinkError};

use super::common::PersistenceClient;

/// A [PersistenceClient] that does not persist anything.
#[derive(Default)]
pub struct InMemoryPersistence {
    cursor: Option<Cursor>,
    serialized_filter: Option<Vec<u8>>,
}

#[async_trait]
impl PersistenceClient for InMemoryPersistence {
    async fn lock(&mut self) -> Result<(), SinkError> {
        Ok(())
    }

    async fn unlock(&mut self) -> Result<(), SinkError> {
        Ok(())
    }

    async fn get_state<F: Filter>(&mut self) -> Result<PersistedState<F>, SinkError> {
        let filter = if let Some(filter) = &self.serialized_filter {
            let filter = F::decode(filter.as_slice())
                .change_context(SinkError::Fatal)
                .attach_printable("failed to deserialize in memory state")?;
            Some(filter)
        } else {
            None
        };
        let cursor = self.cursor.clone();
        Ok(PersistedState { cursor, filter })
    }

    async fn put_state<F: Filter>(&mut self, state: PersistedState<F>) -> Result<(), SinkError> {
        self.cursor = state.cursor;
        self.serialized_filter = state.filter.map(|f| f.encode_to_vec());
        Ok(())
    }

    async fn delete_state(&mut self) -> Result<(), SinkError> {
        self.cursor = None;
        self.serialized_filter = None;
        Ok(())
    }
}
