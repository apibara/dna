use apibara_dna_common::{
    core::Cursor,
    ingestion::{BlockifierError, SingleBlockIngestion},
    storage::{LocalStorageBackend, StorageBackend},
};
use error_stack::{Result, ResultExt};
use tokio::sync::Mutex;

use super::{BeaconApiError, BeaconApiProvider, BeaconResponseExt, BlockId};

#[derive(Debug)]
pub enum BeaconChainBlockIngestionError {
    Api,
}

pub struct BeaconChainBlockIngestion {
    provider: BeaconApiProvider,
    storage: Mutex<LocalStorageBackend>,
}

impl BeaconChainBlockIngestion {
    pub fn new(provider: BeaconApiProvider, storage: LocalStorageBackend) -> Self {
        let storage = Mutex::new(storage);
        Self { provider, storage }
    }

    async fn ingest_proposed_block(
        &self,
        cursor: &Cursor,
    ) -> Result<(), BeaconChainBlockIngestionError> {
        let mut storage = self.storage.lock().await;
        // TODO: write rest of the function
        let xxx = storage.put("mmm", "xxx").await.unwrap();
        Ok(())
    }

    async fn ingest_missed_block(
        &self,
        cursor: &Cursor,
    ) -> Result<(), BeaconChainBlockIngestionError> {
        let mut storage = self.storage.lock().await;
        // If the slot was missed the server returns not found.
        // Simply return the cursor with only the slot number.
        // TODO: should we write a "missed" block placeholder?
        let xxx = storage.put("mmm", "xxx").await.unwrap();
        Ok(())
    }
}

#[async_trait::async_trait]
impl SingleBlockIngestion for BeaconChainBlockIngestion {
    async fn ingest_block(&self, cursor: Cursor) -> Result<Cursor, BlockifierError> {
        let response = self.provider.get_header(BlockId::Slot(cursor.number)).await;

        match response {
            Ok(response) => {
                self.ingest_proposed_block(&cursor)
                    .await
                    .change_context(BlockifierError::BlockIngestion)?;
                Ok(response.cursor())
            }
            Err(err) => match err.current_context() {
                BeaconApiError::NotFound => {
                    self.ingest_missed_block(&cursor)
                        .await
                        .change_context(BlockifierError::BlockIngestion)?;
                    Ok(cursor)
                }
                _ => Err(err)
                    .change_context(BeaconChainBlockIngestionError::Api)
                    .attach_printable_lazy(|| {
                        format!("Failed to get block header for slot {}", cursor.number)
                    })
                    .change_context(BlockifierError::BlockIngestion),
            },
        }
    }
}

impl error_stack::Context for BeaconChainBlockIngestionError {}

impl std::fmt::Display for BeaconChainBlockIngestionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BeaconChainBlockIngestionError::Api => write!(f, "Beacon Chain API error"),
        }
    }
}
