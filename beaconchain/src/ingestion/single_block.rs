use apibara_dna_common::{
    core::Cursor,
    ingestion::{BlockifierError, SingleBlockIngestion},
    storage::{block_prefix, LocalStorageBackend, StorageBackend, BLOCK_NAME},
};
use error_stack::{Result, ResultExt};
use tokio::{io::AsyncWriteExt, sync::Mutex};
use tracing::debug;

use crate::segment::store;

use super::{BeaconApiError, BeaconApiProvider, BeaconResponseExt, BlockId};

#[derive(Debug)]
pub enum BeaconChainBlockIngestionError {
    Api,
    Serialization,
    Storage,
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
        debug!(slot = ?cursor, "ingesting proposed block");
        let block = self.download_proposed_block(cursor).await?;
        self.write_block(cursor, store::Slot::Proposed(block)).await
    }

    async fn ingest_missed_block(
        &self,
        cursor: &Cursor,
    ) -> Result<(), BeaconChainBlockIngestionError> {
        debug!(slot = cursor.number, "ingesting missed block");
        let block = store::Slot::<store::SingleBlock>::Missed;
        self.write_block(cursor, block).await
    }

    async fn write_block(
        &self,
        cursor: &Cursor,
        block: store::Slot<store::SingleBlock>,
    ) -> Result<(), BeaconChainBlockIngestionError> {
        let bytes = rkyv::to_bytes::<_, 0>(&block)
            .change_context(BeaconChainBlockIngestionError::Serialization)
            .attach_printable("failed to serialize block")?;

        let mut storage = self.storage.lock().await;

        let mut writer = storage
            .put(block_prefix(cursor), BLOCK_NAME)
            .await
            .change_context(BeaconChainBlockIngestionError::Storage)?;

        writer
            .write_all(&bytes)
            .await
            .change_context(BeaconChainBlockIngestionError::Storage)
            .attach_printable("failed to write block")
            .attach_printable_lazy(|| format!("slot: {cursor}"))?;

        writer
            .shutdown()
            .await
            .change_context(BeaconChainBlockIngestionError::Storage)?;

        Ok(())
    }

    async fn download_proposed_block(
        &self,
        cursor: &Cursor,
    ) -> Result<store::SingleBlock, BeaconChainBlockIngestionError> {
        let block_id = BlockId::Slot(cursor.number);
        let block = self
            .provider
            .get_block(block_id.clone())
            .await
            .change_context(BeaconChainBlockIngestionError::Api)
            .attach_printable("failed to get block")
            .attach_printable_lazy(|| format!("slot: {cursor}"))?;

        let blob_sidecars = self
            .provider
            .get_blob_sidecar(block_id.clone())
            .await
            .change_context(BeaconChainBlockIngestionError::Api)
            .attach_printable("failed to get blob sidecar")
            .attach_printable_lazy(|| format!("slot: {cursor}"))?;

        let validators = self
            .provider
            .get_validators(block_id)
            .await
            .change_context(BeaconChainBlockIngestionError::Api)
            .attach_printable("failed to get validators")
            .attach_printable_lazy(|| format!("slot: {cursor}"))?;

        println!("{:?}", validators);
        todo!();
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
            BeaconChainBlockIngestionError::Serialization => write!(f, "Serialization error"),
            BeaconChainBlockIngestionError::Storage => write!(f, "Storage error"),
        }
    }
}
