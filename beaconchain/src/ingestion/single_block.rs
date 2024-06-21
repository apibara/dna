use alloy_eips::eip2718::Decodable2718;
use apibara_dna_common::{
    core::Cursor,
    ingestion::{BlockifierError, SingleBlockIngestion},
    storage::{block_prefix, LocalStorageBackend, StorageBackend, BLOCK_NAME},
};
use error_stack::{Result, ResultExt};
use tokio::{io::AsyncWriteExt, sync::Mutex};
use tracing::debug;

use crate::segment::store;

use super::{models, BeaconApiError, BeaconApiProvider, BeaconResponseExt, BlockId};

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

        debug!(slot = cursor.number, size = bytes.len(), "writing block");
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
        let mut block = self
            .provider
            .get_block(block_id.clone())
            .await
            .change_context(BeaconChainBlockIngestionError::Api)
            .attach_printable("failed to get block")
            .attach_printable_lazy(|| format!("slot: {cursor}"))?;

        // Move out transactions since they're stored separately.
        let transactions =
            std::mem::take(&mut block.data.message.body.execution_payload.transactions);

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

        let header = store::BlockHeader::from(block.data.message);

        let transactions = transactions
            .into_iter()
            .map(|b| decode_transaction(&b))
            .collect::<Result<Vec<_>, _>>()?;

        let blobs = blob_sidecars
            .data
            .into_iter()
            .map(store::Blob::from)
            .collect::<Vec<_>>();

        let validators = validators
            .data
            .into_iter()
            .map(store::Validator::from)
            .collect::<Vec<_>>();

        debug!(
            slot = cursor.number,
            transactions = transactions.len(),
            blobs = blobs.len(),
            validators = validators.len(),
            "ingested proposed block"
        );

        Ok(store::SingleBlock {
            header,
            transactions,
            blobs,
            validators,
        })
    }
}

#[async_trait::async_trait]
impl SingleBlockIngestion for BeaconChainBlockIngestion {
    async fn ingest_block(&self, cursor: Cursor) -> Result<Cursor, BlockifierError> {
        let response = self.provider.get_header(BlockId::Slot(cursor.number)).await;

        match response {
            Ok(response) => {
                let cursor = response.cursor();
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
pub fn decode_transaction(
    bytes: &[u8],
) -> Result<store::Transaction, BeaconChainBlockIngestionError> {
    let tx = models::TxEnvelope::network_decode(&mut bytes.as_ref())
        .change_context(BeaconChainBlockIngestionError::Serialization)
        .attach_printable("failed to decode EIP 2718 transaction")?;
    let tx = store::Transaction::try_from(tx)
        .change_context(BeaconChainBlockIngestionError::Serialization)?;
    Ok(tx)
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
