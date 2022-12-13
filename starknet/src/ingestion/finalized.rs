//! Ingest finalized block data.
use std::sync::Arc;

use apibara_node::db::libmdbx::EnvironmentKind;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::{
    core::GlobalBlockId,
    db::{DatabaseStorage, StorageReader, StorageWriter},
    ingestion::accepted::AcceptedBlockIngestion,
    provider::{BlockId, Provider},
};

use super::{
    config::BlockIngestionConfig, downloader::Downloader, error::BlockIngestionError,
    subscription::IngestionStreamPublisher,
};

pub struct FinalizedBlockIngestion<G: Provider + Send, E: EnvironmentKind> {
    config: BlockIngestionConfig,
    provider: Arc<G>,
    downloader: Downloader<G>,
    storage: DatabaseStorage<E>,
    publisher: IngestionStreamPublisher,
}

#[derive(Debug)]
enum IngestResult {
    Ingested(GlobalBlockId),
    TransitionToAccepted(GlobalBlockId),
}

impl<G, E> FinalizedBlockIngestion<G, E>
where
    G: Provider + Send,
    E: EnvironmentKind,
{
    pub fn new(
        provider: Arc<G>,
        storage: DatabaseStorage<E>,
        config: BlockIngestionConfig,
        publisher: IngestionStreamPublisher,
    ) -> Self {
        let downloader = Downloader::new(provider.clone(), config.rpc_concurrency);
        FinalizedBlockIngestion {
            config,
            provider,
            storage,
            downloader,
            publisher,
        }
    }

    pub async fn start(
        self,
        latest_indexed: GlobalBlockId,
        ct: CancellationToken,
    ) -> Result<(), BlockIngestionError> {
        info!(
            latest_indexed = %latest_indexed,
            "start ingesting finalized blocks"
        );

        let mut current_block = latest_indexed;

        let latest_indexed = loop {
            if ct.is_cancelled() {
                return Ok(());
            }

            let next_block_number = current_block.number() + 1;
            match self.ingest_block_by_number(next_block_number).await? {
                IngestResult::Ingested(global_id) => {
                    self.publisher.publish_finalized(global_id)?;
                    current_block = global_id;
                }
                IngestResult::TransitionToAccepted(global_id) => {
                    info!(
                        block_id = %global_id,
                        "transition to ingest accepted"
                    );
                    break current_block;
                }
            }
        };

        AcceptedBlockIngestion::new(self.provider, self.storage, self.config, self.publisher)
            .start(latest_indexed, ct)
            .await
    }

    #[tracing::instrument(skip(self))]
    async fn ingest_block_by_number(
        &self,
        number: u64,
    ) -> Result<IngestResult, BlockIngestionError> {
        debug!(
            block_number = %number,
            "ingest block by number"
        );
        let block_id = BlockId::Number(number);
        let block = self
            .provider
            .get_block(&block_id)
            .await
            .map_err(BlockIngestionError::provider)?;

        let global_id = GlobalBlockId::from_block(&block)?;

        if !block.status().is_finalized() {
            return Ok(IngestResult::TransitionToAccepted(global_id));
        }

        let mut txn = self.storage.begin_txn()?;
        self.downloader
            .finish_ingesting_block(&global_id, block, &mut txn)
            .await?;
        txn.update_canonical_chain(&global_id)?;
        txn.commit()?;

        info!(
            block_id = %global_id,
            "ingested finalized block"
        );

        Ok(IngestResult::Ingested(global_id))
    }
}
