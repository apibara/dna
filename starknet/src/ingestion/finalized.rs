//! Ingest finalized block data.
use std::{sync::Arc, time::Duration};

use apibara_node::db::libmdbx::EnvironmentKind;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::{
    core::{pb::starknet::v1alpha2, GlobalBlockId},
    db::{DatabaseStorage, StorageWriter},
    ingestion::accepted::AcceptedBlockIngestion,
    provider::{BlockId, Provider, ProviderError},
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
    RetryWithDelay(Duration),
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

        // mark the latest indexed block as finalized.
        // this is fine because if the execution is here it must have
        // checked that `latest_indexed` is finalized.
        let mut txn = self.storage.begin_txn()?;
        txn.write_status(&latest_indexed, v1alpha2::BlockStatus::AcceptedOnL1)?;
        txn.commit()?;

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
                IngestResult::RetryWithDelay(delay) => {
                    tokio::time::sleep(delay).await;
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
        let (status, header, body) = match self.provider.get_block(&block_id).await {
            Ok(result) => result,
            Err(err) if err.is_block_not_found() => {
                return Ok(IngestResult::RetryWithDelay(Duration::from_secs(60)))
            }
            Err(err) => return Err(BlockIngestionError::provider(err)),
        };

        let global_id = GlobalBlockId::from_block_header(&header)?;

        if !status.is_finalized() {
            return Ok(IngestResult::TransitionToAccepted(global_id));
        }

        let mut txn = self.storage.begin_txn()?;
        self.downloader
            .finish_ingesting_block(&global_id, status, header, body, &mut txn)
            .await?;
        txn.extend_canonical_chain(&global_id)?;
        txn.commit()?;

        info!(
            block_id = %global_id,
            "ingested finalized block"
        );

        Ok(IngestResult::Ingested(global_id))
    }
}
