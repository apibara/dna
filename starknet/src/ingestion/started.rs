//! First step of block ingestion.
use std::sync::Arc;

use apibara_node::db::libmdbx::EnvironmentKind;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    core::GlobalBlockId,
    ingestion::finalized::FinalizedBlockIngestion,
    provider::{BlockId, Provider},
};

use super::{
    accepted::AcceptedBlockIngestion, config::BlockIngestionConfig, downloader::Downloader,
    error::BlockIngestionError, storage::IngestionStorage,
};

pub struct StartedBlockIngestion<G: Provider + Send, E: EnvironmentKind> {
    config: BlockIngestionConfig,
    provider: Arc<G>,
    downloader: Downloader<G>,
    storage: IngestionStorage<E>,
}

impl<G, E> StartedBlockIngestion<G, E>
where
    G: Provider + Send,
    E: EnvironmentKind,
{
    pub fn new(
        provider: Arc<G>,
        storage: IngestionStorage<E>,
        config: BlockIngestionConfig,
    ) -> Self {
        let downloader = Downloader::new(provider.clone(), config.rpc_concurrency);
        StartedBlockIngestion {
            config,
            provider,
            storage,
            downloader,
        }
    }

    pub async fn start(self, ct: CancellationToken) -> Result<(), BlockIngestionError> {
        let latest_indexed = match self.storage.latest_indexed_block()? {
            Some(block) => block,
            None => self.ingest_genesis_block().await?,
        };

        info!(
            id = %latest_indexed,
            "latest indexed block"
        );

        // check if should jump to accepted ingestion directly based
        // on the status of the latest indexed block.
        if self.is_block_accepted(&latest_indexed).await? {
            self.into_accepted_block_ingestion()
                .start(latest_indexed, ct)
                .await
        } else {
            self.into_finalized_block_ingestion()
                .start(latest_indexed, ct)
                .await
        }
    }

    fn into_accepted_block_ingestion(self) -> AcceptedBlockIngestion<G, E> {
        AcceptedBlockIngestion::new(self.provider, self.storage, self.config)
    }

    fn into_finalized_block_ingestion(self) -> FinalizedBlockIngestion<G, E> {
        FinalizedBlockIngestion::new(self.provider, self.storage, self.config)
    }

    async fn is_block_accepted(
        &self,
        global_id: &GlobalBlockId,
    ) -> Result<bool, BlockIngestionError> {
        let block_id = BlockId::Hash(*global_id.hash());
        let block = self
            .provider
            .get_block(&block_id)
            .await
            .map_err(BlockIngestionError::provider)?;

        // blocks can be either finalized (accepted on l1)), accepted (on l2) or aborted.
        // if a block was aborted, then it was not finalized
        Ok(!block.status().is_finalized())
    }

    #[tracing::instrument(skip(self))]
    async fn ingest_genesis_block(&self) -> Result<GlobalBlockId, BlockIngestionError> {
        info!("ingest genesis block");
        let block_id = BlockId::Number(0);
        let block = self
            .provider
            .get_block(&block_id)
            .await
            .map_err(BlockIngestionError::provider)?;

        let global_id = GlobalBlockId::from_block(&block)?;
        info!(id = %global_id, "genesis block");

        let mut txn = self.storage.begin_txn()?;
        self.downloader
            .finish_ingesting_block(&global_id, block, &mut txn)
            .await?;
        txn.commit()?;
        self.storage.update_canonical_block(&global_id)?;
        Ok(global_id)
    }
}
