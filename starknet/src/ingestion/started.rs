//! First step of block ingestion.
use std::{error::Error, sync::Arc, time::Duration};

use apibara_node::db::{
    libmdbx::{Environment, EnvironmentKind, Error as MdbxError},
    MdbxRWTransactionExt,
};
use futures::{stream, StreamExt, TryFutureExt};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::{
    core::{pb::v1alpha2, BlockHash, GlobalBlockId},
    db::tables,
    ingestion::finalized::FinalizedBlockIngestion,
    provider::{BlockId, Provider},
};

use super::{downloader::Downloader, error::BlockIngestionError, storage::IngestionStorage};

pub struct StartedBlockIngestion<G: Provider + Send, E: EnvironmentKind> {
    provider: Arc<G>,
    downloader: Downloader<G>,
    storage: IngestionStorage<E>,
}

impl<G, E> StartedBlockIngestion<G, E>
where
    G: Provider + Send,
    E: EnvironmentKind,
{
    pub fn new(provider: Arc<G>, storage: IngestionStorage<E>, receipt_concurrency: usize) -> Self {
        let downloader = Downloader::new(provider.clone(), receipt_concurrency);
        StartedBlockIngestion {
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

        self.into_finalized_block_ingestion()
            .start(latest_indexed, ct)
            .await
    }

    fn into_finalized_block_ingestion(self) -> FinalizedBlockIngestion<G, E> {
        FinalizedBlockIngestion::new(self.provider, self.storage, self.downloader)
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
