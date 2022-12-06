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
        let current_head = self
            .provider
            .get_head()
            .map_err(BlockIngestionError::provider)
            .await?;

        let latest_indexed = match self.storage.latest_indexed_block()? {
            Some(block) => block,
            None => self.ingest_genesis_block(ct.clone()).await?,
        };

        info!(
            id = %latest_indexed,
            "latest indexed block"
        );

        todo!()
    }

    #[tracing::instrument(skip(self, ct))]
    async fn ingest_genesis_block(
        &self,
        ct: CancellationToken,
    ) -> Result<GlobalBlockId, BlockIngestionError> {
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

    #[tracing::instrument(skip(self, ct))]
    async fn ingest_block_by_number(
        &self,
        block_number: u64,
        ct: CancellationToken,
    ) -> Result<(), BlockIngestionError> {
        let block_id = BlockId::Number(block_number);
        let block = self
            .provider
            .get_block(&block_id)
            .await
            .map_err(|err| BlockIngestionError::Provider(Box::new(err)))?;

        // check block contains header and hash.
        let block_header = block
            .header
            .ok_or(BlockIngestionError::MissingBlockHeader)?;
        let block_hash = block_header
            .block_hash
            .ok_or(BlockIngestionError::MissingBlockHash)?;

        // store block header
        // store block body (transactions)
        // retrieve and store block receipts

        todo!()
    }
}
