//! Ingest accepted block data.
use std::sync::Arc;

use apibara_node::db::libmdbx::EnvironmentKind;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::{
    core::GlobalBlockId,
    provider::{BlockId, Provider},
};

use super::{
    config::BlockIngestionConfig, downloader::Downloader, error::BlockIngestionError,
    storage::IngestionStorage,
};

pub struct AcceptedBlockIngestion<G: Provider + Send, E: EnvironmentKind> {
    config: BlockIngestionConfig,
    provider: Arc<G>,
    downloader: Downloader<G>,
    storage: IngestionStorage<E>,
}

impl<G, E> AcceptedBlockIngestion<G, E>
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
        AcceptedBlockIngestion {
            config,
            provider,
            storage,
            downloader,
        }
    }

    pub async fn start(
        self,
        latest_indexed: GlobalBlockId,
        ct: CancellationToken,
    ) -> Result<(), BlockIngestionError> {
        info!(
            latest_indexed = %latest_indexed,
            "start ingesting accepted blocks"
        );

        // TODO: check if block was reorged while offline.

        let mut current_head = self.refresh_head().await?;

        debug!(
            current_head = %current_head,
            "got current head"
        );

        // if we're in the accepted block ingestion, there must be at least one finalized block
        let mut finalized_block = self
            .storage
            .latest_finalized_block()?
            .ok_or(BlockIngestionError::InconsistentDatabase)?;

        debug!(
            finalized_block = %finalized_block,
            "got finalized block"
        );

        let mut previous_block = latest_indexed;
        let mut current_block_number = latest_indexed.number() + 1;

        loop {
            if ct.is_cancelled() {
                return Ok(());
            }

            if current_block_number >= current_head.number() {
                let new_current_head = self.refresh_head().await?;
                if new_current_head != current_head {
                    current_head = new_current_head;
                    info!(current_head = %current_head, "refreshed head");

                    while let Some(new_finalized_block) = self
                        .refresh_finalized_block_status(finalized_block.number() + 1)
                        .await?
                    {
                        info!(
                            block_id = %new_finalized_block,
                            "refreshed finalized block"
                        );
                        finalized_block = new_finalized_block
                    }
                    continue;
                }
                let sleep = tokio::time::sleep(self.config.head_refresh_interval);
                tokio::select! {
                    _ = sleep => {},
                    _ = ct.cancelled() => {}
                }
                continue;
            }

            let (current_block_id, parent_block_id) =
                self.ingest_block_by_number(current_block_number).await?;

            // looks like a reorg?
            if parent_block_id != previous_block {
                todo!()
            }

            self.storage.update_canonical_block(&current_block_id)?;

            info!(
                block_id = %current_block_id,
                head = %current_head,
                "ingested accepted"
            );

            current_block_number += 1;
            previous_block = current_block_id;
        }
    }

    #[tracing::instrument(skip(self))]
    async fn refresh_head(&self) -> Result<GlobalBlockId, BlockIngestionError> {
        let current_head = self
            .provider
            .get_head()
            .await
            .map_err(BlockIngestionError::provider)?;
        Ok(current_head)
    }

    #[tracing::instrument(skip(self))]
    async fn refresh_finalized_block_status(
        &self,
        number: u64,
    ) -> Result<Option<GlobalBlockId>, BlockIngestionError> {
        let global_id = self.storage.canonical_block_id_by_number(number)?;
        let block_id = BlockId::Hash(*global_id.hash());
        let block = self
            .provider
            .get_block(&block_id)
            .await
            .map_err(BlockIngestionError::provider)?;

        if !block.status().is_finalized() {
            return Ok(None);
        }

        let mut txn = self.storage.begin_txn()?;
        txn.write_status(&global_id, block.status())?;
        txn.commit()?;
        Ok(Some(global_id))
    }

    #[tracing::instrument(skip(self))]
    async fn ingest_block_by_number(
        &self,
        number: u64,
    ) -> Result<(GlobalBlockId, GlobalBlockId), BlockIngestionError> {
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

        // extract parent id
        let header = block
            .header
            .as_ref()
            .ok_or(BlockIngestionError::MissingBlockHeader)?;
        let parent_hash = header
            .parent_block_hash
            .as_ref()
            .ok_or(BlockIngestionError::MissingBlockHash)?
            .try_into()?;
        let parent_id = GlobalBlockId::new(header.block_number - 1, parent_hash);

        // write block data to storage
        let mut txn = self.storage.begin_txn()?;
        self.downloader
            .finish_ingesting_block(&global_id, block, &mut txn)
            .await?;
        txn.commit()?;

        Ok((global_id, parent_id))
    }
}
