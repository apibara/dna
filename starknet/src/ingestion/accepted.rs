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

struct AcceptedBlockIngestionImpl<G: Provider + Send, E: EnvironmentKind> {
    finalized: GlobalBlockId,
    previous: GlobalBlockId,
    current_head: GlobalBlockId,
    config: BlockIngestionConfig,
    provider: Arc<G>,
    downloader: Downloader<G>,
    storage: IngestionStorage<E>,
}

enum TickResult {
    FullySynced,
    MoreToSync,
}

struct IngestBlockResult {
    pub new_block_id: GlobalBlockId,
    pub parent_id: GlobalBlockId,
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

        let current_head = self
            .provider
            .get_head()
            .await
            .map_err(BlockIngestionError::provider)?;

        // if we're in the accepted block ingestion, there must be at least one finalized block
        let finalized = self
            .storage
            .latest_finalized_block()?
            .ok_or(BlockIngestionError::InconsistentDatabase)?;

        let ingestion = AcceptedBlockIngestionImpl {
            current_head,
            finalized,
            previous: latest_indexed,
            config: self.config,
            provider: self.provider,
            storage: self.storage,
            downloader: self.downloader,
        };
        ingestion.start(ct).await
    }
}

impl<G, E> AcceptedBlockIngestionImpl<G, E>
where
    G: Provider + Send,
    E: EnvironmentKind,
{
    pub async fn start(mut self, ct: CancellationToken) -> Result<(), BlockIngestionError> {
        loop {
            if ct.is_cancelled() {
                return Ok(());
            }

            match self.tick().await? {
                TickResult::MoreToSync => {}
                TickResult::FullySynced => {
                    // no need to do anything for now
                    tokio::select! {
                        _ = tokio::time::sleep(self.config.head_refresh_interval) => {},
                        _ = ct.cancelled() => {},
                    }
                }
            }
        }
    }

    /// Perform one tick in the loop that keeps the indexer up-to-date with the chain.
    ///
    /// If the indexer has not caught up with the head, then it will ingest one more
    /// block.
    /// If the indexer is up to date, it will refresh the current head. If the head
    /// has not changed, then it will signal the caller to wait. If the head changed,
    /// then the function returns signaling the caller to call it again.
    #[tracing::instrument(skip(self))]
    pub async fn tick(&mut self) -> Result<TickResult, BlockIngestionError> {
        debug!(
            current_head = %self.current_head,
            finalized = %self.finalized,
            previous = %self.previous,
            "accepted ingestion tick"
        );

        if self.previous == self.current_head {
            // this function only _updates_ the current head.
            // if the head changed, then at the next iteration we will it the
            // other branch and check how to apply the next block.
            // notice that if any of the the current head hash or block number
            // changes then comparison fails.
            self.update_current_head().await
        } else {
            self.update_accepted().await
        }
    }

    #[tracing::instrument(skip(self))]
    async fn update_current_head(&mut self) -> Result<TickResult, BlockIngestionError> {
        let new_head = self
            .provider
            .get_head()
            .await
            .map_err(BlockIngestionError::provider)?;

        if new_head == self.current_head {
            return Ok(TickResult::FullySynced);
        }

        // only advance finalized blocks state when head changes.
        // this is to avoid fetching the same block too often.
        self.advance_finalized().await?;

        self.current_head = new_head;
        Ok(TickResult::MoreToSync)
    }

    #[tracing::instrument(skip(self))]
    async fn update_accepted(&mut self) -> Result<TickResult, BlockIngestionError> {
        if self.previous.number() >= self.current_head.number() {
            // there was a chain reorg that caused the chain to shrink.
            // this can and does happen on starknet.
            //
            // this in handled separately because querying for a block number
            // that's outside the current chain range will result in an error.
            todo!()
        }

        // fetch block following the one fetched in the previous iteration.
        // then check if the new block's parent id is the previous block id.
        // if that's not the case, then a reorg happened and we need to recover
        // from that.
        let ingest_result = self
            .ingest_block_by_number(self.previous.number() + 1)
            .await?;
        if ingest_result.parent_id == self.previous {
            self.storage
                .update_canonical_block(&ingest_result.new_block_id)?;
            self.previous = ingest_result.new_block_id;
            return Ok(TickResult::MoreToSync);
        }
        todo!()
    }

    #[tracing::instrument(skip(self))]
    async fn advance_finalized(&mut self) -> Result<(), BlockIngestionError> {
        while let Some(new_finalized) = self
            .refresh_finalized_block_status(self.finalized.number() + 1)
            .await?
        {
            self.finalized = new_finalized;
            info!(
                finalized = %self.finalized,
                "updated finalized block"
            );
        }
        Ok(())
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
    ) -> Result<IngestBlockResult, BlockIngestionError> {
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

        let new_block_id = GlobalBlockId::from_block(&block)?;

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
            .finish_ingesting_block(&new_block_id, block, &mut txn)
            .await?;
        txn.commit()?;

        Ok(IngestBlockResult {
            new_block_id,
            parent_id,
        })
    }
}
