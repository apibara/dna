//! Ingest accepted block data.
use std::{sync::Arc, time::Duration};

use apibara_node::db::libmdbx::EnvironmentKind;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn};

use crate::{
    core::GlobalBlockId,
    db::{DatabaseStorage, StorageReader, StorageWriter},
    provider::{BlockId, Provider, ProviderError},
};

use super::{
    config::BlockIngestionConfig, downloader::Downloader, error::BlockIngestionError,
    subscription::IngestionStreamPublisher,
};

pub struct AcceptedBlockIngestion<G: Provider + Send, E: EnvironmentKind> {
    config: BlockIngestionConfig,
    provider: Arc<G>,
    downloader: Downloader<G>,
    storage: DatabaseStorage<E>,
    publisher: IngestionStreamPublisher,
}

struct AcceptedBlockIngestionImpl<G: Provider + Send, E: EnvironmentKind> {
    finalized: Option<GlobalBlockId>,
    previous: GlobalBlockId,
    current_head: GlobalBlockId,
    previous_pending_body_size: usize,
    config: BlockIngestionConfig,
    provider: Arc<G>,
    downloader: Downloader<G>,
    storage: DatabaseStorage<E>,
    publisher: IngestionStreamPublisher,
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
        storage: DatabaseStorage<E>,
        config: BlockIngestionConfig,
        publisher: IngestionStreamPublisher,
    ) -> Self {
        let downloader = Downloader::new(provider.clone(), config.rpc_concurrency);
        AcceptedBlockIngestion {
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
            "start ingesting accepted blocks"
        );

        let current_head = self
            .provider
            .get_head()
            .await
            .map_err(BlockIngestionError::provider)?;

        let finalized = self.storage.highest_finalized_block()?;

        let ingestion = AcceptedBlockIngestionImpl {
            current_head,
            finalized,
            previous: latest_indexed,
            previous_pending_body_size: 0,
            config: self.config,
            provider: self.provider,
            storage: self.storage,
            downloader: self.downloader,
            publisher: self.publisher,
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
            value.head_height = self.current_head.number(),
            value.finalized_height = self.finalized.map(|b| b.number()),
            head = %self.current_head,
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

        let is_synced = new_head == self.current_head;
        debug!(
            new_head = ?new_head,
            current_head = ?self.current_head,
            "check head"
        );

        // synced, so now keep polling pending block.
        if is_synced {
            self.ingest_pending().await?;
            return Ok(TickResult::FullySynced);
        }

        // only advance finalized blocks state when head changes.
        // this is to avoid fetching the same block too often.
        self.advance_finalized().await?;

        // Reset the pending body size to 0, since we are now fetching a new block.
        self.previous_pending_body_size = 0;
        self.current_head = new_head;
        Ok(TickResult::MoreToSync)
    }

    #[tracing::instrument(skip(self))]
    async fn update_accepted(&mut self) -> Result<TickResult, BlockIngestionError> {
        // if either type 1 or type 2 chain reorganization happened, simply
        // shrink the current chain up to the divergence and then continue
        // as if still catching up.

        if self.previous.number() >= self.current_head.number() {
            // type 1 reorg
            // there was a chain reorg that caused the chain to shrink.
            // this can and does happen on starknet.
            //
            // this in handled separately because querying for a block number
            // that's outside the current chain range will result in an error.
            return self.shrink_diverging_chain().await;
        }

        // fetch block following the one fetched in the previous iteration.
        // then check if the new block's parent id is the previous block id.
        // if that's not the case, then a reorg happened and we need to recover
        // from that.
        let ingest_result = self
            .ingest_block_by_number(self.previous.number() + 1)
            .await?;

        if ingest_result.parent_id == self.previous {
            // update canonical chain and notify subscribers
            let mut txn = self.storage.begin_txn()?;
            txn.extend_canonical_chain(&ingest_result.new_block_id)?;
            txn.commit()?;

            self.publisher
                .publish_accepted(ingest_result.new_block_id)?;
            self.previous = ingest_result.new_block_id;
            Ok(TickResult::MoreToSync)
        } else {
            // type 2 reorg
            // block exists but it belongs to a different (now canonical) chain.
            self.shrink_diverging_chain().await
        }
    }

    #[tracing::instrument(skip(self))]
    async fn advance_finalized(&mut self) -> Result<(), BlockIngestionError> {
        while let Some(new_finalized) = self
            .refresh_finalized_block_status(self.finalized.map(|b| b.number() + 1).unwrap_or(0))
            .await?
        {
            self.finalized = Some(new_finalized);
            info!(
                finalized = %new_finalized,
                "updated finalized block"
            );
        }

        if let Some(finalized) = self.finalized {
            self.publisher.publish_finalized(finalized)?;
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn ingest_pending(&mut self) -> Result<(), BlockIngestionError> {
        // some node configurations don't support pending data.
        // in that case, simply ignore any error.
        debug!("ingest pending block");

        let Some((status, mut header, body)) =
            self.provider.get_maybe_block(&BlockId::Pending).await
        else {
            return Ok(());
        };

        // pending block is not what was expected, do nothing.
        let is_next_pending_block = if let Some(hash) = header.parent_block_hash.as_ref() {
            *self.current_head.hash() == hash.into()
        } else {
            false
        };

        if !is_next_pending_block {
            return Ok(());
        }

        let num_txs = body.transactions.len();
        // Use number of transactions as quick way to check if the pending block
        // changed.
        // Only re-fetch pending block data if it changed.
        trace!(
            current_size = %num_txs,
            previous_size = %self.previous_pending_body_size,
            "check if new pending block"
        );

        if num_txs > self.previous_pending_body_size {
            // block number is not set, so do it here.
            header.block_number = self.current_head.number() + 1;

            // finish ingesting data.
            let new_block_id = GlobalBlockId::from_block_header(&header)?;
            let mut txn = self.storage.begin_txn()?;
            self.downloader
                .finish_ingesting_block(&new_block_id, status, header, body, &mut txn)
                .await?;
            txn.commit()?;

            self.previous_pending_body_size = num_txs;
            self.publisher.publish_pending(new_block_id)?;
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn refresh_finalized_block_status(
        &self,
        number: u64,
    ) -> Result<Option<GlobalBlockId>, BlockIngestionError> {
        let global_id = self
            .storage
            .canonical_block_id(number)?
            .ok_or(BlockIngestionError::InconsistentDatabase)?;
        let block_id = BlockId::Hash(*global_id.hash());
        let (status, _header, _body) = self
            .provider
            .get_block(&block_id)
            .await
            .map_err(BlockIngestionError::provider)?;

        if !status.is_finalized() {
            return Ok(None);
        }

        let mut txn = self.storage.begin_txn()?;
        txn.write_status(&global_id, status)?;
        txn.commit()?;
        Ok(Some(global_id))
    }

    #[tracing::instrument(skip(self), err(Debug))]
    async fn ingest_block_by_number(
        &self,
        number: u64,
    ) -> Result<IngestBlockResult, BlockIngestionError> {
        info!(
            block_number = %number,
            "ingest block by number"
        );
        let block_id = BlockId::Number(number);
        let (status, header, body) = {
            loop {
                // if the node is not fully synced it will fail to fetch the block
                // in that case, simply wait a bit and retry.
                match self.provider.get_block(&block_id).await {
                    Ok(result) => break result,
                    Err(err) if err.is_block_not_found() => {
                        warn!("node is not fully synced");
                        tokio::time::sleep(Duration::from_secs(30)).await;
                    }
                    Err(err) => return Err(BlockIngestionError::provider(err)),
                }
            }
        };

        let new_block_id = GlobalBlockId::from_block_header(&header)?;

        // extract parent id
        let parent_hash = header
            .parent_block_hash
            .as_ref()
            .ok_or(BlockIngestionError::MissingBlockHash)?
            .into();
        let parent_id = GlobalBlockId::new(header.block_number - 1, parent_hash);

        // write block data to storage
        let mut txn = self.storage.begin_txn()?;
        self.downloader
            .finish_ingesting_block(&new_block_id, status, header, body, &mut txn)
            .await?;
        txn.commit()?;

        info!(
            block_id = %new_block_id,
            "ingested accepted block"
        );

        Ok(IngestBlockResult {
            new_block_id,
            parent_id,
        })
    }

    /// Shrink the old canonical chain until it joins with the new canonical chain.
    #[tracing::instrument(skip(self))]
    async fn shrink_diverging_chain(&mut self) -> Result<TickResult, BlockIngestionError> {
        info!(
            previous = %self.previous,
            current = %self.current_head,
            "shrinking canonical chain"
        );

        let mut txn = self.storage.begin_txn()?;
        let mut ingested_tip = self.previous;

        loop {
            let belongs_to_new_canonical_chain =
                if ingested_tip.number() <= self.current_head.number() {
                    // check status of the
                    let block_id = BlockId::Hash(*ingested_tip.hash());
                    let (status, _header, _body) = self
                        .provider
                        .get_block(&block_id)
                        .await
                        .map_err(BlockIngestionError::provider)?;
                    !status.is_rejected()
                } else {
                    // outside of the new chain range, it doesn't belong.
                    false
                };

            debug!(
                tip = %ingested_tip,
                belongs = %belongs_to_new_canonical_chain,
                "check if tip belongs to chain"
            );

            // finished shrinking old canonical chain
            if belongs_to_new_canonical_chain {
                break;
            }

            txn.reject_block_from_canonical_chain(&ingested_tip)?;

            // header must exist in the database
            let header = self
                .storage
                .read_header(&ingested_tip)?
                .ok_or(BlockIngestionError::InconsistentDatabase)?;

            let parent_hash = header
                .parent_block_hash
                .as_ref()
                .ok_or(BlockIngestionError::MissingBlockHash)?
                .into();

            ingested_tip = GlobalBlockId::new(header.block_number - 1, parent_hash);
        }

        txn.commit()?;

        // `ingested_tip` is the new chain root, that is the highest common block
        // between the old canonical chain and the new canonical chain.
        // restart ingestion from the new canonical chain head
        self.previous = ingested_tip;
        self.publisher.publish_invalidate(ingested_tip)?;

        Ok(TickResult::MoreToSync)
    }
}
