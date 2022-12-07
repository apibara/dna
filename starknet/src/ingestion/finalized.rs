//! Ingest finalized block data.
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

pub struct FinalizedBlockIngestion<G: Provider + Send, E: EnvironmentKind> {
    provider: Arc<G>,
    downloader: Downloader<G>,
    storage: IngestionStorage<E>,
}

#[derive(Debug)]
enum IngestResult {
    Ingested(GlobalBlockId),
    TransitionToAccepted,
}

impl<G, E> FinalizedBlockIngestion<G, E>
where
    G: Provider + Send,
    E: EnvironmentKind,
{
    pub fn new(provider: Arc<G>, storage: IngestionStorage<E>, downloader: Downloader<G>) -> Self {
        FinalizedBlockIngestion {
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
            "start ingesting finalized blocks"
        );

        let mut current_block = latest_indexed;

        loop {
            if ct.is_cancelled() {
                return Ok(());
            }

            let next_block_number = current_block.number() + 1;
            match self.ingest_block_by_number(next_block_number).await? {
                IngestResult::Ingested(global_id) => {
                    current_block = global_id;
                }
                IngestResult::TransitionToAccepted => {
                    todo!()
                }
            }
        }
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

        if !block.status().is_finalized() {
            return Ok(IngestResult::TransitionToAccepted);
        }

        let global_id = GlobalBlockId::from_block(&block)?;

        let mut txn = self.storage.begin_txn()?;
        self.downloader
            .finish_ingesting_block(&global_id, block, &mut txn)
            .await?;
        txn.commit()?;

        self.storage.update_canonical_block(&global_id)?;

        info!(
            block_id = %global_id,
            "ingested finalized block"
        );

        Ok(IngestResult::Ingested(global_id))
    }
}
