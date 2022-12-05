use std::{error::Error, sync::Arc, time::Duration};

use futures::{stream, StreamExt};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::{
    core::{pb::v1alpha2, BlockHash},
    provider::{BlockId, Provider},
};

#[derive(Debug, thiserror::Error)]
pub enum BlockIngestionError {
    #[error("failed to fetch provider data")]
    Provider(#[from] Box<dyn Error + Send + Sync + 'static>),
    #[error("block does not contain header")]
    MissingBlockHeader,
    #[error("block doesn't have hash")]
    MissingBlockHash,
    #[error("transaction is missing data")]
    MalformedTransaction,
}

pub struct BlockIngestion<G: Provider + Send> {
    provider: Arc<G>,
}

pub struct StartedBlockIngestion<G: Provider + Send> {
    provider: Arc<G>,
    current_head: v1alpha2::BlockHeader,
    receipt_concurrency: usize,
}

impl<G> BlockIngestion<G>
where
    G: Provider + Send,
{
    pub fn new(provider: Arc<G>) -> Self {
        BlockIngestion { provider }
    }

    pub async fn start(self, ct: CancellationToken) -> Result<(), BlockIngestionError> {
        let latest_block_id = BlockId::Latest;
        let current_head = self
            .provider
            .get_block(&latest_block_id)
            .await
            .map_err(|err| BlockIngestionError::Provider(Box::new(err)))?
            .header
            .ok_or(BlockIngestionError::MissingBlockHeader)?;

        let ingestion = StartedBlockIngestion {
            provider: self.provider,
            current_head,
            receipt_concurrency: 8,
        };
        ingestion.start(ct).await
    }
}

impl<G> StartedBlockIngestion<G>
where
    G: Provider + Send,
{
    pub async fn start(self, ct: CancellationToken) -> Result<(), BlockIngestionError> {
        info!(current_head = %self.current_head.block_number, "starting block ingestion");

        let mut block_num = 0;
        loop {
            if ct.is_cancelled() {
                return Ok(());
            }
            self.ingest_block_by_number(block_num, ct.clone()).await?;

            block_num += 1;
        }
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
        let receipts = stream::iter(block.transactions)
            .enumerate()
            .map(|(tx_idx, tx)| {
                let provider = &self.provider;
                async move {
                    let tx_hash = tx
                        .meta
                        .ok_or(BlockIngestionError::MalformedTransaction)?
                        .hash;
                    provider
                        .get_transaction_receipt(&tx_hash)
                        .await
                        .map(|mut r| {
                            // update transaction index inside a map or the type checker
                            // will complain about the closure return type.
                            r.transaction_index = tx_idx as u64;
                            r
                        })
                        .map_err(|err| BlockIngestionError::Provider(Box::new(err)))
                }
            })
            .buffer_unordered(self.receipt_concurrency);

        let receipts = receipts
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, BlockIngestionError>>()?;

        info!(
            block_number = %block_number,
            block_hash = ?block_hash,
            num_receipts = %receipts.len(),
            "finished ingesting block"
        );
        Ok(())
    }
}
