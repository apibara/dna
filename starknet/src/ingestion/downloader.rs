//! Download and store block data.

use std::sync::Arc;

use apibara_node::db::libmdbx::EnvironmentKind;
use futures::{stream, StreamExt, TryFutureExt};
use tracing::{debug, info};

use crate::{
    core::{pb::v1alpha2, GlobalBlockId},
    provider::{BlockId, Provider},
};

use super::{storage::BlockWriter, BlockIngestionError};

pub struct Downloader<G: Provider + Send> {
    provider: Arc<G>,
    receipt_concurrency: usize,
}

impl<G> Downloader<G>
where
    G: Provider + Send,
{
    pub fn new(provider: Arc<G>, receipt_concurrency: usize) -> Self {
        Downloader {
            provider,
            receipt_concurrency,
        }
    }

    pub async fn finish_ingesting_block<'env, 'txn, E>(
        &self,
        global_id: &GlobalBlockId,
        block: v1alpha2::Block,
        writer: &mut BlockWriter<'env, 'txn, E>,
    ) -> Result<(), BlockIngestionError>
    where
        E: EnvironmentKind,
    {
        // block already contains header, status, and body
        let status = block.status();
        let header = block
            .header
            .ok_or(BlockIngestionError::MissingBlockHeader)?;
        let transactions = block.transactions;

        // download state update, receipts
        let hashes = transactions
            .iter()
            .map(|tx| {
                let tx_hash = tx
                    .meta
                    .as_ref()
                    .ok_or(BlockIngestionError::MalformedTransaction)?
                    .hash
                    .clone();
                Ok(tx_hash)
            })
            .collect::<Result<Vec<_>, BlockIngestionError>>()?;

        let receipts = stream::iter(hashes)
            .enumerate()
            .map(|(tx_idx, tx_hash)| {
                let provider = &self.provider;
                async move {
                    provider
                        .get_transaction_receipt(&tx_hash)
                        .await
                        .map(|mut r| {
                            // update transaction index inside a map or the type checker
                            // will complain about the closure return type.
                            r.transaction_index = tx_idx as u64;
                            r
                        })
                        .map_err(BlockIngestionError::provider)
                }
            })
            .buffer_unordered(self.receipt_concurrency);

        let receipts = receipts
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, BlockIngestionError>>()?;

        let block_id = BlockId::Hash(global_id.hash().clone());
        let state_update = self
            .provider
            .get_state_update(&block_id)
            .await
            .map_err(BlockIngestionError::provider)?;

        // write block status, header, body, receipts and state update to storage
        writer.write_status(global_id, status)?;
        writer.write_header(global_id, header)?;
        writer.write_body(global_id, transactions)?;
        writer.write_receipts(global_id, receipts)?;
        writer.write_state_update(global_id, state_update)?;

        Ok(())
    }
}
