//! Download and store block data.

use std::sync::Arc;

use apibara_core::starknet::v1alpha2;
use futures::{stream, StreamExt};

use crate::{
    core::GlobalBlockId,
    db::{BlockBody, StorageWriter},
    provider::{BlockId, Provider},
};

use super::BlockIngestionError;

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

    pub async fn finish_ingesting_block<W: StorageWriter>(
        &self,
        global_id: &GlobalBlockId,
        status: v1alpha2::BlockStatus,
        header: v1alpha2::BlockHeader,
        body: BlockBody,
        writer: &mut W,
    ) -> Result<(), BlockIngestionError>
    where
        BlockIngestionError: From<W::Error>,
    {
        // download state update, receipts
        let hashes = body
            .transactions
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
                    let tx_hash = tx_hash.ok_or(BlockIngestionError::MalformedTransaction)?;
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

        // pathfinder doesn't support state update for pending data.
        let state_update = if !global_id.hash().is_zero() {
            let block_id = BlockId::Hash(*global_id.hash());
            let state_update = self
                .provider
                .get_state_update(&block_id)
                .await
                .map_err(BlockIngestionError::provider)?;
            Some(state_update)
        } else {
            None
        };

        // write block status, header, body, receipts and state update to storage
        writer.write_status(global_id, status)?;
        writer.write_header(global_id, header)?;
        writer.write_body(global_id, body)?;
        writer.write_receipts(global_id, receipts)?;

        if let Some(state_update) = state_update {
            writer.write_state_update(global_id, state_update)?;
        }

        Ok(())
    }
}
