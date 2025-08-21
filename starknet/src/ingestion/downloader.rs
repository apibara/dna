//! Download and store block data.

use std::sync::Arc;

use apibara_core::starknet::v1alpha2;

use crate::{
    core::GlobalBlockId,
    db::{BlockBody, StorageWriter},
    provider::{BlockId, Provider},
};

use super::BlockIngestionError;

pub struct Downloader<G: Provider + Send> {
    provider: Arc<G>,
}

impl<G> Downloader<G>
where
    G: Provider + Send,
{
    pub fn new(provider: Arc<G>, _receipt_concurrency: usize) -> Self {
        Downloader { provider }
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
        let block_id = {
            // By convention, the global id of a pending block is all zeros.
            if global_id.hash().is_zero() {
                BlockId::Pending
            } else {
                BlockId::Hash(*global_id.hash())
            }
        };
        let receipts = self
            .provider
            .get_block_receipts(&block_id)
            .await
            .map_err(BlockIngestionError::provider)?;

        // Not all nodes support state updates for pending blocks.
        let state_update = {
            match self.provider.get_state_update(&block_id).await {
                Ok(state_update) => Some(state_update),
                Err(_) => None,
            }
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
