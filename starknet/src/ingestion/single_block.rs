use apibara_dna_common::{
    core::Cursor,
    ingestion::{BlockIngestorError, SingleBlockIngestion},
};
use error_stack::{Result, ResultExt};

use crate::{
    ingestion::models::{self, BlockIdExt},
    segment::store,
};

use super::JsonRpcProvider;

pub struct StarknetSingleBlockIngestion {
    provider: JsonRpcProvider,
}

impl StarknetSingleBlockIngestion {
    pub fn new(provider: JsonRpcProvider) -> Self {
        Self { provider }
    }
}

#[async_trait::async_trait]
impl SingleBlockIngestion<store::SingleBlock> for StarknetSingleBlockIngestion {
    async fn ingest_block(&self, cursor: Cursor) -> Result<store::SingleBlock, BlockIngestorError> {
        let block_id = cursor.to_block_id();
        let block = self
            .provider
            .get_block_with_receipts(&block_id)
            .await
            .change_context(BlockIngestorError::BlockIngestion)?;

        let models::MaybePendingBlockWithReceipts::Block(block) = block else {
            return Err(BlockIngestorError::BlockIngestion)
                .attach_printable("expected block, got pending block")?;
        };

        Ok(store::SingleBlock::from(block))
    }
}
