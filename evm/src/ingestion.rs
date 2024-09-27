use apibara_dna_common::{
    chain::BlockInfo,
    fragment::Block,
    ingestion::{BlockIngestion, IngestionError},
    Cursor,
};
use error_stack::{Result, ResultExt};

use crate::provider::JsonRpcProvider;

#[derive(Clone)]
pub struct EvmBlockIngestion {
    provider: JsonRpcProvider,
}

impl EvmBlockIngestion {
    pub fn new(provider: JsonRpcProvider) -> Self {
        Self { provider }
    }
}

impl BlockIngestion for EvmBlockIngestion {
    #[tracing::instrument("evm_get_head_cursor", skip_all, err(Debug))]
    async fn get_head_cursor(&self) -> Result<Cursor, IngestionError> {
        todo!();
    }

    #[tracing::instrument("evm_get_finalized_cursor", skip_all, err(Debug))]
    async fn get_finalized_cursor(&self) -> Result<Cursor, IngestionError> {
        todo!();
    }

    #[tracing::instrument("evm_get_block_info_by_number", skip(self), err(Debug))]
    async fn get_block_info_by_number(
        &self,
        block_number: u64,
    ) -> Result<BlockInfo, IngestionError> {
        todo!();
    }

    #[tracing::instrument("evm_ingest_block_by_number", skip(self), err(Debug))]
    async fn ingest_block_by_number(
        &self,
        block_number: u64,
    ) -> Result<(BlockInfo, Block), IngestionError> {
        todo!();
    }
}
