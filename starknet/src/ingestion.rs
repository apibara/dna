use apibara_dna_common::{
    chain::BlockInfo,
    ingestion::{BlockIngestion, IngestionError},
    store::Block,
    Cursor,
};
use error_stack::Result;

use crate::provider::StarknetProvider;

#[derive(Debug)]
pub enum StarknetIngestionError {}

#[derive(Clone)]
pub struct StarknetBlockIngestion {
    provider: StarknetProvider,
}

impl StarknetBlockIngestion {
    pub fn new(provider: StarknetProvider) -> Self {
        Self { provider }
    }
}

impl BlockIngestion for StarknetBlockIngestion {
    async fn get_head_cursor(&self) -> Result<Cursor, IngestionError> {
        todo!();
    }

    async fn get_finalized_cursor(&self) -> Result<Cursor, IngestionError> {
        todo!();
    }

    async fn get_block_info_by_number(
        &self,
        block_number: u64,
    ) -> Result<BlockInfo, IngestionError> {
        todo!();
    }

    async fn ingest_block_by_number(
        &self,
        block_number: u64,
    ) -> Result<(BlockInfo, Block), IngestionError> {
        todo!();
    }
}

impl error_stack::Context for StarknetIngestionError {}

impl std::fmt::Display for StarknetIngestionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Starknet ingestion error")
    }
}
