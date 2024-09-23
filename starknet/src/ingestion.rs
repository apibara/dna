use apibara_dna_common::{
    chain::BlockInfo,
    ingestion::{BlockIngestion, IngestionError},
    store::Block,
    Cursor, Hash,
};
use error_stack::{Result, ResultExt};
use tokio::sync::Mutex;
use tracing::trace;

use crate::provider::{models, BlockExt, BlockId, StarknetProvider};

pub struct StarknetBlockIngestion {
    provider: StarknetProvider,
    finalized_hint: Mutex<Option<u64>>,
}

impl StarknetBlockIngestion {
    pub fn new(provider: StarknetProvider) -> Self {
        let finalized_hint = Mutex::new(None);
        Self {
            provider,
            finalized_hint,
        }
    }
}

impl BlockIngestion for StarknetBlockIngestion {
    #[tracing::instrument("starknet_get_head_cursor", skip_all, err(Debug))]
    async fn get_head_cursor(&self) -> Result<Cursor, IngestionError> {
        let cursor = self
            .provider
            .get_block_with_tx_hashes(&BlockId::Head)
            .await
            .change_context(IngestionError::RpcRequest)
            .attach_printable("failed to get head block")?
            .cursor()
            .ok_or(IngestionError::RpcRequest)
            .attach_printable("missing head block cursor")?;

        Ok(cursor)
    }

    #[tracing::instrument("starknet_get_finalized_cursor", skip_all, err(Debug))]
    async fn get_finalized_cursor(&self) -> Result<Cursor, IngestionError> {
        let mut finalized_hint_guard = self.finalized_hint.lock().await;

        let head = self.get_head_cursor().await?;

        let finalized_hint = if let Some(finalized_hint) = finalized_hint_guard.as_ref() {
            Cursor::new_finalized(*finalized_hint)
        } else {
            let mut number = head.number - 50;
            loop {
                let block = self
                    .provider
                    .get_block_with_tx_hashes(&BlockId::Number(number))
                    .await
                    .change_context(IngestionError::RpcRequest)
                    .attach_printable("failed to get block by number")?;

                if block.is_finalized() {
                    let cursor = block
                        .cursor()
                        .ok_or(IngestionError::RpcRequest)
                        .attach_printable("missing block cursor")?;
                    break cursor;
                }

                if number == 0 {
                    return Err(IngestionError::RpcRequest)
                        .attach_printable("failed to find finalized block");
                } else if number < 100 {
                    number = 0;
                } else {
                    number -= 100;
                }
            }
        };

        let finalized = binary_search_finalized_block(&self.provider, finalized_hint, head.number)
            .await
            .change_context(IngestionError::RpcRequest)
            .attach_printable("failed to get finalized block")?;

        finalized_hint_guard.replace(finalized.number);

        Ok(finalized)
    }

    #[tracing::instrument("starknet_get_block_info_by_number", skip(self), err(Debug))]
    async fn get_block_info_by_number(
        &self,
        block_number: u64,
    ) -> Result<BlockInfo, IngestionError> {
        let block = self
            .provider
            .get_block_with_tx_hashes(&BlockId::Number(block_number))
            .await
            .change_context(IngestionError::RpcRequest)
            .attach_printable("failed to get block by number")
            .attach_printable_lazy(|| format!("block number: {}", block_number))?;

        let models::MaybePendingBlockWithTxHashes::Block(block) = block else {
            return Err(IngestionError::RpcRequest)
                .attach_printable("unexpected pending block")
                .attach_printable_lazy(|| format!("block number: {}", block_number));
        };

        let hash = block.block_hash.to_bytes_be().to_vec();
        let parent = block.parent_hash.to_bytes_be().to_vec();
        let number = block.block_number;

        Ok(BlockInfo {
            number,
            hash: Hash(hash),
            parent: Hash(parent),
        })
    }

    #[tracing::instrument("starknet_ingest_block_by_number", skip(self), err(Debug))]
    async fn ingest_block_by_number(
        &self,
        block_number: u64,
    ) -> Result<(BlockInfo, Block), IngestionError> {
        let block = self
            .provider
            .get_block_with_receipts(&BlockId::Number(block_number))
            .await
            .change_context(IngestionError::RpcRequest)
            .attach_printable("failed to get block by number")
            .attach_printable_lazy(|| format!("block number: {}", block_number))?;

        let models::MaybePendingBlockWithReceipts::Block(block) = block else {
            return Err(IngestionError::RpcRequest)
                .attach_printable("unexpected pending block")
                .attach_printable_lazy(|| format!("block number: {}", block_number));
        };

        // Use the block hash to avoid issues with reorgs.
        let block_id = BlockId::Hash(block.block_hash.clone());

        let state_update = self
            .provider
            .get_state_update(&block_id)
            .await
            .change_context(IngestionError::RpcRequest)?;

        let models::MaybePendingStateUpdate::Update(state_update) = state_update else {
            return Err(IngestionError::RpcRequest)
                .attach_printable("unexpected pending state update")
                .attach_printable_lazy(|| format!("block number: {}", block_number));
        };

        let hash = block.block_hash.to_bytes_be().to_vec();
        let parent = block.parent_hash.to_bytes_be().to_vec();
        let number = block.block_number;

        let block_info = BlockInfo {
            number,
            hash: Hash(hash),
            parent: Hash(parent),
        };

        println!("{:?}", state_update);
        println!("{:?}", block);
        println!("{:?}", block_info);

        todo!();
    }
}

impl Clone for StarknetBlockIngestion {
    fn clone(&self) -> Self {
        Self {
            provider: self.provider.clone(),
            finalized_hint: Mutex::new(None),
        }
    }
}

async fn binary_search_finalized_block(
    provider: &StarknetProvider,
    existing_finalized: Cursor,
    head: u64,
) -> Result<Cursor, IngestionError> {
    let mut finalized = existing_finalized;
    let mut head = head;
    let mut step_count = 0;

    loop {
        step_count += 1;

        if step_count > 100 {
            return Err(IngestionError::RpcRequest)
                .attach_printable("maximum number of iterations reached");
        }

        let mid_block_number = finalized.number + (head - finalized.number) / 2;
        trace!(mid_block_number, "binary search iteration");

        if mid_block_number <= finalized.number {
            trace!(?finalized, "finalized block found");
            break;
        }

        let mid_block = provider
            .get_block_with_tx_hashes(&BlockId::Number(mid_block_number))
            .await
            .change_context(IngestionError::RpcRequest)
            .attach_printable("failed to get block by number")?;

        let mid_cursor = mid_block
            .cursor()
            .ok_or(IngestionError::RpcRequest)
            .attach_printable("missing block cursor")?;

        if mid_block.is_finalized() {
            finalized = mid_cursor;
        } else {
            head = mid_cursor.number;
        }
    }

    Ok(finalized)
}
