use apibara_dna_common::{
    chain::BlockInfo,
    ingestion::{BlockIngestion, IngestionError},
    store::Block,
    Cursor, Hash,
};
use error_stack::{Result, ResultExt};
use tokio::sync::Mutex;
use tracing::trace;

use crate::{
    provider::{models, BlockExt, BlockId, StarknetProvider},
    store::{fragment, BlockBuilder},
};

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

        let models::MaybePendingBlockWithReceipts::Block(mut block) = block else {
            return Err(IngestionError::RpcRequest)
                .attach_printable("unexpected pending block")
                .attach_printable_lazy(|| format!("block number: {}", block_number));
        };

        /*
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
        */

        let hash = block.block_hash.to_bytes_be().to_vec();
        let parent = block.parent_hash.to_bytes_be().to_vec();
        let number = block.block_number;

        let block_info = BlockInfo {
            number,
            hash: Hash(hash),
            parent: Hash(parent),
        };

        let transactions_with_receipts = std::mem::take(&mut block.transactions);

        let header = fragment::BlockHeader::from(&block);
        let (transactions, receipts, events, messages) =
            decompose_block(&transactions_with_receipts);

        let block = {
            let block_builder = BlockBuilder {
                header,
                transactions,
                receipts,
                events,
                messages,
            };

            block_builder
                .build()
                .change_context(IngestionError::Model)
                .attach_printable("failed to build block")?
        };

        Ok((block_info, block))
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

fn decompose_block(
    transactions: &[models::TransactionWithReceipt],
) -> (
    Vec<fragment::Transaction>,
    Vec<fragment::TransactionReceipt>,
    Vec<fragment::Event>,
    Vec<fragment::MessageToL1>,
) {
    let mut block_transactions = Vec::new();
    let mut block_receipts = Vec::new();
    let mut block_events = Vec::new();
    let mut block_messages = Vec::new();

    for (transaction_index, transaction_with_receipt) in transactions.iter().enumerate() {
        let transaction_index = transaction_index as u32;
        let transaction_hash =
            fragment::FieldElement::from(transaction_with_receipt.transaction.transaction_hash());
        let transaction_reverted = matches!(
            transaction_with_receipt.receipt.execution_result(),
            models::ExecutionResult::Reverted { .. }
        );

        let events = match &transaction_with_receipt.receipt {
            models::TransactionReceipt::Invoke(rx) => &rx.events,
            models::TransactionReceipt::L1Handler(rx) => &rx.events,
            models::TransactionReceipt::Declare(rx) => &rx.events,
            models::TransactionReceipt::Deploy(rx) => &rx.events,
            models::TransactionReceipt::DeployAccount(rx) => &rx.events,
        };

        for event in events.iter() {
            let mut event = fragment::Event::from(event);

            event.event_index = block_events.len() as u32;
            event.transaction_index = transaction_index;
            event.transaction_hash = transaction_hash;
            event.transaction_reverted = transaction_reverted;

            block_events.push(event);
        }

        let messages = match &transaction_with_receipt.receipt {
            models::TransactionReceipt::Invoke(rx) => &rx.messages_sent,
            models::TransactionReceipt::L1Handler(rx) => &rx.messages_sent,
            models::TransactionReceipt::Declare(rx) => &rx.messages_sent,
            models::TransactionReceipt::Deploy(rx) => &rx.messages_sent,
            models::TransactionReceipt::DeployAccount(rx) => &rx.messages_sent,
        };

        for message in messages.iter() {
            let mut message = fragment::MessageToL1::from(message);

            message.message_index = block_messages.len() as u32;
            message.transaction_index = transaction_index;
            message.transaction_hash = transaction_hash;
            message.transaction_reverted = transaction_reverted;

            block_messages.push(message);
        }

        let mut transaction = fragment::Transaction::from(&transaction_with_receipt.transaction);
        set_transaction_index_and_reverted(
            &mut transaction,
            transaction_index,
            transaction_reverted,
        );

        let mut receipt = fragment::TransactionReceipt::from(&transaction_with_receipt.receipt);
        set_receipt_transaction_index(&mut receipt, transaction_index);

        block_transactions.push(transaction);
        block_receipts.push(receipt);
    }

    (
        block_transactions,
        block_receipts,
        block_events,
        block_messages,
    )
}

fn set_transaction_index_and_reverted(
    transaction: &mut fragment::Transaction,
    index: u32,
    reverted: bool,
) {
    use fragment::Transaction::*;
    let meta = match transaction {
        InvokeTransactionV0(tx) => &mut tx.meta,
        InvokeTransactionV1(tx) => &mut tx.meta,
        InvokeTransactionV3(tx) => &mut tx.meta,
        L1HandlerTransaction(tx) => &mut tx.meta,
        DeployTransaction(tx) => &mut tx.meta,
        DeclareTransactionV0(tx) => &mut tx.meta,
        DeclareTransactionV1(tx) => &mut tx.meta,
        DeclareTransactionV2(tx) => &mut tx.meta,
        DeclareTransactionV3(tx) => &mut tx.meta,
        DeployAccountTransactionV1(tx) => &mut tx.meta,
        DeployAccountTransactionV3(tx) => &mut tx.meta,
    };
    meta.transaction_index = index;
    meta.transaction_reverted = reverted;
}

fn set_receipt_transaction_index(receipt: &mut fragment::TransactionReceipt, index: u32) {
    use fragment::TransactionReceipt::*;
    match receipt {
        Invoke(rx) => rx.meta.transaction_index = index,
        L1Handler(rx) => rx.meta.transaction_index = index,
        Deploy(rx) => rx.meta.transaction_index = index,
        Declare(rx) => rx.meta.transaction_index = index,
        DeployAccount(rx) => rx.meta.transaction_index = index,
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
