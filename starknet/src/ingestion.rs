use std::collections::BTreeMap;

use apibara_dna_common::{
    chain::BlockInfo,
    fragment::{Block, BodyFragment, FragmentIndexes, HeaderFragment, Index},
    index::{self, BitmapIndexBuilder, ScalarValue},
    ingestion::{BlockIngestion, IngestionError},
    Cursor, Hash,
};
use apibara_dna_protocol::starknet;
use error_stack::{Result, ResultExt};
use prost::Message;
use tokio::sync::Mutex;
use tracing::trace;

use crate::{
    fragment::{
        EVENT_FRAGMENT_ID, INDEX_EVENT_BY_ADDRESS, INDEX_EVENT_BY_KEY0, INDEX_EVENT_BY_KEY1,
        INDEX_EVENT_BY_KEY2, INDEX_EVENT_BY_KEY3, INDEX_EVENT_BY_KEY_LENGTH,
        INDEX_EVENT_BY_TRANSACTION_STATUS, INDEX_MESSAGE_BY_FROM_ADDRESS,
        INDEX_MESSAGE_BY_TO_ADDRESS, INDEX_MESSAGE_BY_TRANSACTION_STATUS,
        INDEX_TRANSACTION_BY_STATUS, MESSAGE_FRAGMENT_ID, RECEIPT_FRAGMENT_ID,
        TRANSACTION_FRAGMENT_ID,
    },
    proto::{convert_block_header, ModelExt},
    provider::{models, BlockExt, BlockId, StarknetProvider},
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

        let models::MaybePendingBlockWithReceipts::Block(block) = block else {
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

        let header_fragment = {
            let header = convert_block_header(&block);
            HeaderFragment {
                data: header.encode_to_vec(),
            }
        };

        let (index_fragments, body_fragments) = collect_block_body_and_index(&block.transactions)?;

        let block = Block {
            header: header_fragment,
            indexes: index_fragments,
            body: body_fragments,
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

fn collect_block_body_and_index(
    transactions: &[models::TransactionWithReceipt],
) -> Result<(Vec<FragmentIndexes>, Vec<BodyFragment>), IngestionError> {
    let mut block_transactions = Vec::new();
    let mut block_receipts = Vec::new();
    let mut block_events = Vec::new();
    let mut block_messages = Vec::new();

    let mut index_transaction_by_status = BitmapIndexBuilder::default();

    let mut index_event_by_address = BitmapIndexBuilder::default();
    let mut index_event_by_key0 = BitmapIndexBuilder::default();
    let mut index_event_by_key1 = BitmapIndexBuilder::default();
    let mut index_event_by_key2 = BitmapIndexBuilder::default();
    let mut index_event_by_key3 = BitmapIndexBuilder::default();
    let mut index_event_by_key_length = BitmapIndexBuilder::default();
    let mut index_event_by_transaction_status = BitmapIndexBuilder::default();

    let mut index_message_by_from_address = BitmapIndexBuilder::default();
    let mut index_message_by_to_address = BitmapIndexBuilder::default();
    let mut index_message_by_transaction_status = BitmapIndexBuilder::default();

    for (transaction_index, transaction_with_receipt) in transactions.iter().enumerate() {
        let transaction_index = transaction_index as u32;
        let transaction_hash = transaction_with_receipt
            .transaction
            .transaction_hash()
            .to_proto();

        let transaction_status = match transaction_with_receipt.receipt.execution_result() {
            models::ExecutionResult::Succeeded => starknet::TransactionStatus::Succeeded,
            models::ExecutionResult::Reverted { .. } => starknet::TransactionStatus::Reverted,
        };

        let events = match &transaction_with_receipt.receipt {
            models::TransactionReceipt::Invoke(rx) => &rx.events,
            models::TransactionReceipt::L1Handler(rx) => &rx.events,
            models::TransactionReceipt::Declare(rx) => &rx.events,
            models::TransactionReceipt::Deploy(rx) => &rx.events,
            models::TransactionReceipt::DeployAccount(rx) => &rx.events,
        };

        for event in events.iter() {
            let mut event = event.to_proto();

            event.event_index = block_events.len() as u32;
            event.transaction_index = transaction_index;
            event.transaction_hash = transaction_hash.into();
            event.transaction_status = transaction_status as i32;

            if let Some(address) = event.from_address {
                index_event_by_address
                    .insert(ScalarValue::B256(address.to_bytes()), event.event_index);
            }

            let mut keys = event.keys.iter();

            if let Some(key) = keys.next() {
                index_event_by_key0.insert(ScalarValue::B256(key.to_bytes()), event.event_index);
            }
            if let Some(key) = keys.next() {
                index_event_by_key1.insert(ScalarValue::B256(key.to_bytes()), event.event_index);
            }
            if let Some(key) = keys.next() {
                index_event_by_key2.insert(ScalarValue::B256(key.to_bytes()), event.event_index);
            }
            if let Some(key) = keys.next() {
                index_event_by_key3.insert(ScalarValue::B256(key.to_bytes()), event.event_index);
            }

            index_event_by_key_length.insert(
                ScalarValue::Uint32(event.keys.len() as u32),
                event.event_index,
            );

            index_event_by_transaction_status.insert(
                ScalarValue::Int32(transaction_status as i32),
                event.event_index,
            );

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
            let mut message = message.to_proto();

            message.message_index = block_messages.len() as u32;
            message.transaction_index = transaction_index;
            message.transaction_hash = transaction_hash.into();
            message.transaction_status = transaction_status as i32;

            if let Some(address) = message.from_address {
                index_message_by_from_address
                    .insert(ScalarValue::B256(address.to_bytes()), message.message_index);
            }

            if let Some(address) = message.to_address {
                index_message_by_to_address
                    .insert(ScalarValue::B256(address.to_bytes()), message.message_index);
            }

            index_message_by_transaction_status.insert(
                ScalarValue::Int32(transaction_status as i32),
                message.message_index,
            );

            block_messages.push(message);
        }

        let mut transaction = transaction_with_receipt.transaction.to_proto();
        set_transaction_index_and_status(&mut transaction, transaction_index, transaction_status);

        index_transaction_by_status.insert(
            ScalarValue::Int32(transaction_status as i32),
            transaction_index,
        );

        let mut receipt = transaction_with_receipt.receipt.to_proto();
        set_receipt_transaction_index(&mut receipt, transaction_index);

        block_transactions.push(transaction);
        block_receipts.push(receipt);
    }

    block_events.sort_by_key(|event| event.event_index);
    block_transactions.sort_by_key(|tx| {
        tx.meta
            .as_ref()
            .map(|m| m.transaction_index)
            .unwrap_or_default()
    });
    block_receipts.sort_by_key(|rx| {
        rx.meta
            .as_ref()
            .map(|m| m.transaction_index)
            .unwrap_or_default()
    });
    block_messages.sort_by_key(|msg| msg.message_index);

    let transaction_index = {
        let index_transaction_by_status = Index {
            index_id: INDEX_TRANSACTION_BY_STATUS,
            index: index_transaction_by_status
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        FragmentIndexes {
            fragment_id: TRANSACTION_FRAGMENT_ID,
            indexes: vec![index_transaction_by_status],
        }
    };

    let transaction_fragment = BodyFragment {
        id: TRANSACTION_FRAGMENT_ID,
        data: block_transactions
            .iter()
            .map(Message::encode_to_vec)
            .collect(),
    };

    // Empty since no receipt filter.
    let receipt_index = FragmentIndexes {
        fragment_id: RECEIPT_FRAGMENT_ID,
        indexes: Vec::default(),
    };

    let receipt_fragment = BodyFragment {
        id: RECEIPT_FRAGMENT_ID,
        data: block_receipts.iter().map(Message::encode_to_vec).collect(),
    };

    let event_index = {
        let index_event_by_address = Index {
            index_id: INDEX_EVENT_BY_ADDRESS,
            index: index_event_by_address
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };
        let index_event_by_key0 = Index {
            index_id: INDEX_EVENT_BY_KEY0,
            index: index_event_by_key0
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };
        let index_event_by_key1 = Index {
            index_id: INDEX_EVENT_BY_KEY1,
            index: index_event_by_key1
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };
        let index_event_by_key2 = Index {
            index_id: INDEX_EVENT_BY_KEY2,
            index: index_event_by_key2
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };
        let index_event_by_key3 = Index {
            index_id: INDEX_EVENT_BY_KEY3,
            index: index_event_by_key3
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };
        let index_event_by_key_length = Index {
            index_id: INDEX_EVENT_BY_KEY_LENGTH,
            index: index_event_by_key_length
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };
        let index_event_by_transaction_status = Index {
            index_id: INDEX_EVENT_BY_TRANSACTION_STATUS,
            index: index_event_by_transaction_status
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        FragmentIndexes {
            fragment_id: EVENT_FRAGMENT_ID,
            indexes: vec![
                index_event_by_address,
                index_event_by_key0,
                index_event_by_key1,
                index_event_by_key2,
                index_event_by_key3,
                index_event_by_key_length,
                index_event_by_transaction_status,
            ],
        }
    };

    let event_fragment = BodyFragment {
        id: EVENT_FRAGMENT_ID,
        data: block_events.iter().map(Message::encode_to_vec).collect(),
    };

    let message_index = {
        let index_message_by_from_address = Index {
            index_id: INDEX_MESSAGE_BY_FROM_ADDRESS,
            index: index_message_by_from_address
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };
        let index_message_by_to_address = Index {
            index_id: INDEX_MESSAGE_BY_TO_ADDRESS,
            index: index_message_by_to_address
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };
        let index_message_by_transaction_status = Index {
            index_id: INDEX_MESSAGE_BY_TRANSACTION_STATUS,
            index: index_message_by_transaction_status
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        FragmentIndexes {
            fragment_id: MESSAGE_FRAGMENT_ID,
            indexes: vec![
                index_message_by_from_address,
                index_message_by_to_address,
                index_message_by_transaction_status,
            ],
        }
    };

    let message_fragment = BodyFragment {
        id: MESSAGE_FRAGMENT_ID,
        data: block_messages.iter().map(Message::encode_to_vec).collect(),
    };

    Ok((
        vec![transaction_index, receipt_index, event_index, message_index],
        vec![
            transaction_fragment,
            receipt_fragment,
            event_fragment,
            message_fragment,
        ],
    ))
}

fn set_transaction_index_and_status(
    transaction: &mut starknet::Transaction,
    index: u32,
    status: starknet::TransactionStatus,
) {
    let Some(meta) = transaction.meta.as_mut() else {
        return;
    };

    meta.transaction_index = index;
    meta.transaction_status = status as i32;
}

fn set_receipt_transaction_index(receipt: &mut starknet::TransactionReceipt, index: u32) {
    let Some(meta) = receipt.meta.as_mut() else {
        return;
    };

    meta.transaction_index = index;
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
