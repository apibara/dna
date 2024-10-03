use alloy_rpc_types::BlockId;
use apibara_dna_common::{
    chain::BlockInfo,
    fragment::{
        Block, BodyFragment, HeaderFragment, Index, IndexFragment, IndexGroupFragment, Join,
        JoinFragment, JoinGroupFragment,
    },
    index::{BitmapIndexBuilder, ScalarValue},
    ingestion::{BlockIngestion, IngestionError},
    join::{JoinToManyIndexBuilder, JoinToOneIndexBuilder},
    Cursor,
};
use apibara_dna_protocol::evm;
use error_stack::{Result, ResultExt};
use prost::Message;

use crate::{
    fragment::{
        INDEX_LOG_BY_ADDRESS, INDEX_LOG_BY_TOPIC0, INDEX_LOG_BY_TOPIC1, INDEX_LOG_BY_TOPIC2,
        INDEX_LOG_BY_TOPIC3, INDEX_LOG_BY_TOPIC_LENGTH, INDEX_LOG_BY_TRANSACTION_STATUS,
        INDEX_TRANSACTION_BY_CREATE, INDEX_TRANSACTION_BY_FROM_ADDRESS,
        INDEX_TRANSACTION_BY_STATUS, INDEX_TRANSACTION_BY_TO_ADDRESS, INDEX_WITHDRAWAL_BY_ADDRESS,
        INDEX_WITHDRAWAL_BY_VALIDATOR_INDEX, LOG_FRAGMENT_ID, LOG_FRAGMENT_NAME,
        RECEIPT_FRAGMENT_ID, RECEIPT_FRAGMENT_NAME, TRANSACTION_FRAGMENT_ID,
        TRANSACTION_FRAGMENT_NAME, WITHDRAWAL_FRAGMENT_ID, WITHDRAWAL_FRAGMENT_NAME,
    },
    proto::{convert_block_header, ModelExt},
    provider::{models, BlockExt, JsonRpcProvider},
};

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
        let block = self
            .provider
            .get_block_header(BlockId::latest())
            .await
            .change_context(IngestionError::RpcRequest)?;

        Ok(block.cursor())
    }

    #[tracing::instrument("evm_get_finalized_cursor", skip_all, err(Debug))]
    async fn get_finalized_cursor(&self) -> Result<Cursor, IngestionError> {
        let block = self
            .provider
            .get_block_header(BlockId::finalized())
            .await
            .change_context(IngestionError::RpcRequest)?;

        Ok(block.cursor())
    }

    #[tracing::instrument("evm_get_block_info_by_number", skip(self), err(Debug))]
    async fn get_block_info_by_number(
        &self,
        block_number: u64,
    ) -> Result<BlockInfo, IngestionError> {
        let block = self
            .provider
            .get_block_header(BlockId::number(block_number))
            .await
            .change_context(IngestionError::RpcRequest)?;

        Ok(block.block_info())
    }

    #[tracing::instrument("evm_ingest_block_by_number", skip(self), err(Debug))]
    async fn ingest_block_by_number(
        &self,
        block_number: u64,
    ) -> Result<(BlockInfo, Block), IngestionError> {
        let mut block_with_transactions = self
            .provider
            .get_block_with_transactions(BlockId::number(block_number))
            .await
            .change_context(IngestionError::RpcRequest)
            .attach_printable("failed to get block with transactions")
            .attach_printable_lazy(|| format!("block number: {}", block_number))?;

        let block_id = BlockId::hash(block_with_transactions.header.hash);

        let block_receipts = self
            .provider
            .get_block_receipts(block_id)
            .await
            .change_context(IngestionError::RpcRequest)
            .attach_printable("failed to get block receipts")
            .attach_printable_lazy(|| format!("block number: {}", block_number))
            .attach_printable_lazy(|| {
                format!("block hash: {}", block_with_transactions.header.hash)
            })?;

        let block_transactions = std::mem::take(&mut block_with_transactions.transactions);
        let Some(block_transactions) = block_transactions.as_transactions() else {
            return Err(IngestionError::RpcRequest)
                .attach_printable("unexpected transactions as hashes");
        };

        let block_withdrawals =
            std::mem::take(&mut block_with_transactions.withdrawals).unwrap_or_default();

        let block_info = block_with_transactions.block_info();

        let header_fragment = {
            let header = convert_block_header(block_with_transactions.header);
            HeaderFragment {
                data: header.encode_to_vec(),
            }
        };

        let (body, index, join) =
            collect_block_body_and_index(block_transactions, &block_withdrawals, &block_receipts)?;

        let block = Block {
            header: header_fragment,
            index,
            body,
            join,
        };

        Ok((block_info, block))
    }
}

fn collect_block_body_and_index(
    transactions: &[models::Transaction],
    withdrawals: &[models::Withdrawal],
    receipts: &[models::TransactionReceipt],
) -> Result<(Vec<BodyFragment>, IndexGroupFragment, JoinGroupFragment), IngestionError> {
    let mut block_withdrawals = Vec::new();
    let mut block_transactions = Vec::new();
    let mut block_receipts = Vec::new();
    let mut block_logs = Vec::new();

    let mut index_withdrawal_by_validator_index = BitmapIndexBuilder::default();
    let mut index_withdrawal_by_address = BitmapIndexBuilder::default();

    let mut index_transaction_by_from_address = BitmapIndexBuilder::default();
    let mut index_transaction_by_to_address = BitmapIndexBuilder::default();
    let mut index_transaction_by_create = BitmapIndexBuilder::default();
    let mut index_transaction_by_status = BitmapIndexBuilder::default();
    let mut join_transaction_to_receipt = JoinToOneIndexBuilder::default();
    let mut join_transaction_to_logs = JoinToManyIndexBuilder::default();

    let mut index_log_by_address = BitmapIndexBuilder::default();
    let mut index_log_by_topic0 = BitmapIndexBuilder::default();
    let mut index_log_by_topic1 = BitmapIndexBuilder::default();
    let mut index_log_by_topic2 = BitmapIndexBuilder::default();
    let mut index_log_by_topic3 = BitmapIndexBuilder::default();
    let mut index_log_by_topic_length = BitmapIndexBuilder::default();
    let mut index_log_by_transaction_status = BitmapIndexBuilder::default();
    let mut join_log_to_transaction = JoinToOneIndexBuilder::default();
    let mut join_log_to_receipt = JoinToOneIndexBuilder::default();
    let mut join_log_to_siblings = JoinToManyIndexBuilder::default();

    for (withdrawal_index, withdrawal) in withdrawals.iter().enumerate() {
        let withdrawal_index = withdrawal_index as u32;

        let mut withdrawal = withdrawal.to_proto();

        withdrawal.withdrawal_index = withdrawal_index;

        index_withdrawal_by_validator_index.insert(
            ScalarValue::Uint32(withdrawal.validator_index),
            withdrawal_index,
        );

        if let Some(address) = withdrawal.address {
            index_withdrawal_by_address
                .insert(ScalarValue::B160(address.to_bytes()), withdrawal_index);
        }

        block_withdrawals.push(withdrawal);
    }

    for (transaction_index, (transaction, receipt)) in
        transactions.iter().zip(receipts.iter()).enumerate()
    {
        let transaction_index = transaction_index as u32;
        let transaction_hash = receipt.transaction_hash.to_proto();

        let transaction_status = if receipt.status() {
            evm::TransactionStatus::Succeeded as i32
        } else {
            evm::TransactionStatus::Reverted as i32
        };

        if let Some(rpc_transaction_index) = transaction.transaction_index {
            if rpc_transaction_index != transaction_index as u64 {
                return Err(IngestionError::Model)
                    .attach_printable("transaction index mismatch")
                    .attach_printable_lazy(|| format!("transaction index: {}", transaction_index))
                    .attach_printable_lazy(|| {
                        format!("rpc transaction index: {}", rpc_transaction_index)
                    });
            }
        }

        if let Some(rpc_transaction_index) = receipt.transaction_index {
            if rpc_transaction_index != transaction_index as u64 {
                return Err(IngestionError::Model)
                    .attach_printable("transaction index mismatch in receipt")
                    .attach_printable_lazy(|| format!("transaction index: {}", transaction_index))
                    .attach_printable_lazy(|| {
                        format!("rpc transaction index: {}", rpc_transaction_index)
                    });
            }
        }

        let mut transaction = transaction.to_proto();

        transaction.transaction_index = transaction_index;
        transaction.transaction_hash = transaction_hash.into();
        transaction.transaction_status = transaction_status;

        join_transaction_to_receipt.insert(transaction_index, transaction_index);

        if let Some(from) = transaction.from {
            index_transaction_by_from_address
                .insert(ScalarValue::B160(from.to_bytes()), transaction_index);
        }

        match transaction.to {
            Some(to) => {
                index_transaction_by_to_address
                    .insert(ScalarValue::B160(to.to_bytes()), transaction_index);
                index_transaction_by_create.insert(ScalarValue::Bool(false), transaction_index);
            }
            None => {
                index_transaction_by_create.insert(ScalarValue::Bool(true), transaction_index);
            }
        }

        index_transaction_by_status
            .insert(ScalarValue::Int32(transaction_status), transaction_index);

        block_transactions.push(transaction);

        let mut transaction_logs_id = Vec::new();

        for log in receipt.inner.logs() {
            let log_index = block_logs.len() as u32;

            if let Some(rpc_log_index) = log.log_index {
                if rpc_log_index != log_index as u64 {
                    return Err(IngestionError::Model)
                        .attach_printable("log index mismatch in receipt")
                        .attach_printable_lazy(|| format!("expected log index: {}", log_index))
                        .attach_printable_lazy(|| format!("rpc log index: {}", rpc_log_index));
                }
            }

            if let Some(rpc_transaction_index) = log.transaction_index {
                if rpc_transaction_index != transaction_index as u64 {
                    return Err(IngestionError::Model)
                        .attach_printable("transaction index mismatch in log")
                        .attach_printable_lazy(|| {
                            format!("transaction index: {}", transaction_index)
                        })
                        .attach_printable_lazy(|| {
                            format!("rpc transaction index: {}", rpc_transaction_index)
                        });
                }
            }

            let mut log = log.to_proto();

            log.log_index = log_index;
            log.transaction_index = transaction_index;
            log.transaction_hash = transaction_hash.into();
            log.transaction_status = transaction_status;

            join_log_to_transaction.insert(log_index, transaction_index);
            join_log_to_receipt.insert(log_index, transaction_index);
            join_transaction_to_logs.insert(transaction_index, log_index);

            transaction_logs_id.push(log_index);

            if let Some(address) = log.address {
                index_log_by_address.insert(ScalarValue::B160(address.to_bytes()), log_index);
            }

            let mut topics = log.topics.iter();

            if let Some(topic) = topics.next() {
                index_log_by_topic0.insert(ScalarValue::B256(topic.to_bytes()), log_index);
            }
            if let Some(topic) = topics.next() {
                index_log_by_topic1.insert(ScalarValue::B256(topic.to_bytes()), log_index);
            }
            if let Some(topic) = topics.next() {
                index_log_by_topic2.insert(ScalarValue::B256(topic.to_bytes()), log_index);
            }
            if let Some(topic) = topics.next() {
                index_log_by_topic3.insert(ScalarValue::B256(topic.to_bytes()), log_index);
            }

            index_log_by_topic_length
                .insert(ScalarValue::Uint32(log.topics.len() as u32), log_index);

            index_log_by_transaction_status
                .insert(ScalarValue::Int32(transaction_status), log_index);

            block_logs.push(log);
        }

        for log_id in transaction_logs_id.iter() {
            for sibling_id in transaction_logs_id.iter() {
                if sibling_id != log_id {
                    join_log_to_siblings.insert(*log_id, *sibling_id);
                }
            }
        }

        let mut receipt = receipt.to_proto();

        receipt.transaction_index = transaction_index;
        receipt.transaction_hash = transaction_hash.into();
        receipt.transaction_status = transaction_status;

        block_receipts.push(receipt);
    }

    let withdrawal_index = {
        let index_withdrawal_by_validator_index = Index {
            index_id: INDEX_WITHDRAWAL_BY_VALIDATOR_INDEX,
            index: index_withdrawal_by_validator_index
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        let index_withdrawal_by_address = Index {
            index_id: INDEX_WITHDRAWAL_BY_ADDRESS,
            index: index_withdrawal_by_address
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        IndexFragment {
            fragment_id: WITHDRAWAL_FRAGMENT_ID,
            range_start: 0,
            range_len: block_withdrawals.len() as u32,
            indexes: vec![
                index_withdrawal_by_validator_index,
                index_withdrawal_by_address,
            ],
        }
    };

    let withdrawal_join = JoinFragment {
        fragment_id: WITHDRAWAL_FRAGMENT_ID,
        joins: Vec::default(),
    };

    let withdrawal_fragment = BodyFragment {
        fragment_id: WITHDRAWAL_FRAGMENT_ID,
        name: WITHDRAWAL_FRAGMENT_NAME.to_string(),
        data: block_withdrawals
            .iter()
            .map(Message::encode_to_vec)
            .collect(),
    };

    let transaction_index = {
        let index_transaction_by_from_address = Index {
            index_id: INDEX_TRANSACTION_BY_FROM_ADDRESS,
            index: index_transaction_by_from_address
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        let index_transaction_by_to_address = Index {
            index_id: INDEX_TRANSACTION_BY_TO_ADDRESS,
            index: index_transaction_by_to_address
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        let index_transaction_by_create = Index {
            index_id: INDEX_TRANSACTION_BY_CREATE,
            index: index_transaction_by_create
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        let index_transaction_by_status = Index {
            index_id: INDEX_TRANSACTION_BY_STATUS,
            index: index_transaction_by_status
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        IndexFragment {
            fragment_id: TRANSACTION_FRAGMENT_ID,
            range_start: 0,
            range_len: block_transactions.len() as u32,
            indexes: vec![
                index_transaction_by_from_address,
                index_transaction_by_to_address,
                index_transaction_by_create,
                index_transaction_by_status,
            ],
        }
    };

    let transaction_join = {
        let join_transaction_to_receipt = Join {
            to_fragment_id: RECEIPT_FRAGMENT_ID,
            index: join_transaction_to_receipt.build().into(),
        };

        let join_transaction_to_logs = Join {
            to_fragment_id: LOG_FRAGMENT_ID,
            index: join_transaction_to_logs
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        JoinFragment {
            fragment_id: TRANSACTION_FRAGMENT_ID,
            joins: vec![join_transaction_to_receipt, join_transaction_to_logs],
        }
    };

    let transaction_fragment = BodyFragment {
        fragment_id: TRANSACTION_FRAGMENT_ID,
        name: TRANSACTION_FRAGMENT_NAME.to_string(),
        data: block_transactions
            .iter()
            .map(Message::encode_to_vec)
            .collect(),
    };

    // Empty since no receipt filter.
    let receipt_index = IndexFragment {
        fragment_id: RECEIPT_FRAGMENT_ID,
        range_start: 0,
        range_len: block_receipts.len() as u32,
        indexes: Vec::default(),
    };

    let receipt_join = JoinFragment {
        fragment_id: RECEIPT_FRAGMENT_ID,
        joins: Vec::default(),
    };

    let receipt_fragment = BodyFragment {
        fragment_id: RECEIPT_FRAGMENT_ID,
        name: RECEIPT_FRAGMENT_NAME.to_string(),
        data: block_receipts.iter().map(Message::encode_to_vec).collect(),
    };

    let log_index = {
        let index_log_by_address = Index {
            index_id: INDEX_LOG_BY_ADDRESS,
            index: index_log_by_address
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        let index_log_by_topic0 = Index {
            index_id: INDEX_LOG_BY_TOPIC0,
            index: index_log_by_topic0
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        let index_log_by_topic1 = Index {
            index_id: INDEX_LOG_BY_TOPIC1,
            index: index_log_by_topic1
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        let index_log_by_topic2 = Index {
            index_id: INDEX_LOG_BY_TOPIC2,
            index: index_log_by_topic2
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        let index_log_by_topic3 = Index {
            index_id: INDEX_LOG_BY_TOPIC3,
            index: index_log_by_topic3
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        let index_log_by_topic_length = Index {
            index_id: INDEX_LOG_BY_TOPIC_LENGTH,
            index: index_log_by_topic_length
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        let index_log_by_transaction_status = Index {
            index_id: INDEX_LOG_BY_TRANSACTION_STATUS,
            index: index_log_by_transaction_status
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        IndexFragment {
            fragment_id: LOG_FRAGMENT_ID,
            range_start: 0,
            range_len: block_logs.len() as u32,
            indexes: vec![
                index_log_by_address,
                index_log_by_topic0,
                index_log_by_topic1,
                index_log_by_topic2,
                index_log_by_topic3,
                index_log_by_topic_length,
                index_log_by_transaction_status,
            ],
        }
    };

    let log_join = {
        let join_log_to_transaction = Join {
            to_fragment_id: TRANSACTION_FRAGMENT_ID,
            index: join_log_to_transaction.build().into(),
        };

        let join_log_to_receipt = Join {
            to_fragment_id: RECEIPT_FRAGMENT_ID,
            index: join_log_to_receipt.build().into(),
        };

        let join_log_to_siblings = Join {
            to_fragment_id: LOG_FRAGMENT_ID,
            index: join_log_to_siblings
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        JoinFragment {
            fragment_id: LOG_FRAGMENT_ID,
            joins: vec![
                join_log_to_transaction,
                join_log_to_receipt,
                join_log_to_siblings,
            ],
        }
    };

    let log_fragment = BodyFragment {
        fragment_id: LOG_FRAGMENT_ID,
        name: LOG_FRAGMENT_NAME.to_string(),
        data: block_logs.iter().map(Message::encode_to_vec).collect(),
    };

    let index_group = IndexGroupFragment {
        indexes: vec![
            withdrawal_index,
            transaction_index,
            receipt_index,
            log_index,
        ],
    };

    let join_group = JoinGroupFragment {
        joins: vec![withdrawal_join, transaction_join, receipt_join, log_join],
    };

    Ok((
        vec![
            withdrawal_fragment,
            transaction_fragment,
            receipt_fragment,
            log_fragment,
        ],
        index_group,
        join_group,
    ))
}
