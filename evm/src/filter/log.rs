use apibara_dna_common::{
    index::ScalarValue,
    query::{Condition, Filter},
};
use apibara_dna_protocol::evm;

use crate::fragment::{
    INDEX_LOG_BY_ADDRESS, INDEX_LOG_BY_TOPIC0, INDEX_LOG_BY_TOPIC1, INDEX_LOG_BY_TOPIC2,
    INDEX_LOG_BY_TOPIC3, INDEX_LOG_BY_TOPIC_LENGTH, INDEX_LOG_BY_TRANSACTION_STATUS,
    LOG_FRAGMENT_ID,
};

use super::helpers::FragmentFilterExt;

impl FragmentFilterExt for evm::LogFilter {
    fn compile_to_filter(&self) -> tonic::Result<Filter, tonic::Status> {
        let mut conditions = Vec::new();

        if let Some(address) = self.address {
            conditions.push(Condition {
                index_id: INDEX_LOG_BY_ADDRESS,
                key: ScalarValue::B160(address.to_bytes()),
            });
        }

        if let Some(true) = self.strict {
            conditions.push(Condition {
                index_id: INDEX_LOG_BY_TOPIC_LENGTH,
                key: ScalarValue::Uint32(self.topics.len() as u32),
            });
        }

        let mut topics = self.topics.iter();

        if let Some(topic) = topics.next().and_then(|t| t.value.as_ref()) {
            conditions.push(Condition {
                index_id: INDEX_LOG_BY_TOPIC0,
                key: ScalarValue::B256(topic.to_bytes()),
            });
        }
        if let Some(topic) = topics.next().and_then(|t| t.value.as_ref()) {
            conditions.push(Condition {
                index_id: INDEX_LOG_BY_TOPIC1,
                key: ScalarValue::B256(topic.to_bytes()),
            });
        }
        if let Some(topic) = topics.next().and_then(|t| t.value.as_ref()) {
            conditions.push(Condition {
                index_id: INDEX_LOG_BY_TOPIC2,
                key: ScalarValue::B256(topic.to_bytes()),
            });
        }
        if let Some(topic) = topics.next().and_then(|t| t.value.as_ref()) {
            conditions.push(Condition {
                index_id: INDEX_LOG_BY_TOPIC3,
                key: ScalarValue::B256(topic.to_bytes()),
            });
        }

        let transaction_status = if let Some(transaction_status) = self.transaction_status {
            evm::TransactionStatusFilter::try_from(transaction_status).map_err(|_| {
                tonic::Status::invalid_argument(format!(
                    "invalid transaction status in log filter with id {}",
                    self.id
                ))
            })?
        } else {
            evm::TransactionStatusFilter::Succeeded
        };

        match transaction_status {
            evm::TransactionStatusFilter::Unspecified => {}
            evm::TransactionStatusFilter::All => {}
            evm::TransactionStatusFilter::Succeeded => {
                conditions.push(Condition {
                    index_id: INDEX_LOG_BY_TRANSACTION_STATUS,
                    key: ScalarValue::Int32(evm::TransactionStatus::Succeeded as i32),
                });
            }
            evm::TransactionStatusFilter::Reverted => {
                conditions.push(Condition {
                    index_id: INDEX_LOG_BY_TRANSACTION_STATUS,
                    key: ScalarValue::Int32(evm::TransactionStatus::Reverted as i32),
                });
            }
        };

        Ok(Filter {
            filter_id: self.id,
            fragment_id: LOG_FRAGMENT_ID,
            conditions,
        })
    }
}
