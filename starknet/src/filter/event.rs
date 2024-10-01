use apibara_dna_common::{
    index::ScalarValue,
    query::{Condition, Filter},
};
use apibara_dna_protocol::starknet;

use crate::fragment::{
    EVENT_FRAGMENT_ID, INDEX_EVENT_BY_ADDRESS, INDEX_EVENT_BY_KEY0, INDEX_EVENT_BY_KEY1,
    INDEX_EVENT_BY_KEY2, INDEX_EVENT_BY_KEY3, INDEX_EVENT_BY_KEY_LENGTH,
    INDEX_EVENT_BY_TRANSACTION_STATUS,
};

use super::helpers::FragmentFilterExt;

impl FragmentFilterExt for starknet::EventFilter {
    fn compile_to_filter(&self) -> tonic::Result<Filter, tonic::Status> {
        let mut conditions = Vec::new();

        if let Some(address) = self.address.as_ref() {
            conditions.push(Condition {
                index_id: INDEX_EVENT_BY_ADDRESS,
                key: ScalarValue::B256(address.to_bytes()),
            });
        }

        if let Some(true) = self.strict.as_ref() {
            conditions.push(Condition {
                index_id: INDEX_EVENT_BY_KEY_LENGTH,
                key: ScalarValue::Uint32(self.keys.len() as u32),
            });
        }

        let mut keys = self.keys.iter();

        if let Some(key) = keys.next().and_then(|key| key.value.as_ref()) {
            conditions.push(Condition {
                index_id: INDEX_EVENT_BY_KEY0,
                key: ScalarValue::B256(key.to_bytes()),
            });
        }
        if let Some(key) = keys.next().and_then(|key| key.value.as_ref()) {
            conditions.push(Condition {
                index_id: INDEX_EVENT_BY_KEY1,
                key: ScalarValue::B256(key.to_bytes()),
            });
        }
        if let Some(key) = keys.next().and_then(|key| key.value.as_ref()) {
            conditions.push(Condition {
                index_id: INDEX_EVENT_BY_KEY2,
                key: ScalarValue::B256(key.to_bytes()),
            });
        }
        if let Some(key) = keys.next().and_then(|key| key.value.as_ref()) {
            conditions.push(Condition {
                index_id: INDEX_EVENT_BY_KEY3,
                key: ScalarValue::B256(key.to_bytes()),
            });
        }

        let transaction_status = if let Some(transaction_status) = self.transaction_status {
            starknet::TransactionStatusFilter::try_from(transaction_status).map_err(|_| {
                tonic::Status::invalid_argument(format!(
                    "invalid transaction status in event filter with id {}",
                    self.id
                ))
            })?
        } else {
            starknet::TransactionStatusFilter::Succeeded
        };

        match transaction_status {
            starknet::TransactionStatusFilter::Unspecified => {}
            starknet::TransactionStatusFilter::All => {}
            starknet::TransactionStatusFilter::Succeeded => {
                conditions.push(Condition {
                    index_id: INDEX_EVENT_BY_TRANSACTION_STATUS,
                    key: ScalarValue::Int32(starknet::TransactionStatus::Succeeded as i32),
                });
            }
            starknet::TransactionStatusFilter::Reverted => {
                conditions.push(Condition {
                    index_id: INDEX_EVENT_BY_TRANSACTION_STATUS,
                    key: ScalarValue::Int32(starknet::TransactionStatus::Reverted as i32),
                });
            }
        };

        Ok(Filter {
            filter_id: self.id,
            fragment_id: EVENT_FRAGMENT_ID,
            conditions,
        })
    }
}
