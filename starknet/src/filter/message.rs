use apibara_dna_common::{
    index::ScalarValue,
    query::{Condition, Filter},
};
use apibara_dna_protocol::starknet;

use crate::fragment::{
    EVENT_FRAGMENT_ID, INDEX_MESSAGE_BY_FROM_ADDRESS, INDEX_MESSAGE_BY_TO_ADDRESS,
    INDEX_MESSAGE_BY_TRANSACTION_STATUS, MESSAGE_FRAGMENT_ID, RECEIPT_FRAGMENT_ID,
    TRACE_FRAGMENT_ID, TRANSACTION_FRAGMENT_ID,
};

use super::helpers::FragmentFilterExt;

impl FragmentFilterExt for starknet::MessageToL1Filter {
    fn compile_to_filter(&self) -> tonic::Result<Filter, tonic::Status> {
        let mut conditions = Vec::new();

        if let Some(address) = self.from_address.as_ref() {
            conditions.push(Condition {
                index_id: INDEX_MESSAGE_BY_FROM_ADDRESS,
                key: ScalarValue::B256(address.to_bytes()),
            })
        }

        if let Some(address) = self.to_address.as_ref() {
            conditions.push(Condition {
                index_id: INDEX_MESSAGE_BY_TO_ADDRESS,
                key: ScalarValue::B256(address.to_bytes()),
            })
        }

        let transaction_status = if let Some(transaction_status) = self.transaction_status {
            starknet::TransactionStatusFilter::try_from(transaction_status).map_err(|_| {
                tonic::Status::invalid_argument(format!(
                    "invalid transaction status in message filter with id {}",
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
                    index_id: INDEX_MESSAGE_BY_TRANSACTION_STATUS,
                    key: ScalarValue::Int32(starknet::TransactionStatus::Succeeded as i32),
                });
            }
            starknet::TransactionStatusFilter::Reverted => {
                conditions.push(Condition {
                    index_id: INDEX_MESSAGE_BY_TRANSACTION_STATUS,
                    key: ScalarValue::Int32(starknet::TransactionStatus::Reverted as i32),
                });
            }
        };

        let mut joins = Vec::new();

        if let Some(true) = self.include_transaction {
            joins.push(TRANSACTION_FRAGMENT_ID);
        }

        if let Some(true) = self.include_receipt {
            joins.push(RECEIPT_FRAGMENT_ID);
        }

        if let Some(true) = self.include_events {
            joins.push(EVENT_FRAGMENT_ID);
        }

        if let Some(true) = self.include_siblings {
            joins.push(MESSAGE_FRAGMENT_ID);
        }

        if let Some(true) = self.include_transaction_trace {
            joins.push(TRACE_FRAGMENT_ID);
        }

        Ok(Filter {
            filter_id: self.id,
            fragment_id: MESSAGE_FRAGMENT_ID,
            conditions,
            joins,
        })
    }
}
