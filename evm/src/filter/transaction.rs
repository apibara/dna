use apibara_dna_common::{
    index::ScalarValue,
    query::{Condition, Filter},
};
use apibara_dna_protocol::evm;

use crate::fragment::{
    INDEX_TRANSACTION_BY_CREATE, INDEX_TRANSACTION_BY_FROM_ADDRESS, INDEX_TRANSACTION_BY_STATUS,
    INDEX_TRANSACTION_BY_TO_ADDRESS, LOG_FRAGMENT_ID, RECEIPT_FRAGMENT_ID, TRACE_FRAGMENT_ID,
    TRANSACTION_FRAGMENT_ID,
};

use super::helpers::FragmentFilterExt;

impl FragmentFilterExt for evm::TransactionFilter {
    fn compile_to_filter(&self) -> tonic::Result<Filter, tonic::Status> {
        let mut conditions = Vec::new();

        if let Some(from) = self.from {
            conditions.push(Condition {
                index_id: INDEX_TRANSACTION_BY_FROM_ADDRESS,
                key: ScalarValue::B160(from.to_bytes()),
            });
        }

        if let Some(to) = self.to {
            conditions.push(Condition {
                index_id: INDEX_TRANSACTION_BY_TO_ADDRESS,
                key: ScalarValue::B160(to.to_bytes()),
            });
        }

        if let Some(true) = self.create {
            conditions.push(Condition {
                index_id: INDEX_TRANSACTION_BY_CREATE,
                key: ScalarValue::Bool(true),
            });
        }

        let transaction_status = if let Some(transaction_status) = self.transaction_status {
            evm::TransactionStatusFilter::try_from(transaction_status).map_err(|_| {
                tonic::Status::invalid_argument(format!(
                    "invalid transaction status in transaction filter with id {}",
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
                    index_id: INDEX_TRANSACTION_BY_STATUS,
                    key: ScalarValue::Int32(evm::TransactionStatus::Succeeded as i32),
                });
            }
            evm::TransactionStatusFilter::Reverted => {
                conditions.push(Condition {
                    index_id: INDEX_TRANSACTION_BY_STATUS,
                    key: ScalarValue::Int32(evm::TransactionStatus::Reverted as i32),
                });
            }
        };

        let mut joins = Vec::new();

        if let Some(true) = self.include_receipt {
            joins.push(RECEIPT_FRAGMENT_ID);
        }

        if let Some(true) = self.include_logs {
            joins.push(LOG_FRAGMENT_ID);
        }

        if let Some(true) = self.include_transaction_trace {
            joins.push(TRACE_FRAGMENT_ID);
        }

        Ok(Filter {
            filter_id: self.id,
            fragment_id: TRANSACTION_FRAGMENT_ID,
            conditions,
            joins,
        })
    }
}
