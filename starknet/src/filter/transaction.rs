use apibara_dna_common::{
    index::ScalarValue,
    query::{Condition, Filter},
};
use apibara_dna_protocol::starknet;

use crate::fragment::{INDEX_TRANSACTION_BY_STATUS, TRANSACTION_FRAGMENT_ID};

use super::helpers::FragmentFilterExt;

impl FragmentFilterExt for starknet::TransactionFilter {
    fn compile_to_filter(&self) -> tonic::Result<Filter, tonic::Status> {
        let mut conditions = Vec::new();

        let transaction_status = if let Some(transaction_status) = self.transaction_status {
            starknet::TransactionStatusFilter::try_from(transaction_status).map_err(|_| {
                tonic::Status::invalid_argument(format!(
                    "invalid transaction status in transaction filter with id {}",
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
                    index_id: INDEX_TRANSACTION_BY_STATUS,
                    key: ScalarValue::Int32(starknet::TransactionStatus::Succeeded as i32),
                });
            }
            starknet::TransactionStatusFilter::Reverted => {
                conditions.push(Condition {
                    index_id: INDEX_TRANSACTION_BY_STATUS,
                    key: ScalarValue::Int32(starknet::TransactionStatus::Reverted as i32),
                });
            }
        };

        if let Some(_inner) = self.inner.as_ref() {
            return Err(tonic::Status::unimplemented(
                "filter by transaction type is not implemented",
            ));
        }

        Ok(Filter {
            filter_id: self.id,
            fragment_id: TRANSACTION_FRAGMENT_ID,
            conditions,
        })
    }
}
