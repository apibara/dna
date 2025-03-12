use apibara_dna_common::{
    index::ScalarValue,
    query::{Condition, Filter},
};
use apibara_dna_protocol::starknet;

use crate::fragment::{
    EVENT_FRAGMENT_ID, INDEX_TRANSACTION_BY_STATUS, INDEX_TRANSACTION_BY_TYPE, MESSAGE_FRAGMENT_ID,
    RECEIPT_FRAGMENT_ID, TRACE_FRAGMENT_ID, TRANSACTION_FRAGMENT_ID,
};

use super::helpers::FragmentFilterExt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionType {
    InvokeV0 = 0,
    InvokeV1 = 1,
    InvokeV3 = 2,

    Deploy = 3,

    DeclareV0 = 4,
    DeclareV1 = 5,
    DeclareV2 = 6,
    DeclareV3 = 7,

    L1Handler = 8,

    DeployAccountV1 = 9,
    DeployAccountV3 = 10,
}

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

        if let Some(inner) = self.inner.as_ref() {
            use starknet::transaction_filter::Inner;
            let key = match inner {
                Inner::InvokeV0(_) => TransactionType::InvokeV0,
                Inner::InvokeV1(_) => TransactionType::InvokeV1,
                Inner::InvokeV3(_) => TransactionType::InvokeV3,
                Inner::Deploy(_) => TransactionType::Deploy,
                Inner::DeclareV0(_) => TransactionType::DeclareV0,
                Inner::DeclareV1(_) => TransactionType::DeclareV1,
                Inner::DeclareV2(_) => TransactionType::DeclareV2,
                Inner::DeclareV3(_) => TransactionType::DeclareV3,
                Inner::L1Handler(_) => TransactionType::L1Handler,
                Inner::DeployAccountV1(_) => TransactionType::DeployAccountV1,
                Inner::DeployAccountV3(_) => TransactionType::DeployAccountV3,
            };

            conditions.push(Condition {
                index_id: INDEX_TRANSACTION_BY_TYPE,
                key: key.to_scalar_value(),
            });
        }

        let mut joins = Vec::new();

        if let Some(true) = self.include_receipt {
            joins.push(RECEIPT_FRAGMENT_ID);
        }

        if let Some(true) = self.include_events {
            joins.push(EVENT_FRAGMENT_ID);
        }

        if let Some(true) = self.include_messages {
            joins.push(MESSAGE_FRAGMENT_ID);
        }

        if let Some(true) = self.include_trace {
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

impl TransactionType {
    pub fn to_scalar_value(&self) -> ScalarValue {
        ScalarValue::Uint32(*self as u32)
    }
}
