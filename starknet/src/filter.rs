use apibara_dna_common::{
    data_stream::BlockFilterFactory,
    index::ScalarValue,
    query::{BlockFilter, Condition, Filter},
};
use apibara_dna_protocol::starknet;
use prost::Message;

use crate::fragment::{
    EVENT_FRAGMENT_ID, INDEX_EVENT_BY_ADDRESS, INDEX_EVENT_BY_KEY0, INDEX_EVENT_BY_KEY1,
    INDEX_EVENT_BY_KEY2, INDEX_EVENT_BY_KEY3, INDEX_EVENT_BY_KEY_LENGTH,
    INDEX_EVENT_BY_TRANSACTION_STATUS,
};

#[derive(Debug, Clone)]
pub struct StarknetFilterFactory;

impl BlockFilterFactory for StarknetFilterFactory {
    fn create_block_filter(
        &self,
        filters: &[Vec<u8>],
    ) -> tonic::Result<Vec<BlockFilter>, tonic::Status> {
        let proto_filters = filters
            .iter()
            .map(|bytes| starknet::Filter::decode(bytes.as_slice()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| tonic::Status::invalid_argument("failed to decode filter"))?;

        if proto_filters.is_empty() {
            return Err(tonic::Status::invalid_argument("no filters provided"));
        }

        if proto_filters.len() > 5 {
            return Err(tonic::Status::invalid_argument(format!(
                "too many filters ({} > 5)",
                proto_filters.len(),
            )));
        }

        proto_filters
            .iter()
            .map(FilterExt::compile_to_block_filter)
            .collect()
    }
}

trait FilterExt {
    fn compile_to_block_filter(&self) -> tonic::Result<BlockFilter, tonic::Status>;
}

impl FilterExt for starknet::Filter {
    fn compile_to_block_filter(&self) -> tonic::Result<BlockFilter, tonic::Status> {
        let mut block_filter = BlockFilter::default();

        if self.header.map(|h| h.always()).unwrap_or(false) {
            block_filter.set_always_include_header(true);
        }

        for filter in self.events.iter() {
            let mut conditions = Vec::new();

            if let Some(address) = filter.address.as_ref() {
                conditions.push(Condition {
                    index_id: INDEX_EVENT_BY_ADDRESS,
                    key: ScalarValue::B256(address.to_bytes()),
                });
            }

            if let Some(true) = filter.strict.as_ref() {
                conditions.push(Condition {
                    index_id: INDEX_EVENT_BY_KEY_LENGTH,
                    key: ScalarValue::Uint32(filter.keys.len() as u32),
                });
            }

            let mut keys = filter.keys.iter();

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

            if let Some(transaction_status) = filter.transaction_status {
                let transaction_status = starknet::TransactionStatusFilter::try_from(
                    transaction_status,
                )
                .map_err(|_| {
                    tonic::Status::invalid_argument(format!(
                        "invalid transaction status in event filter with id {}",
                        filter.id
                    ))
                })?;

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
            }

            block_filter.add_filter(Filter {
                filter_id: filter.id,
                fragment_id: EVENT_FRAGMENT_ID,
                conditions,
            });
        }

        Ok(block_filter)
    }
}
