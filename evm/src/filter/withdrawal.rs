use apibara_dna_common::{
    index::ScalarValue,
    query::{Condition, Filter},
};
use apibara_dna_protocol::evm;

use crate::fragment::{
    INDEX_WITHDRAWAL_BY_ADDRESS, INDEX_WITHDRAWAL_BY_VALIDATOR_INDEX, WITHDRAWAL_FRAGMENT_ID,
};

use super::helpers::FragmentFilterExt;

impl FragmentFilterExt for evm::WithdrawalFilter {
    fn compile_to_filter(&self) -> tonic::Result<Filter, tonic::Status> {
        let mut conditions = Vec::new();

        if let Some(validator_index) = self.validator_index {
            conditions.push(Condition {
                index_id: INDEX_WITHDRAWAL_BY_VALIDATOR_INDEX,
                key: ScalarValue::Uint32(validator_index),
            });
        }

        if let Some(address) = self.address {
            conditions.push(Condition {
                index_id: INDEX_WITHDRAWAL_BY_ADDRESS,
                key: ScalarValue::B160(address.to_bytes()),
            });
        }

        Ok(Filter {
            filter_id: self.id,
            fragment_id: WITHDRAWAL_FRAGMENT_ID,
            conditions,
        })
    }
}
