use apibara_dna_common::{
    index::ScalarValue,
    query::{Condition, Filter},
};
use apibara_dna_protocol::beaconchain;

use crate::fragment::{INDEX_VALIDATOR_BY_INDEX, INDEX_VALIDATOR_BY_STATUS, VALIDATOR_FRAGMENT_ID};

use super::helpers::FragmentFilterExt;

impl FragmentFilterExt for beaconchain::ValidatorFilter {
    fn compile_to_filter(&self) -> tonic::Result<Filter, tonic::Status> {
        let mut conditions = Vec::new();

        if let Some(index) = self.validator_index {
            conditions.push(Condition {
                index_id: INDEX_VALIDATOR_BY_INDEX,
                key: ScalarValue::Uint32(index),
            });
        }

        if let Some(status) = self.status {
            conditions.push(Condition {
                index_id: INDEX_VALIDATOR_BY_STATUS,
                key: ScalarValue::Int32(status),
            });
        }

        Ok(Filter {
            filter_id: self.id,
            fragment_id: VALIDATOR_FRAGMENT_ID,
            conditions,
        })
    }
}
