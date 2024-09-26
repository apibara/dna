use apibara_dna_common::{
    index::ScalarValue,
    query::{Condition, Filter},
};
use apibara_dna_protocol::beaconchain;

use crate::fragment::{
    INDEX_TRANSACTION_BY_CREATE, INDEX_TRANSACTION_BY_FROM_ADDRESS,
    INDEX_TRANSACTION_BY_TO_ADDRESS, TRANSACTION_FRAGMENT_ID,
};

use super::helpers::FragmentFilterExt;

impl FragmentFilterExt for beaconchain::TransactionFilter {
    fn compile_to_filter(&self) -> tonic::Result<Filter, tonic::Status> {
        let mut conditions = Vec::new();

        if let Some(from) = self.from.as_ref() {
            conditions.push(Condition {
                index_id: INDEX_TRANSACTION_BY_FROM_ADDRESS,
                key: ScalarValue::B160(from.to_bytes()),
            });
        }

        if let Some(to) = self.to.as_ref() {
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

        Ok(Filter {
            filter_id: self.id,
            fragment_id: TRANSACTION_FRAGMENT_ID,
            conditions,
        })
    }
}
