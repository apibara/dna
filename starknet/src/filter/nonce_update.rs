use apibara_dna_common::{
    index::ScalarValue,
    query::{Condition, Filter},
};
use apibara_dna_protocol::starknet;

use crate::fragment::{INDEX_NONCE_UPDATE_BY_CONTRACT_ADDRESS, NONCE_UPDATE_FRAGMENT_ID};

use super::helpers::FragmentFilterExt;

impl FragmentFilterExt for starknet::NonceUpdateFilter {
    fn compile_to_filter(&self) -> tonic::Result<Filter, tonic::Status> {
        let mut conditions = Vec::new();

        if let Some(address) = self.contract_address.as_ref() {
            conditions.push(Condition {
                index_id: INDEX_NONCE_UPDATE_BY_CONTRACT_ADDRESS,
                key: ScalarValue::B256(address.to_bytes()),
            })
        }

        Ok(Filter {
            filter_id: self.id,
            fragment_id: NONCE_UPDATE_FRAGMENT_ID,
            conditions,
            joins: Vec::default(),
        })
    }
}
