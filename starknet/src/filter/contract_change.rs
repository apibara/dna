use apibara_dna_common::{
    index::ScalarValue,
    query::{Condition, Filter},
};
use apibara_dna_protocol::starknet;

use crate::fragment::{CONTRACT_CHANGE_FRAGMENT_ID, INDEX_CONTRACT_CHANGE_BY_TYPE};

use super::helpers::FragmentFilterExt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContractChangeType {
    DeclaredClass = 0,
    Deployed = 1,
    Replaced = 2,
}

impl FragmentFilterExt for starknet::ContractChangeFilter {
    fn compile_to_filter(&self) -> tonic::Result<Filter, tonic::Status> {
        let mut conditions = Vec::new();

        if let Some(change) = self.change.as_ref() {
            use starknet::contract_change_filter::Change;

            let key = match change {
                Change::DeclaredClass(_) => ContractChangeType::DeclaredClass,
                Change::DeployedContract(_) => ContractChangeType::Deployed,
                Change::ReplacedClass(_) => ContractChangeType::Replaced,
            };

            conditions.push(Condition {
                index_id: INDEX_CONTRACT_CHANGE_BY_TYPE,
                key: key.to_scalar_value(),
            });
        }

        Ok(Filter {
            filter_id: self.id,
            fragment_id: CONTRACT_CHANGE_FRAGMENT_ID,
            conditions,
            joins: Vec::default(),
        })
    }
}

impl ContractChangeType {
    pub fn to_scalar_value(&self) -> ScalarValue {
        ScalarValue::Uint32(*self as u32)
    }
}
