use apibara_dna_common::query::Filter;
use apibara_dna_protocol::beaconchain;

use crate::fragment::{BLOB_FRAGMENT_ID, TRANSACTION_FRAGMENT_ID};

use super::helpers::FragmentFilterExt;

impl FragmentFilterExt for beaconchain::BlobFilter {
    fn compile_to_filter(&self) -> tonic::Result<Filter, tonic::Status> {
        let mut joins = Vec::new();

        if let Some(true) = self.include_transaction {
            joins.push(TRANSACTION_FRAGMENT_ID);
        }

        Ok(Filter {
            filter_id: self.id,
            fragment_id: BLOB_FRAGMENT_ID,
            conditions: Vec::default(),
            joins,
        })
    }
}
