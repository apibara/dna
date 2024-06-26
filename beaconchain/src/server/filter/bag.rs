use std::collections::BTreeMap;

use apibara_dna_protocol::beaconchain;

use crate::segment::store;

/// Holds the data shared by one or more blocks.
#[derive(Default, Debug)]
pub struct DataBag {
    validators: BTreeMap<u32, beaconchain::Validator>,
}

impl DataBag {
    pub fn add_validator(&mut self, index: u32, validator: store::Validator) {
        if self.validators.contains_key(&index) {
            return;
        }

        let validator = beaconchain::Validator::from(validator);
        self.validators.insert(index, validator);
    }

    pub fn validator(&mut self, index: &u32) -> Option<beaconchain::Validator> {
        self.validators.get(index).cloned()
    }
}
