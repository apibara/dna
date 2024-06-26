use apibara_dna_protocol::beaconchain;
use roaring::RoaringBitmap;

use crate::segment::store;

use super::validator::ValidatorFilter;

pub struct Filter {
    header: HeaderFilter,
    validators: Vec<ValidatorFilter>,
}

pub struct HeaderFilter {
    always: bool,
}

impl Filter {
    pub fn needs_linear_scan(&self) -> bool {
        self.has_required_header() || self.has_validators()
    }

    pub fn has_required_header(&self) -> bool {
        self.header.always
    }

    pub fn has_validators(&self) -> bool {
        !self.validators.is_empty()
    }

    pub fn fill_validator_bitmap(
        &self,
        validators_count: usize,
        index: &store::ValidatorsIndex,
        bitmap: &mut RoaringBitmap,
    ) -> Result<(), std::io::Error> {
        for filter in &self.validators {
            filter.fill_validator_bitmap(validators_count, index, bitmap)?;
        }

        Ok(())
    }
}

impl Default for HeaderFilter {
    fn default() -> Self {
        Self { always: false }
    }
}

impl From<beaconchain::Filter> for Filter {
    fn from(filter: beaconchain::Filter) -> Self {
        let header = filter.header.map(HeaderFilter::from).unwrap_or_default();
        let validators = filter
            .validators
            .into_iter()
            .map(ValidatorFilter::from)
            .collect();
        Self { header, validators }
    }
}

impl From<beaconchain::HeaderFilter> for HeaderFilter {
    fn from(value: beaconchain::HeaderFilter) -> Self {
        Self {
            always: value.always.unwrap_or(false),
        }
    }
}
