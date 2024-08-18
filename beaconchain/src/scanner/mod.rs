use std::collections::{BTreeMap, BTreeSet};

/// A set of filter ids.
pub type FilterSet = BTreeSet<u16>;

/// Map a data index to a set of filter ids.
#[derive(Debug, Default)]
pub struct DataReference(BTreeMap<u32, FilterSet>);

#[derive(Debug, Default)]
pub struct BeaconChainDataReference {
    pub validators: DataReference,
    pub transactions: DataReference,
    pub blobs: DataReference,
}

impl DataReference {
    /// Adds a matching filter for the given index.
    pub fn add(&mut self, index: u32, filter: u16) {
        self.0.entry(index).or_default().insert(filter);
    }
}
