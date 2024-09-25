use std::collections::BTreeMap;

use error_stack::Result;
use roaring::RoaringBitmap;
use tracing::trace;

use crate::{
    fragment::{FragmentId, IndexFragment, IndexId},
    index::{self, ScalarValue},
};

pub type FilterId = u32;

/// Filter a fragment based on the values from this index.
#[derive(Debug, Clone)]
pub struct Condition {
    /// The index to filter on.
    pub index_id: IndexId,
    /// The value to filter on.
    pub key: ScalarValue,
}

/// A single filter.
#[derive(Debug, Clone)]
pub struct Filter {
    /// The filter id.
    pub filter_id: FilterId,
    /// The fragment to filter.
    pub fragment_id: FragmentId,
    /// The conditions to filter on.
    ///
    /// These conditions are logically ANDed together.
    pub conditions: Vec<Condition>,
}

/// A collection of filters.
#[derive(Debug, Clone, Default)]
pub struct BlockFilter {
    pub always_include_header: bool,
    filters: BTreeMap<FragmentId, Vec<Filter>>,
}

impl BlockFilter {
    pub fn set_always_include_header(&mut self, value: bool) {
        self.always_include_header = value;
    }

    /// Add a filter to the block filter.
    pub fn add_filter(&mut self, filter: Filter) {
        self.filters
            .entry(filter.fragment_id)
            .or_default()
            .push(filter);
    }

    /// Returns an iterator over the filters, grouped by fragment.
    pub fn iter(&self) -> impl Iterator<Item = (&FragmentId, &Vec<Filter>)> {
        self.filters.iter()
    }
}

#[derive(Debug)]
pub struct FilterError;

impl Filter {
    pub fn filter(&self, indexes: &IndexFragment) -> Result<RoaringBitmap, FilterError> {
        let mut result = RoaringBitmap::from_iter(
            indexes.range_start..(indexes.range_start + indexes.range_len),
        );
        trace!(starting = ?result, "starting bitmap");

        for cond in self.conditions.iter() {
            let cond_index = indexes
                .indexes
                .get(cond.index_id as usize)
                .ok_or(FilterError)?;

            match &cond_index.index {
                index::Index::Bitmap(bitmap) => {
                    if let Some(bitmap) = bitmap.get(&cond.key) {
                        result &= bitmap;
                        trace!(result = ?result, "bitmap match");
                    } else {
                        trace!("no match");
                        result.clear();
                        break;
                    }
                }
            }
        }

        Ok(result)
    }
}

impl error_stack::Context for FilterError {}

impl std::fmt::Display for FilterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "failed to filter block")
    }
}
