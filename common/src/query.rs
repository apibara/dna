use std::collections::BTreeMap;

use error_stack::Result;
use roaring::RoaringBitmap;
use tracing::trace;

use crate::{
    fragment::{ArchivedIndexFragment, FragmentId, IndexId},
    index::{self, ScalarValue},
};

pub type FilterId = u32;

#[derive(Debug, Clone)]
pub enum HeaderFilter {
    Always,
    OnData,
    OnDataOrOnNewBlock,
}

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
    /// Join results from this filter with the given fragments.
    pub joins: Vec<FragmentId>,
}

/// A collection of filters.
#[derive(Debug, Clone, Default)]
pub struct BlockFilter {
    pub header_filter: HeaderFilter,
    filters: BTreeMap<FragmentId, Vec<Filter>>,
}

impl BlockFilter {
    pub fn always_include_header(&self) -> bool {
        matches!(self.header_filter, HeaderFilter::Always)
    }

    pub fn set_header_filter(&mut self, value: HeaderFilter) {
        self.header_filter = value;
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

    pub fn is_empty(&self) -> bool {
        self.filters.is_empty()
    }

    pub fn len(&self) -> usize {
        self.filters.len()
    }
}

#[derive(Debug)]
pub struct FilterError;

impl Filter {
    pub fn filter(&self, indexes: &ArchivedIndexFragment) -> Result<RoaringBitmap, FilterError> {
        let range_start = indexes.range_start.to_native();
        let range_len = indexes.range_len.to_native();
        let mut result = RoaringBitmap::from_iter(range_start..(range_start + range_len));
        trace!(starting = ?result, "starting bitmap");

        for cond in self.conditions.iter() {
            let cond_index = indexes
                .indexes
                .get(cond.index_id as usize)
                .ok_or(FilterError)?;

            match &cond_index.index {
                index::ArchivedIndex::Empty => {}
                index::ArchivedIndex::Bitmap(bitmap) => {
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

impl Default for HeaderFilter {
    fn default() -> Self {
        Self::OnData
    }
}
