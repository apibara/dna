use std::collections::{HashMap, HashSet};

use roaring::RoaringBitmap;

pub type FilterId = u32;

#[derive(Debug)]
pub struct DataReference {
    pub index: u32,
    pub filter_ids: HashSet<FilterId>,
}

pub struct DataReferenceIter {
    index: u32,
    count: u32,
    all: HashSet<FilterId>,
    exact: HashMap<u32, HashSet<FilterId>>,
}

#[derive(Debug, Default)]
pub struct FilterMatchSet {
    /// Filters that matched all data.
    all: HashSet<FilterId>,
    /// Filters that matched exact data.
    exact: HashMap<u32, HashSet<FilterId>>,
}

#[derive(Debug)]
pub enum FilterMatch {
    All,
    Exact(RoaringBitmap),
}

impl FilterMatchSet {
    pub fn is_empty(&self) -> bool {
        self.all.is_empty() && self.exact.is_empty()
    }

    pub fn add_match(&mut self, filter_id: FilterId, match_: &FilterMatch) {
        match match_ {
            FilterMatch::All => {
                self.all.insert(filter_id);
            }
            FilterMatch::Exact(bitmap) => {
                for index in bitmap.iter() {
                    self.exact.entry(index).or_default().insert(filter_id);
                }
            }
        }
    }

    pub fn add_data_ref(&mut self, data_ref: DataReference) {
        let entry = self.exact.entry(data_ref.index).or_default();
        entry.extend(data_ref.filter_ids);
    }

    pub fn take_matches(&mut self, count: usize) -> impl Iterator<Item = DataReference> {
        let all = std::mem::take(&mut self.all);
        let exact = std::mem::take(&mut self.exact);

        DataReferenceIter {
            index: 0,
            count: count as u32,
            all,
            exact,
        }
    }
}

impl Iterator for DataReferenceIter {
    type Item = DataReference;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.count {
            return None;
        }

        if self.all.is_empty() && self.exact.is_empty() {
            return None;
        }

        if self.all.is_empty() {
            while self.index < self.count {
                if let Some(exact) = self.exact.get(&self.index) {
                    let current_index = self.index;
                    self.index += 1;
                    let filter_ids = exact.clone();

                    return Some(DataReference {
                        index: current_index,
                        filter_ids,
                    });
                }
                self.index += 1;
            }
            return None;
        }

        let current_index = self.index;
        self.index += 1;

        let mut filter_ids = HashSet::new();
        if let Some(exact) = self.exact.get(&current_index) {
            filter_ids.extend(exact);
        }
        filter_ids.extend(self.all.iter());

        Some(DataReference {
            index: current_index,
            filter_ids,
        })
    }
}

impl FilterMatch {
    pub fn reset(&mut self) {
        *self = Self::All;
    }

    pub fn clear(&mut self) {
        *self = Self::Exact(RoaringBitmap::new());
    }

    pub fn intersect(&mut self, other: &RoaringBitmap) {
        match self {
            FilterMatch::All => {
                *self = FilterMatch::Exact(other.clone());
            }
            FilterMatch::Exact(bitmap) => {
                *bitmap &= other;
            }
        }
    }
}

impl Default for FilterMatch {
    fn default() -> Self {
        Self::All
    }
}
