use std::collections::{BTreeMap, HashSet};

use roaring::RoaringBitmap;

use crate::query::{BlockFilter, FilterId};

pub trait BlockFilterFactory {
    fn create_block_filter(
        &self,
        filters: &[Vec<u8>],
    ) -> tonic::Result<Vec<BlockFilter>, tonic::Status>;
}

#[derive(Debug, Default)]
pub struct FilterMatch(BTreeMap<u32, HashSet<FilterId>>);

#[derive(Debug)]
pub struct Match {
    pub index: u32,
    pub filter_ids: Vec<FilterId>,
}

impl FilterMatch {
    pub fn clear(&mut self) {
        self.0.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn add_match(&mut self, filter_id: FilterId, bitmap: &RoaringBitmap) {
        for index in bitmap.iter() {
            self.0.entry(index).or_default().insert(filter_id);
        }
    }

    pub fn add_single_match(&mut self, filter_id: FilterId, index: u32) {
        self.0.entry(index).or_default().insert(filter_id);
    }

    pub fn iter(&self) -> impl Iterator<Item = Match> + '_ {
        self.0.iter().map(|(index, filter_ids)| Match {
            index: *index,
            filter_ids: filter_ids.iter().copied().collect(),
        })
    }
}
