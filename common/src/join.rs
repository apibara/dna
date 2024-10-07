use std::{collections::BTreeMap, ops::RangeBounds};

use rkyv::{Archive, Deserialize, Serialize};
use roaring::RoaringBitmap;

#[derive(Debug, Default, Archive, Serialize, Deserialize)]
pub struct JoinToOneIndex {
    pub keys: Vec<u32>,
    pub values: Vec<u32>,
}

#[derive(Debug, Default, Archive, Serialize, Deserialize)]
pub struct JoinToManyIndex {
    pub keys: Vec<u32>,
    pub values: Vec<Vec<u8>>,
}

/// Index to join one fragment to another.
#[derive(Debug, Archive, Serialize, Deserialize)]
pub enum JoinTo {
    One(JoinToOneIndex),
    Many(JoinToManyIndex),
}

#[derive(Debug, Default)]
pub struct JoinToOneIndexBuilder(BTreeMap<u32, u32>);

impl JoinToOneIndexBuilder {
    pub fn insert(&mut self, key: u32, value: u32) {
        self.0.insert(key, value);
    }

    pub fn build(self) -> JoinToOneIndex {
        self.0
            .iter()
            .fold(JoinToOneIndex::default(), |mut index, (key, value)| {
                index.keys.push(*key);
                index.values.push(*value);
                index
            })
    }
}

#[derive(Debug, Default)]
pub struct JoinToManyIndexBuilder(BTreeMap<u32, RoaringBitmap>);

impl JoinToManyIndexBuilder {
    pub fn insert(&mut self, key: u32, value: u32) {
        self.0.entry(key).or_default().insert(value);
    }

    pub fn insert_range<R>(&mut self, key: u32, range: R)
    where
        R: RangeBounds<u32>,
    {
        self.0.entry(key).or_default().insert_range(range);
    }

    pub fn build(self) -> std::io::Result<JoinToManyIndex> {
        self.0
            .iter()
            .try_fold(JoinToManyIndex::default(), |mut index, (key, value)| {
                index.keys.push(*key);
                let mut out = Vec::new();
                value.serialize_into(&mut out)?;
                index.values.push(out);

                Ok(index)
            })
    }
}

impl ArchivedJoinToOneIndex {
    pub fn get(&self, key: &u32) -> Option<u32> {
        let pos = self
            .keys
            .binary_search_by(|entry| entry.to_native().cmp(key))
            .ok()?;

        let value = &self.values[pos];
        Some(value.to_native())
    }
}

impl ArchivedJoinToManyIndex {
    pub fn get(&self, key: &u32) -> Option<RoaringBitmap> {
        let pos = self
            .keys
            .binary_search_by(|entry| entry.to_native().cmp(key))
            .ok()?;

        let value = &self.values[pos];
        RoaringBitmap::deserialize_unchecked_from(value.as_slice())
            .expect("failed to deserialize bitmap")
            .into()
    }
}

impl From<JoinToOneIndex> for JoinTo {
    fn from(value: JoinToOneIndex) -> Self {
        JoinTo::One(value)
    }
}

impl From<JoinToManyIndex> for JoinTo {
    fn from(value: JoinToManyIndex) -> Self {
        JoinTo::Many(value)
    }
}
