//! Block fragments contain pieces of block data.

use std::collections::BTreeMap;

use rkyv::{with::AsVec, Archive, Deserialize, Serialize};

use crate::index;

pub type FragmentId = u8;

pub type IndexId = u8;

/// A pre-serialized protobuf message without the `filter_ids` field.
pub type SerializedProto = Vec<u8>;

pub trait Fragment {
    /// Returns the fragment's unique ID.
    fn id(&self) -> FragmentId;
}

#[derive(Archive, Serialize, Deserialize, Debug)]
pub struct Block {
    pub header: HeaderFragment,
    pub indexes: Vec<FragmentIndexes>,
    pub body: Vec<BodyFragment>,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
pub struct HeaderFragment {
    pub data: SerializedProto,
}

/// Information needed to join the fragment with another fragment.
#[derive(Archive, Serialize, Deserialize, Debug)]
pub struct JoinIndex {
    /// Map the source index to the index in the destination index.
    pub indexes: Vec<u32>,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
pub struct Index {
    pub index_id: IndexId,
    pub index: index::Index,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
pub struct FragmentIndexes {
    pub fragment_id: FragmentId,
    pub indexes: Vec<Index>,
    // #[rkyv(with = AsVec)]
    // pub joins: BTreeMap<FragmentId, JoinIndex>,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
pub struct BodyFragment {
    /// The fragment's unique ID.
    pub id: FragmentId,
    /// The fragment's data.
    pub data: Vec<SerializedProto>,
}

impl Fragment for HeaderFragment {
    fn id(&self) -> FragmentId {
        1
    }
}

impl Fragment for BodyFragment {
    fn id(&self) -> FragmentId {
        self.id
    }
}
