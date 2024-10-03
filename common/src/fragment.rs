//! Block fragments contain pieces of block data.

use rkyv::{Archive, Deserialize, Serialize};

use crate::{index, join};

pub const INDEX_FRAGMENT_ID: FragmentId = 0;
pub const INDEX_FRAGMENT_NAME: &str = "index";

pub const JOIN_FRAGMENT_ID: FragmentId = u8::MAX;
pub const JOIN_FRAGMENT_NAME: &str = "join";

pub const HEADER_FRAGMENT_ID: FragmentId = 1;
pub const HEADER_FRAGMENT_NAME: &str = "header";

pub type FragmentId = u8;

pub type IndexId = u8;

/// Information about a fragment.
#[derive(Debug, Clone)]
pub struct FragmentInfo {
    /// The fragment's unique ID.
    pub fragment_id: FragmentId,
    /// The fragment's name.
    pub name: String,
}

/// A pre-serialized protobuf message without the `filter_ids` field.
pub type SerializedProto = Vec<u8>;

#[derive(Archive, Serialize, Deserialize, Debug)]
pub struct Block {
    pub header: HeaderFragment,
    pub index: IndexGroupFragment,
    pub join: JoinGroupFragment,
    pub body: Vec<BodyFragment>,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
pub struct IndexGroupFragment {
    pub indexes: Vec<IndexFragment>,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
pub struct JoinGroupFragment {
    pub joins: Vec<JoinFragment>,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
pub struct HeaderFragment {
    pub data: SerializedProto,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
pub struct IndexFragment {
    pub fragment_id: FragmentId,
    pub range_start: u32,
    pub range_len: u32,
    pub indexes: Vec<Index>,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
pub struct JoinFragment {
    pub fragment_id: FragmentId,
    pub joins: Vec<Join>,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
pub struct BodyFragment {
    /// The fragment's unique ID.
    pub fragment_id: FragmentId,
    /// The fragment's name.
    pub name: String,
    /// The fragment's data.
    pub data: Vec<SerializedProto>,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
pub struct Index {
    pub index_id: IndexId,
    pub index: index::Index,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
pub struct Join {
    pub to_fragment_id: FragmentId,
    pub index: join::JoinTo,
}

impl IndexGroupFragment {
    pub fn is_empty(&self) -> bool {
        self.indexes.is_empty()
    }

    pub fn len(&self) -> usize {
        self.indexes.len()
    }
}
