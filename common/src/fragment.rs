//! Block fragments contain pieces of block data.

use rkyv::{Archive, Deserialize, Serialize};

pub type FragmentId = u8;

/// A pre-serialized protobuf message without the `filter_ids` field.
pub type SerializedProto = Vec<u8>;

pub trait Fragment {
    /// Returns the fragment's unique ID.
    fn id(&self) -> FragmentId;

    fn byte_size(&self) -> usize;
}

#[derive(Archive, Serialize, Deserialize, Debug)]
pub struct Block {
    pub index: IndexFragment,
    pub header: HeaderFragment,
    pub body: Vec<BodyFragment>,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
pub struct HeaderFragment {
    pub data: SerializedProto,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
pub struct IndexFragment {}

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

    fn byte_size(&self) -> usize {
        self.data.len()
    }
}

impl Fragment for IndexFragment {
    fn id(&self) -> FragmentId {
        0
    }

    fn byte_size(&self) -> usize {
        0
    }
}

impl Fragment for BodyFragment {
    fn id(&self) -> FragmentId {
        self.id
    }

    fn byte_size(&self) -> usize {
        self.data.iter().map(Vec::len).sum()
    }
}

impl Block {
    pub fn byte_size(&self) -> usize {
        self.index.byte_size()
            + self.header.byte_size()
            + self.body.iter().map(Fragment::byte_size).sum::<usize>()
    }
}
