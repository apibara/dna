use error_stack::{Result, ResultExt};
use rkyv::{rancor::Strategy, Archive, Archived, Deserialize, Portable, Serialize};

use crate::rkyv::{Aligned, Checked, Serializable};

use super::{
    bitmap::{ArchivedBitmapMap, Bitmap, BitmapMap},
    fragment::Fragment,
};

#[derive(Debug, Clone)]
pub struct IndexError;

#[derive(Archive, Serialize, Deserialize)]
pub struct SerializedIndex {
    pub tag: u8,
    pub key_size: usize,
    pub range: Bitmap,
    #[rkyv(with = Aligned<16>)]
    pub data: Vec<u8>,
}

/// A collection of tagged indices.
#[derive(Archive, Serialize, Deserialize, Debug, Default)]
pub struct IndexGroup {
    pub indices: Vec<SerializedIndex>,
}

pub trait TaggedIndex {
    type Key: rkyv::Archive + Ord;

    fn tag() -> u8;
    fn key_size() -> usize;
    fn name() -> &'static str;
}

impl Fragment for IndexGroup {
    fn name() -> &'static str {
        "index"
    }

    fn tag() -> u8 {
        0
    }
}

impl IndexGroup {
    pub fn add_index<TI: TaggedIndex>(
        &mut self,
        range: Bitmap,
        index: BitmapMap<TI::Key>,
    ) -> Result<usize, IndexError>
    where
        TI::Key: for<'a> Serializable<'a>,
    {
        self.add_index_raw(TI::tag(), TI::key_size(), range, index, TI::name())
    }

    pub fn add_index_raw<K>(
        &mut self,
        tag: u8,
        key_size: usize,
        range: Bitmap,
        index: BitmapMap<K>,
        name: &'static str,
    ) -> Result<usize, IndexError>
    where
        K: for<'a> Serializable<'a>,
    {
        let id = self.indices.len();
        let data = rkyv::to_bytes(&index)
            .change_context(IndexError)
            .attach_printable("failed to serialize index")
            .attach_printable_lazy(|| format!("tag: {}({})", name, tag))?
            .to_vec();

        self.indices.push(SerializedIndex {
            tag,
            key_size,
            range,
            data,
        });

        Ok(id)
    }

    pub fn access_archived_index<'a, TI: TaggedIndex>(
        &'a self,
    ) -> Result<&'a ArchivedBitmapMap<TI::Key>, IndexError>
    where
        <TI::Key as rkyv::Archive>::Archived: Portable + for<'b> Checked<'b>,
    {
        let Some(serialized) = self.indices.iter().find(|i| i.tag == TI::tag()) else {
            return Err(IndexError)
                .attach_printable("missing index")
                .attach_printable_lazy(|| format!("tag: {}({})", TI::name(), TI::tag()));
        };

        let archived = rkyv::access::<Archived<BitmapMap<TI::Key>>, _>(&serialized.data)
            .change_context(IndexError)
            .attach_printable("failed to deserialize index")
            .attach_printable_lazy(|| format!("tag: {}({})", TI::name(), TI::tag()))?;

        Ok(archived)
    }

    pub fn deserialize_index<'a, TI, D>(
        &'a self,
        deserializer: &mut D,
    ) -> Result<BitmapMap<TI::Key>, IndexError>
    where
        TI: TaggedIndex,
        <TI::Key as rkyv::Archive>::Archived:
            Portable + Deserialize<TI::Key, Strategy<D, rkyv::rancor::Error>> + for<'b> Checked<'b>,
        <Self as Archive>::Archived: Deserialize<Self, Strategy<D, rkyv::rancor::Error>>,
    {
        let bitmap_map = self.access_archived_index::<TI>()?;
        rkyv::api::deserialize_using::<BitmapMap<TI::Key>, _, rkyv::rancor::Error>(
            bitmap_map,
            deserializer,
        )
        .change_context(IndexError)
        .attach_printable("failed to deserialize index")
        .attach_printable_lazy(|| format!("tag: {}({})", TI::name(), TI::tag()))
    }
}

impl ArchivedIndexGroup {
    pub fn access_archived_index<'a, TI: TaggedIndex>(
        &'a self,
    ) -> Result<Option<&'a ArchivedBitmapMap<TI::Key>>, IndexError>
    where
        <TI::Key as rkyv::Archive>::Archived: Portable + for<'b> Checked<'b>,
    {
        let Some(serialized) = self.indices.iter().find(|i| i.tag == TI::tag()) else {
            return Ok(None);
        };

        let archived = rkyv::access::<Archived<BitmapMap<TI::Key>>, _>(&serialized.data)
            .change_context(IndexError)
            .attach_printable("failed to deserialize index")
            .attach_printable_lazy(|| format!("tag: {}({})", TI::name(), TI::tag()))?;

        Ok(Some(archived))
    }

    pub fn deserialize_index<'a, TI, D>(
        &'a self,
        deserializer: &mut D,
    ) -> Result<Option<BitmapMap<TI::Key>>, IndexError>
    where
        TI: TaggedIndex,
        TI::Key: 'a,
        <TI::Key as rkyv::Archive>::Archived: Portable + for<'b> Checked<'b>,
        // <TI::Key as rkyv::Archive>::Archived:
        //     Portable + Deserialize<TI::Key, Strategy<D, rkyv::rancor::Error>> + for<'b> Checked<'b>,
        <BitmapMap<TI::Key> as Archive>::Archived:
            Deserialize<BitmapMap<TI::Key>, Strategy<D, rkyv::rancor::Error>>,
    {
        let Some(bitmap_map) = self.access_archived_index::<TI>()? else {
            return Ok(None);
        };

        rkyv::api::deserialize_using::<BitmapMap<TI::Key>, _, rkyv::rancor::Error>(
            bitmap_map,
            deserializer,
        )
        .change_context(IndexError)
        .attach_printable("failed to deserialize index")
        .attach_printable_lazy(|| format!("tag: {}({})", TI::name(), TI::tag()))
        .map(Some)
    }
}

impl error_stack::Context for IndexError {}

impl std::fmt::Display for IndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "index error")
    }
}

impl std::fmt::Debug for SerializedIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SerializedIndex")
            .field("tag", &self.tag)
            .field("key_size", &self.key_size)
            .field("data", &format!("<{} bytes>", self.data.len()))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use rkyv::de::Pool;
    use roaring::RoaringBitmap;

    use crate::store::bitmap::{BitmapMapBuilder, RoaringBitmapExt};

    use super::*;

    #[derive(Archive, Serialize, Deserialize, Debug)]
    pub struct IndexA;

    #[derive(Archive, Serialize, Deserialize, Debug)]
    pub struct IndexB;

    impl TaggedIndex for IndexA {
        type Key = [u8; 4];

        fn tag() -> u8 {
            1
        }

        fn key_size() -> usize {
            4
        }

        fn name() -> &'static str {
            "IndexA"
        }
    }

    impl TaggedIndex for IndexB {
        type Key = u64;

        fn tag() -> u8 {
            2
        }

        fn key_size() -> usize {
            8
        }

        fn name() -> &'static str {
            "IndexB"
        }
    }

    #[test]
    fn test_index_group() {
        let mut deserializer = Pool::default();

        let mut index_a = BitmapMapBuilder::default();
        index_a.entry([1, 2, 3, 4]).insert(100);
        index_a.entry([0, 0, 0, 0]).insert(200);
        let index_a = index_a.into_bitmap_map().unwrap();

        let mut index_b = BitmapMapBuilder::default();
        index_b.entry(0).insert(10);
        index_b.entry(1).insert(20);
        let index_b = index_b.into_bitmap_map().unwrap();

        let mut index_group = IndexGroup::default();
        let range_a = RoaringBitmap::from_iter(100..200).into_bitmap().unwrap();
        let range_b = RoaringBitmap::from_iter(10..20).into_bitmap().unwrap();

        let index_a_id = index_group.add_index::<IndexA>(range_a, index_a).unwrap();
        let index_b_id = index_group.add_index::<IndexB>(range_b, index_b).unwrap();

        assert_eq!(index_a_id, 0);
        assert_eq!(index_b_id, 1);

        let index_a = index_group
            .access_archived_index::<IndexA>()
            .unwrap()
            .deserialize(&mut deserializer)
            .unwrap();
        let index_b = index_group
            .access_archived_index::<IndexB>()
            .unwrap()
            .deserialize(&mut deserializer)
            .unwrap();

        assert!(index_a
            .get_bitmap(&[1, 2, 3, 4])
            .unwrap()
            .unwrap()
            .contains(100));
        assert!(index_a
            .get_bitmap(&[0, 0, 0, 0])
            .unwrap()
            .unwrap()
            .contains(200));
        assert!(index_a.get_bitmap(&[9, 0, 0, 9]).unwrap().is_none());
        assert!(index_b.get_bitmap(&0).unwrap().unwrap().contains(10));
        assert!(index_b.get_bitmap(&1).unwrap().unwrap().contains(20));
    }
}
