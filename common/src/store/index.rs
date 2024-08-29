use error_stack::{Result, ResultExt};
use rkyv::{validation::validators::DefaultValidator, Archive, Deserialize, Serialize};

use super::bitmap::{ArchivedBitmapMap, BitmapMap};

#[derive(Debug, Clone)]
pub struct IndexError;

/// A collection of tagged indices.
#[derive(Archive, Serialize, Deserialize, Default)]
#[archive(check_bytes)]
pub struct IndexGroup {
    pub tags: Vec<u8>,
    pub key_sizes: Vec<usize>,
    pub data: Vec<Vec<u8>>,
}

pub trait TaggedIndex {
    type Key: rkyv::Archive + Ord;

    fn tag() -> u8;
    fn key_size() -> usize;
    fn name() -> &'static str;
}

impl IndexGroup {
    pub fn add_index<TI: TaggedIndex>(
        &mut self,
        index: &BitmapMap<TI::Key>,
    ) -> Result<usize, IndexError>
    where
        TI::Key: rkyv::Serialize<rkyv::ser::serializers::AllocSerializer<0>>,
    {
        self.add_index_raw(TI::tag(), TI::key_size(), index, TI::name())
    }

    pub fn add_index_raw<K>(
        &mut self,
        tag: u8,
        key_size: usize,
        index: &BitmapMap<K>,
        name: &'static str,
    ) -> Result<usize, IndexError>
    where
        K: rkyv::Serialize<rkyv::ser::serializers::AllocSerializer<0>>,
    {
        let id = self.tags.len();
        let data = rkyv::to_bytes(index)
            .change_context(IndexError)
            .attach_printable("failed to serialize index")
            .attach_printable_lazy(|| format!("tag: {}({})", name, tag))?
            .to_vec();

        self.tags.push(tag);
        self.key_sizes.push(key_size);
        self.data.push(data);

        Ok(id)
    }

    pub fn get_archived_index<'a, TI: TaggedIndex>(
        &'a self,
    ) -> Result<&'a ArchivedBitmapMap<TI::Key>, IndexError>
    where
        <TI::Key as rkyv::Archive>::Archived: rkyv::CheckBytes<DefaultValidator<'a>>,
    {
        let pos = self
            .tags
            .iter()
            .position(|t| *t == TI::tag())
            .ok_or(IndexError)
            .attach_printable("missing index")
            .attach_printable_lazy(|| format!("tag: {}({})", TI::name(), TI::tag()))?;

        if pos >= self.tags.len() {
            return Err(IndexError)
                .attach_printable("invalid index (out of bounds)")
                .attach_printable_lazy(|| format!("tag: {}({})", TI::name(), TI::tag()));
        }

        let data = &self.data[pos];
        let archived = rkyv::check_archived_root::<BitmapMap<TI::Key>>(data).or_else(|err| {
            Err(IndexError)
                .attach_printable("failed to deserialize index")
                .attach_printable_lazy(|| format!("error: {}", err))
                .attach_printable_lazy(|| format!("tag: {}({})", TI::name(), TI::tag()))
        })?;

        Ok(archived)
    }

    pub fn get_index<'a, TI: TaggedIndex>(&'a self) -> Result<BitmapMap<TI::Key>, IndexError>
    where
        <TI::Key as rkyv::Archive>::Archived:
            rkyv::CheckBytes<DefaultValidator<'a>> + rkyv::Deserialize<TI::Key, rkyv::Infallible>,
        TI::Key: 'a,
    {
        let bitmap_map = self.get_archived_index::<TI>()?;
        bitmap_map
            .deserialize(&mut rkyv::Infallible)
            .change_context(IndexError)
            .attach_printable("failed to deserialize index")
            .attach_printable_lazy(|| format!("tag: {}({})", TI::name(), TI::tag()))
    }
}

impl error_stack::Context for IndexError {}

impl std::fmt::Display for IndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "index error")
    }
}

impl std::fmt::Debug for IndexGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let data = self
            .data
            .iter()
            .map(|d| format!("<{} bytes>", d.len()))
            .collect::<Vec<_>>();
        f.debug_struct("IndexGroup")
            .field("tags", &self.tags)
            .field("data", &data)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use crate::store::bitmap::BitmapMapBuilder;

    use super::*;

    #[derive(Archive, Serialize, Deserialize, Debug)]
    #[archive(check_bytes)]
    pub struct IndexA;

    #[derive(Archive, Serialize, Deserialize, Debug)]
    #[archive(check_bytes)]
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
        let mut index_a = BitmapMapBuilder::default();
        index_a.entry([1, 2, 3, 4]).insert(100);
        index_a.entry([0, 0, 0, 0]).insert(200);
        let index_a = index_a.into_bitmap_map().unwrap();

        let mut index_b = BitmapMapBuilder::default();
        index_b.entry(0).insert(10);
        index_b.entry(1).insert(20);
        let index_b = index_b.into_bitmap_map().unwrap();

        let mut index_group = IndexGroup::default();
        let index_a_id = index_group.add_index::<IndexA>(&index_a).unwrap();
        let index_b_id = index_group.add_index::<IndexB>(&index_b).unwrap();

        assert_eq!(index_a_id, 0);
        assert_eq!(index_b_id, 1);

        let index_a = index_group
            .get_archived_index::<IndexA>()
            .unwrap()
            .deserialize(&mut rkyv::Infallible)
            .unwrap();
        let index_b = index_group
            .get_archived_index::<IndexB>()
            .unwrap()
            .deserialize(&mut rkyv::Infallible)
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
