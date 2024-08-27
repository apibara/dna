use std::collections::BTreeMap;

use error_stack::{Result, ResultExt};
use rkyv::{collections::util::Entry, with::AsVec, Archive, Deserialize, Serialize};
use roaring::RoaringBitmap;

#[derive(Debug, Clone)]
pub struct BitmapError;

/// Serialized roaring bitmap.
#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct Bitmap(pub Vec<u8>);

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct BitmapMap<K>(#[with(AsVec)] BTreeMap<K, Bitmap>);

impl<K: Ord> BitmapMap<K> {
    /// Returns true if the map is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the number of elements in the map.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Get the bitmap for the given key.
    pub fn get_bitmap(&self, key: &K) -> Result<Option<RoaringBitmap>, BitmapError> {
        let Some(bytes) = self.0.get(key) else {
            return Ok(None);
        };

        let bitmap = RoaringBitmap::deserialize_from(bytes.0.as_slice())
            .change_context(BitmapError)
            .attach_printable("failed to deserialize bitmap")?;

        Ok(Some(bitmap))
    }
}

impl<K> ArchivedBitmapMap<K>
where
    K: Ord + rkyv::Archive,
{
    pub fn deserialize<D>(&self, deserializer: &mut D) -> Result<BitmapMap<K>, BitmapError>
    where
        D: rkyv::Fallible + ?Sized,
        D::Error: std::fmt::Display,
        K::Archived: rkyv::Deserialize<K, D>,
    {
        <Self as rkyv::Deserialize<BitmapMap<K>, D>>::deserialize(self, deserializer).or_else(
            |err| {
                Err(BitmapError)
                    .attach_printable("failed to deserialize bitmap")
                    .attach_printable_lazy(|| format!("error: {}", err))
            },
        )
    }

    pub fn iter(&self) -> impl Iterator<Item = &Entry<K::Archived, ArchivedBitmap>> {
        self.0.iter()
    }
}

impl ArchivedBitmap {
    pub fn deserialize(&self) -> Result<RoaringBitmap, BitmapError> {
        let bitmap = RoaringBitmap::deserialize_from(self.0.as_slice())
            .change_context(BitmapError)
            .attach_printable("failed to deserialize bitmap")?;
        Ok(bitmap)
    }
}

pub struct BitmapMapBuilder<K: Ord>(BTreeMap<K, RoaringBitmap>);

impl<K: Ord> BitmapMapBuilder<K> {
    pub fn entry(&mut self, key: K) -> &mut RoaringBitmap {
        self.0.entry(key).or_default()
    }

    pub fn into_bitmap_map(self) -> Result<BitmapMap<K>, BitmapError> {
        let mut result = BTreeMap::default();
        for (k, v) in self.0.into_iter() {
            let sv = v.into_bitmap()?;
            result.insert(k, sv);
        }
        Ok(BitmapMap(result))
    }
}

pub trait RoaringBitmapExt {
    fn into_bitmap(self) -> Result<Bitmap, BitmapError>;
}

impl RoaringBitmapExt for RoaringBitmap {
    fn into_bitmap(self) -> Result<Bitmap, BitmapError> {
        let mut buf = Vec::with_capacity(self.serialized_size());
        self.serialize_into(&mut buf)
            .change_context(BitmapError)
            .attach_printable("failed to serialize roaring bitmap")?;
        Ok(Bitmap(buf))
    }
}

impl<K: Ord> Default for BitmapMapBuilder<K> {
    fn default() -> Self {
        Self(BTreeMap::new())
    }
}

impl error_stack::Context for BitmapError {}

impl std::fmt::Display for BitmapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "bitmap error")
    }
}

#[cfg(test)]
mod tests {
    use rkyv::{Archive, Deserialize, Serialize};

    use super::*;

    #[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord)]
    #[archive(check_bytes)]
    pub struct Test(pub [u8; 4]);

    #[test]
    pub fn test_bitmap_map_builder() {
        let mut builder = BitmapMapBuilder::default();

        builder.entry(Test([1, 2, 3, 4])).insert(1);
        builder.entry(Test([1, 2, 3, 4])).insert(2);

        builder.entry(Test([0, 0, 0, 0])).insert(1);
        builder.entry(Test([0, 0, 0, 0])).insert(9);

        let raw = builder.into_bitmap_map().unwrap();

        let bm = raw.get_bitmap(&Test([1, 2, 3, 4])).unwrap().unwrap();
        assert!(bm.contains(1));
        assert!(!bm.contains(9));
    }
}
