use std::{collections::BTreeMap, ops::RangeBounds};

use rkyv::{with::AsVec, Archive, Deserialize, Serialize};
use roaring::RoaringBitmap;

/// Fixed-size scalar values.
#[derive(Archive, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ScalarValue {
    /// A null or empty value.
    Null,
    /// A boolean.
    Bool(bool),
    /// A signed integer with 32 bits.
    Int32(i32),
    /// An unsigned integer with 8 bits.
    Uint8(u8),
    /// An unsigned integer with 16 bits.
    Uint16(u16),
    /// An unsigned integer with 32 bits.
    Uint32(u32),
    /// An unsigned integer with 64 bits.
    Uint64(u64),
    /// A byte array with 20 elements.
    B160([u8; 20]),
    /// A byte array with 32 elements.
    B256([u8; 32]),
    /// A byte array with 48 elements.
    B384([u8; 48]),
}

/// Map scalar values to bitmaps.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Default)]
pub struct BitmapIndex(#[rkyv(with = AsVec)] BTreeMap<ScalarValue, Vec<u8>>);

#[derive(Debug, Default)]
pub struct BitmapIndexBuilder(BTreeMap<ScalarValue, RoaringBitmap>);

impl BitmapIndexBuilder {
    /// Insert a value in the index.
    pub fn insert(&mut self, key: ScalarValue, value: u32) {
        self.0.entry(key).or_default().insert(value);
    }

    /// Insert a range of values in the index.
    pub fn insert_range<R>(&mut self, key: ScalarValue, range: R)
    where
        R: RangeBounds<u32>,
    {
        self.0.entry(key).or_default().insert_range(range);
    }

    pub fn build(&self) -> std::io::Result<BitmapIndex> {
        let key_values = self
            .0
            .iter()
            .map(|(k, v)| {
                let mut out = Vec::new();
                v.serialize_into(&mut out)?;
                Ok((k.clone(), out))
            })
            .collect::<std::io::Result<Vec<_>>>()?;

        let inner = BTreeMap::from_iter(key_values);

        Ok(BitmapIndex(inner))
    }
}

impl BitmapIndex {
    pub fn keys(&self) -> impl Iterator<Item = &ScalarValue> {
        self.0.keys()
    }

    pub fn get(&self, key: &ScalarValue) -> Option<RoaringBitmap> {
        let bytes = self.0.get(key)?;
        RoaringBitmap::deserialize_from(bytes.as_slice())
            .expect("failed to deserialize bitmap")
            .into()
    }
}

/// Data index.
#[derive(Debug, Archive, Serialize, Deserialize)]
pub enum Index {
    /// An index containing bitmap values.
    Bitmap(BitmapIndex),
}

impl std::fmt::Debug for ScalarValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScalarValue::Null => write!(f, "Null"),
            ScalarValue::Bool(v) => write!(f, "Bool({})", v),
            ScalarValue::Int32(v) => write!(f, "Int32({})", v),
            ScalarValue::Uint8(v) => write!(f, "Uint8({})", v),
            ScalarValue::Uint16(v) => write!(f, "Uint16({})", v),
            ScalarValue::Uint32(v) => write!(f, "Uint32({})", v),
            ScalarValue::Uint64(v) => write!(f, "Uint64({})", v),
            ScalarValue::B160(v) => write!(f, "B160(0x{})", hex::encode(v)),
            ScalarValue::B256(v) => write!(f, "B256(0x{})", hex::encode(v)),
            ScalarValue::B384(v) => write!(f, "B384({})", hex::encode(v)),
        }
    }
}

impl From<BitmapIndex> for Index {
    fn from(value: BitmapIndex) -> Self {
        Index::Bitmap(value)
    }
}
