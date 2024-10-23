use std::{collections::BTreeMap, ops::RangeBounds};

use rkyv::{Archive, Deserialize, Serialize};
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
pub struct BitmapIndex {
    keys: Vec<ScalarValue>,
    values: Vec<Vec<u8>>,
}

#[derive(Debug, Default)]
pub struct BitmapIndexBuilder(BTreeMap<ScalarValue, RoaringBitmap>);

impl BitmapIndexBuilder {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

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
        self.0
            .iter()
            .try_fold(BitmapIndex::default(), |mut index, (key, bitmap)| {
                index.keys.push(key.clone());
                let mut out = Vec::new();
                bitmap.serialize_into(&mut out)?;
                index.values.push(out);

                Ok(index)
            })
    }
}

impl BitmapIndex {
    pub fn keys(&self) -> impl Iterator<Item = &ScalarValue> {
        self.keys.iter()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ScalarValue, &Vec<u8>)> {
        self.keys.iter().zip(self.values.iter())
    }
}

impl ArchivedBitmapIndex {
    pub fn get(&self, key: &ScalarValue) -> Option<RoaringBitmap> {
        let pos = self
            .keys
            .binary_search_by(|entry| cmp_scalar_value(entry, key))
            .ok()?;

        let value = &self.values[pos];
        RoaringBitmap::deserialize_unchecked_from(value.as_slice())
            .expect("failed to deserialize bitmap")
            .into()
    }
}

fn cmp_scalar_value(a: &ArchivedScalarValue, b: &ScalarValue) -> std::cmp::Ordering {
    match (a, b) {
        (ArchivedScalarValue::Null, ScalarValue::Null) => std::cmp::Ordering::Equal,
        (ArchivedScalarValue::Bool(a), ScalarValue::Bool(b)) => a.cmp(b),
        (ArchivedScalarValue::Int32(a), ScalarValue::Int32(b)) => a.to_native().cmp(b),
        (ArchivedScalarValue::Uint8(a), ScalarValue::Uint8(b)) => a.cmp(b),
        (ArchivedScalarValue::Uint16(a), ScalarValue::Uint16(b)) => a.to_native().cmp(b),
        (ArchivedScalarValue::Uint32(a), ScalarValue::Uint32(b)) => a.to_native().cmp(b),
        (ArchivedScalarValue::Uint64(a), ScalarValue::Uint64(b)) => a.to_native().cmp(b),
        (ArchivedScalarValue::B160(a), ScalarValue::B160(b)) => a.cmp(b),
        (ArchivedScalarValue::B256(a), ScalarValue::B256(b)) => a.cmp(b),
        (ArchivedScalarValue::B384(a), ScalarValue::B384(b)) => a.cmp(b),
        _ => std::cmp::Ordering::Greater,
    }
}

/// Data index.
#[derive(Debug, Archive, Serialize, Deserialize)]
pub enum Index {
    /// An index containing bitmap values.
    Bitmap(BitmapIndex),
    /// An empty index.
    Empty,
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

impl std::fmt::Debug for ArchivedScalarValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArchivedScalarValue::Null => write!(f, "ArchivedNull"),
            ArchivedScalarValue::Bool(v) => write!(f, "ArchivedBool({})", v),
            ArchivedScalarValue::Int32(v) => write!(f, "ArchivedInt32({})", v),
            ArchivedScalarValue::Uint8(v) => write!(f, "ArchivedUint8({})", v),
            ArchivedScalarValue::Uint16(v) => write!(f, "ArchivedUint16({})", v),
            ArchivedScalarValue::Uint32(v) => write!(f, "ArchivedUint32({})", v),
            ArchivedScalarValue::Uint64(v) => write!(f, "ArchivedUint64({})", v),
            ArchivedScalarValue::B160(v) => write!(f, "ArchivedB160(0x{})", hex::encode(v)),
            ArchivedScalarValue::B256(v) => write!(f, "ArchivedB256(0x{})", hex::encode(v)),
            ArchivedScalarValue::B384(v) => write!(f, "ArchivedB384({})", hex::encode(v)),
        }
    }
}

impl From<BitmapIndex> for Index {
    fn from(value: BitmapIndex) -> Self {
        Index::Bitmap(value)
    }
}
