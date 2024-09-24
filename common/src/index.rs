use std::{collections::HashMap, ops::RangeBounds};

use rkyv::{Archive, Deserialize, Serialize};
use roaring::RoaringBitmap;

/// Fixed-size scalar values.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ScalarValue {
    /// A null or empty value.
    Null,
    /// A boolean.
    Bool(bool),
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
#[derive(Default, Debug)]
pub struct BitmapIndex(HashMap<ScalarValue, RoaringBitmap>);

impl BitmapIndex {
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

    /// Get the bitmap for the given key.
    pub fn get(&self, key: &ScalarValue) -> Option<&RoaringBitmap> {
        self.0.get(key)
    }
}

/// Data index.
#[derive(Debug)]
pub enum Index {
    /// An index containing bitmap values.
    Bitmap(BitmapIndex),
}
