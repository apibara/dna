use std::collections::BTreeMap;

use apibara_dna_common::segment::convert_bitmap_map;
use roaring::RoaringBitmap;

use super::store;

#[derive(Default, Debug)]
pub struct Index {
    pub event_by_address: BTreeMap<store::FieldElement, RoaringBitmap>,
    pub event_by_key_0: BTreeMap<store::FieldElement, RoaringBitmap>,
}

impl Index {
    pub fn merge(&mut self, other: Index) {
        for (address, bitmap) in other.event_by_address {
            let entry = self.event_by_address.entry(address).or_default();
            *entry |= bitmap;
        }

        for (key_0, bitmap) in other.event_by_key_0 {
            let entry = self.event_by_key_0.entry(key_0).or_default();
            *entry |= bitmap;
        }
    }
}

impl TryFrom<Index> for store::Index {
    type Error = std::io::Error;

    fn try_from(index: Index) -> Result<store::Index, std::io::Error> {
        let event_by_address = convert_bitmap_map(index.event_by_address)?;
        let event_by_key_0 = convert_bitmap_map(index.event_by_key_0)?;
        Ok(store::Index {
            event_by_address,
            event_by_key_0,
        })
    }
}
