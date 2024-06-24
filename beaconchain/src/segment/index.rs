use std::collections::BTreeMap;

use apibara_dna_common::segment::convert_bitmap_map;
use roaring::RoaringBitmap;

use super::store;

#[derive(Debug, Default)]
pub struct SegmentGroupIndex {
    pub transaction_by_from_address: BTreeMap<store::Address, RoaringBitmap>,
    pub transaction_by_to_address: BTreeMap<store::Address, RoaringBitmap>,
}

impl TryFrom<SegmentGroupIndex> for store::SegmentGroupIndex {
    type Error = std::io::Error;

    fn try_from(x: SegmentGroupIndex) -> Result<Self, Self::Error> {
        let transaction_by_from_address = convert_bitmap_map(x.transaction_by_from_address)?;
        let transaction_by_to_address = convert_bitmap_map(x.transaction_by_to_address)?;
        Ok(store::SegmentGroupIndex {
            transaction_by_from_address,
            transaction_by_to_address,
        })
    }
}
