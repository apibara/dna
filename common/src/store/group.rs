use std::collections::HashMap;

use error_stack::{Result, ResultExt};
use rkyv::{Archive, Deserialize, Serialize};
use roaring::RoaringBitmap;

use crate::{store::bitmap::BitmapMap, Cursor};

use super::{
    bitmap::{ArchivedBitmapMap, BitmapMapBuilder, RoaringBitmapExt},
    index::IndexGroup,
    segment::Segment,
};

#[derive(Debug, Clone)]
pub struct SegmentGroupError;

#[derive(Archive, Serialize, Deserialize, Debug)]
pub struct SegmentGroup {
    /// The first block in the segment group.
    pub first_block: Cursor,
    /// The segment body.
    pub index: IndexGroup,
}

/// Builder for a segment group.
#[derive(Default)]
pub struct SegmentGroupBuilder {
    block_range: Option<(Cursor, Cursor)>,
    block_indexes: HashMap<u8, RawBitmapMapBuilder>,
}

impl SegmentGroupBuilder {
    pub fn add_segment(&mut self, segment: &Segment) -> Result<(), SegmentGroupError> {
        if self.block_range.is_none() {
            self.block_range = Some((segment.first_block.clone(), segment.first_block.clone()));
        }

        for block_data in segment.data.iter() {
            let cursor: Cursor = block_data.cursor.clone();

            let index_group = block_data
                .access_fragment::<IndexGroup>()
                .change_context(SegmentGroupError)?;

            for serialized_index in index_group.indices.iter() {
                let key_size = serialized_index.key_size.to_native();
                let bitmap = &serialized_index.data;

                let bitmap = match key_size {
                    0 => rkyv::access::<rkyv::Archived<BitmapMap<[u8; 0]>>, rkyv::rancor::Error>(
                        bitmap,
                    )
                    .map(RawArchivedBitmapMap::Len0),
                    1 => rkyv::access::<rkyv::Archived<BitmapMap<[u8; 1]>>, rkyv::rancor::Error>(
                        bitmap,
                    )
                    .map(RawArchivedBitmapMap::Len1),
                    4 => rkyv::access::<rkyv::Archived<BitmapMap<[u8; 4]>>, rkyv::rancor::Error>(
                        bitmap,
                    )
                    .map(RawArchivedBitmapMap::Len4),
                    20 => rkyv::access::<rkyv::Archived<BitmapMap<[u8; 20]>>, rkyv::rancor::Error>(
                        bitmap,
                    )
                    .map(RawArchivedBitmapMap::Len20),
                    32 => rkyv::access::<rkyv::Archived<BitmapMap<[u8; 32]>>, rkyv::rancor::Error>(
                        bitmap,
                    )
                    .map(RawArchivedBitmapMap::Len32),
                    _ => {
                        return Err(SegmentGroupError)
                            .attach_printable("failed to deserialize bitmap")
                            .attach_printable_lazy(|| format!("invalid key size: {}", key_size))
                    }
                }
                .change_context(SegmentGroupError)
                .attach_printable("failed to deserialize bitmap")
                .attach_printable_lazy(|| format!("tag: {}", serialized_index.tag))
                .attach_printable_lazy(|| format!("key size: {}", serialized_index.key_size))?;

                let index =
                    self.block_indexes
                        .entry(serialized_index.tag)
                        .or_insert(match key_size {
                            0 => RawBitmapMapBuilder::Len0(Default::default()),
                            1 => RawBitmapMapBuilder::Len1(Default::default()),
                            4 => RawBitmapMapBuilder::Len4(Default::default()),
                            20 => RawBitmapMapBuilder::Len20(Default::default()),
                            32 => RawBitmapMapBuilder::Len32(Default::default()),
                            _ => {
                                return Err(SegmentGroupError)
                                    .attach_printable("failed to deserialize bitmap")
                                    .attach_printable_lazy(|| {
                                        format!("invalid key size: {}", key_size)
                                    })
                            }
                        });

                match (bitmap, index) {
                    (RawArchivedBitmapMap::Len0(bitmap), RawBitmapMapBuilder::Len0(index)) => {
                        add_cursor_to_bitmap_map(&cursor, bitmap, index);
                    }
                    (RawArchivedBitmapMap::Len1(bitmap), RawBitmapMapBuilder::Len1(index)) => {
                        add_cursor_to_bitmap_map(&cursor, bitmap, index);
                    }
                    (RawArchivedBitmapMap::Len4(bitmap), RawBitmapMapBuilder::Len4(index)) => {
                        add_cursor_to_bitmap_map(&cursor, bitmap, index);
                    }
                    (RawArchivedBitmapMap::Len20(bitmap), RawBitmapMapBuilder::Len20(index)) => {
                        add_cursor_to_bitmap_map(&cursor, bitmap, index);
                    }
                    (RawArchivedBitmapMap::Len32(bitmap), RawBitmapMapBuilder::Len32(index)) => {
                        add_cursor_to_bitmap_map(&cursor, bitmap, index);
                    }
                    _ => {
                        return Err(SegmentGroupError)
                            .attach_printable("failed to deserialize bitmap")
                            .attach_printable_lazy(|| format!("invalid key size: {}", key_size))
                    }
                }
            }
        }

        Ok(())
    }

    pub fn build(self) -> Result<SegmentGroup, SegmentGroupError> {
        let Some((first_block, last_block)) = self.block_range else {
            return Err(SegmentGroupError)
                .attach_printable("segment group builder has no segments");
        };

        let mut index_group = IndexGroup::default();
        let block_range = (first_block.number as u32)..=(last_block.number as u32);
        let block_range = RoaringBitmap::from_iter(block_range)
            .into_bitmap()
            .change_context(SegmentGroupError)
            .attach_printable("failed to convert block range to bitmap")?;

        for (tag, index) in self.block_indexes {
            match index {
                RawBitmapMapBuilder::Len0(index) => {
                    let bitmap = index.into_bitmap_map().change_context(SegmentGroupError)?;
                    index_group
                        .add_index_raw(tag, 0, block_range.clone(), bitmap, "index size 0")
                        .change_context(SegmentGroupError)?;
                }
                RawBitmapMapBuilder::Len1(index) => {
                    let bitmap = index.into_bitmap_map().change_context(SegmentGroupError)?;
                    index_group
                        .add_index_raw(tag, 1, block_range.clone(), bitmap, "index size 1")
                        .change_context(SegmentGroupError)?;
                }
                RawBitmapMapBuilder::Len4(index) => {
                    let bitmap = index.into_bitmap_map().change_context(SegmentGroupError)?;
                    index_group
                        .add_index_raw(tag, 4, block_range.clone(), bitmap, "index size 4")
                        .change_context(SegmentGroupError)?;
                }
                RawBitmapMapBuilder::Len20(index) => {
                    let bitmap = index.into_bitmap_map().change_context(SegmentGroupError)?;
                    index_group
                        .add_index_raw(tag, 20, block_range.clone(), bitmap, "index size 20")
                        .change_context(SegmentGroupError)?;
                }
                RawBitmapMapBuilder::Len32(index) => {
                    let bitmap = index.into_bitmap_map().change_context(SegmentGroupError)?;
                    index_group
                        .add_index_raw(tag, 32, block_range.clone(), bitmap, "index size 32")
                        .change_context(SegmentGroupError)?;
                }
            };
        }

        Ok(SegmentGroup {
            first_block,
            index: index_group,
        })
    }
}

fn add_cursor_to_bitmap_map<K: Copy + Ord + rkyv::Archive<Archived = K>>(
    cursor: &Cursor,
    archived: &ArchivedBitmapMap<K>,
    index: &mut BitmapMapBuilder<K>,
) {
    for entry in archived.iter() {
        index.entry(entry.key).insert(cursor.number as u32);
    }
}

enum RawBitmapMapBuilder {
    Len0(BitmapMapBuilder<[u8; 0]>),
    Len1(BitmapMapBuilder<[u8; 1]>),
    Len4(BitmapMapBuilder<[u8; 4]>),
    Len20(BitmapMapBuilder<[u8; 20]>),
    Len32(BitmapMapBuilder<[u8; 32]>),
}

enum RawArchivedBitmapMap<'a> {
    Len0(&'a ArchivedBitmapMap<[u8; 0]>),
    Len1(&'a ArchivedBitmapMap<[u8; 1]>),
    Len4(&'a ArchivedBitmapMap<[u8; 4]>),
    Len20(&'a ArchivedBitmapMap<[u8; 20]>),
    Len32(&'a ArchivedBitmapMap<[u8; 32]>),
}

impl error_stack::Context for SegmentGroupError {}

impl std::fmt::Display for SegmentGroupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Segment group error")
    }
}

#[cfg(test)]
mod tests {
    /*
    use rkyv::{de::Pool, Archive, Deserialize, Serialize};

    use crate::{
        store::{
            bitmap::BitmapMapBuilder,
            index::{IndexGroup, TaggedIndex},
            segment::{IndexSegment, SegmentBlock},
        },
        Cursor,
    };

    #[derive(
        Archive, Serialize, Deserialize, PartialEq, Clone, Copy, Default, PartialOrd, Eq, Ord, Debug,
    )]
    pub struct Byte20(pub [u8; 20]);

    #[derive(
        Archive, Serialize, Deserialize, Default, PartialEq, Clone, Copy, PartialOrd, Eq, Ord, Debug,
    )]
    pub struct Byte32(pub [u8; 32]);

    #[derive(
        Archive, Serialize, Deserialize, PartialEq, Clone, Copy, PartialOrd, Eq, Ord, Debug,
    )]
    pub enum DU {
        Even,
        Odd,
    }

    struct IndexBytes20;
    struct IndexBytes20Again;
    struct IndexBytes32;
    struct IndexDU;
    struct IndexEmpty;

    impl TaggedIndex for IndexBytes20 {
        type Key = Byte20;

        fn tag() -> u8 {
            1
        }

        fn key_size() -> usize {
            20
        }

        fn name() -> &'static str {
            "IndexBytes20"
        }
    }

    impl TaggedIndex for IndexBytes20Again {
        type Key = Byte20;

        fn tag() -> u8 {
            2
        }

        fn key_size() -> usize {
            20
        }

        fn name() -> &'static str {
            "IndexBytes20Again"
        }
    }

    impl TaggedIndex for IndexBytes32 {
        type Key = Byte32;

        fn tag() -> u8 {
            3
        }

        fn key_size() -> usize {
            32
        }

        fn name() -> &'static str {
            "IndexBytes32"
        }
    }

    impl TaggedIndex for IndexDU {
        type Key = DU;

        fn tag() -> u8 {
            4
        }

        fn key_size() -> usize {
            1
        }

        fn name() -> &'static str {
            "IndexDU"
        }
    }

    impl TaggedIndex for IndexEmpty {
        type Key = ();

        fn tag() -> u8 {
            5
        }

        fn key_size() -> usize {
            0
        }

        fn name() -> &'static str {
            "IndexEmpty"
        }
    }

    fn create_segment(start_block: u64, indices: Vec<IndexGroup>) -> IndexSegment {
        let mut blocks = Vec::new();

        for (i, group) in indices.into_iter().enumerate() {
            blocks.push(SegmentBlock {
                cursor: Cursor::new(start_block + i as u64, Default::default()),
                data: group,
            });
        }

        IndexSegment {
            first_block: Cursor::new(start_block, Default::default()),
            blocks,
        }
    }

    fn create_index_group(block_number: u64) -> IndexGroup {
        let mut index_group = IndexGroup::default();

        {
            let mut index = BitmapMapBuilder::default();
            index.entry(Byte20([0; 20])).insert(0);
            index.entry(Byte20([0; 20])).insert(0);
            if block_number % 2 == 0 {
                index.entry(Byte20([1; 20])).insert(10);
            }
            let index = index.into_bitmap_map().unwrap();
            index_group.add_index::<IndexBytes20>(&index).unwrap();
        }

        {
            let mut index = BitmapMapBuilder::default();
            index.entry(Byte20([10; 20])).insert(0);
            index.entry(Byte20([10; 20])).insert(0);
            if block_number % 2 == 0 {
                index.entry(Byte20([11; 20])).insert(10);
            }
            let index = index.into_bitmap_map().unwrap();
            index_group.add_index::<IndexBytes20Again>(&index).unwrap();
        }

        {
            let mut index = BitmapMapBuilder::default();
            index.entry(Byte32([0; 32])).insert(0);
            index.entry(Byte32([0; 32])).insert(0);
            if block_number % 2 == 0 {
                index.entry(Byte32([1; 32])).insert(10);
            }
            let index = index.into_bitmap_map().unwrap();
            index_group.add_index::<IndexBytes32>(&index).unwrap();
        }

        {
            let mut index = BitmapMapBuilder::default();
            index.entry(DU::Odd).insert(0);
            index.entry(DU::Odd).insert(0);
            if block_number % 2 == 0 {
                index.entry(DU::Even).insert(10);
            }
            let index = index.into_bitmap_map().unwrap();
            index_group.add_index::<IndexDU>(&index).unwrap();
        }

        {
            let mut index = BitmapMapBuilder::default();
            index.entry(()).insert(0);
            index.entry(()).insert(10);
            let index = index.into_bitmap_map().unwrap();
            index_group.add_index::<IndexEmpty>(&index).unwrap();
        }

        index_group
    }
    */

    #[test]
    fn test_segment_group_builder() {
        /*
        let mut deserializer = Pool::default();

        let mut builder = super::SegmentGroupBuilder::default();

        let segment_size = 10;
        let mut block = 100;

        for _ in 0..3 {
            let mut indices = Vec::new();
            for i in 0..segment_size {
                indices.push(create_index_group(block + i));
            }
            let segment = create_segment(block, indices);
            builder.add_segment(&segment).unwrap();
            block += segment_size;
        }

        let segment_group = builder.build().unwrap();

        assert!(segment_group.first_block.number == 100);
        assert!(segment_group.index.indices.len() == 5);

        // Check key with size 20 works.
        {
            let index = segment_group
                .index
                .deserialize_index::<IndexBytes20, _>(&mut deserializer)
                .unwrap();

            // All blocks have values with this key.
            let bitmap = index.get_bitmap(&Byte20([0; 20])).unwrap().unwrap();
            assert!(bitmap.len() == segment_size * 3);

            // Only even blocks have values with this key.
            let bitmap = index.get_bitmap(&Byte20([1; 20])).unwrap().unwrap();
            for b in bitmap.iter() {
                assert!(b % 2 == 0);
            }

            // Theres no value with this key.
            let bitmap = index.get_bitmap(&Byte20([10; 20])).unwrap();
            assert!(bitmap.is_none());
        }

        // Check different key with size 20 works and is not mixed with the previous key.
        {
            let index = segment_group
                .index
                .deserialize_index::<IndexBytes20Again, _>(&mut deserializer)
                .unwrap();

            // Theres no value with this key.
            let bitmap = index.get_bitmap(&Byte20([0; 20])).unwrap();
            assert!(bitmap.is_none());

            // All blocks have values with this key.
            let bitmap = index.get_bitmap(&Byte20([10; 20])).unwrap().unwrap();
            assert!(bitmap.len() == segment_size * 3);

            // Only even blocks have values with this key.
            let bitmap = index.get_bitmap(&Byte20([11; 20])).unwrap().unwrap();
            for b in bitmap.iter() {
                assert!(b % 2 == 0);
            }
        }

        // Check key with size 32 works.
        {
            let index = segment_group
                .index
                .deserialize_index::<IndexBytes32, _>(&mut deserializer)
                .unwrap();

            // All blocks have values with this key.
            let bitmap = index.get_bitmap(&Byte32([0; 32])).unwrap().unwrap();
            assert!(bitmap.len() == segment_size * 3);

            // Only even blocks have values with this key.
            let bitmap = index.get_bitmap(&Byte32([1; 32])).unwrap().unwrap();
            for b in bitmap.iter() {
                assert!(b % 2 == 0);
            }

            // Theres no value with this key.
            let bitmap = index.get_bitmap(&Byte32([10; 32])).unwrap();
            assert!(bitmap.is_none());
        }

        // Check key with size 1 works.
        {
            let index = segment_group
                .index
                .deserialize_index::<IndexDU, _>(&mut deserializer)
                .unwrap();

            // All blocks have values with this key.
            let bitmap = index.get_bitmap(&DU::Odd).unwrap().unwrap();
            assert!(bitmap.len() == segment_size * 3);

            // Only even blocks have values with this key.
            let bitmap = index.get_bitmap(&DU::Even).unwrap().unwrap();
            for b in bitmap.iter() {
                assert!(b % 2 == 0);
            }
        }

        // Check key with size 0 works.
        {
            let index = segment_group
                .index
                .deserialize_index::<IndexEmpty, _>(&mut deserializer)
                .unwrap();

            // All blocks have values with this key.
            let bitmap = index.get_bitmap(&()).unwrap().unwrap();
            assert!(bitmap.len() == segment_size * 3);
        }
        */
        todo!();
    }
}
