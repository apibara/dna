use std::collections::HashMap;

use error_stack::{Result, ResultExt};
use rkyv::{Archive, Deserialize, Serialize};

use crate::{store::bitmap::BitmapMap, Cursor};

use super::{
    bitmap::{ArchivedBitmapMap, BitmapMapBuilder},
    index::IndexGroup,
    segment::{ArchivedSegment, Segment},
};

#[derive(Debug, Clone)]
pub struct SegmentGroupError;

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct SegmentGroup {
    /// The first block in the segment group.
    pub first_block: Cursor,
    /// The segment body.
    pub index: IndexGroup,
}

/// Builder for a segment group.
#[derive(Default)]
pub struct SegmentGroupBuilder {
    first_block: Option<Cursor>,
    block_indexes: HashMap<u8, RawBitmapMapBuilder>,
}

impl SegmentGroupBuilder {
    pub fn add_archived_segment(
        &mut self,
        segment: &ArchivedSegment<IndexGroup>,
    ) -> Result<(), SegmentGroupError> {
        let segment = segment
            .deserialize(&mut rkyv::Infallible)
            .change_context(SegmentGroupError)
            .attach_printable("failed to deserialize segment")?;
        self.add_segment(&segment)
    }

    pub fn add_segment(&mut self, segment: &Segment<IndexGroup>) -> Result<(), SegmentGroupError> {
        if self.first_block.is_none() {
            self.first_block = Some(segment.first_block.clone());
        }

        for block_data in segment.blocks.iter() {
            let cursor: Cursor = block_data.cursor.clone();

            let index_group = &block_data.data;
            for ((tag, key_size), bitmap) in index_group
                .tags
                .iter()
                .zip(index_group.key_sizes.iter())
                .zip(index_group.data.iter())
            {
                let bitmap = match key_size {
                    0 => rkyv::check_archived_root::<BitmapMap<[u8; 0]>>(bitmap)
                        .map(RawArchivedBitmapMap::Len0),
                    1 => rkyv::check_archived_root::<BitmapMap<[u8; 1]>>(bitmap)
                        .map(RawArchivedBitmapMap::Len1),
                    20 => rkyv::check_archived_root::<BitmapMap<[u8; 20]>>(bitmap)
                        .map(RawArchivedBitmapMap::Len20),
                    32 => rkyv::check_archived_root::<BitmapMap<[u8; 32]>>(bitmap)
                        .map(RawArchivedBitmapMap::Len32),
                    _ => {
                        return Err(SegmentGroupError)
                            .attach_printable("failed to deserialize bitmap")
                            .attach_printable_lazy(|| format!("invalid key size: {}", key_size))
                    }
                }
                .or_else(|err| {
                    Err(SegmentGroupError)
                        .attach_printable("failed to deserialize bitmap")
                        .attach_printable_lazy(|| format!("error: {}", err))
                })?;

                let index = self.block_indexes.entry(*tag).or_insert(match key_size {
                    0 => RawBitmapMapBuilder::Len0(Default::default()),
                    1 => RawBitmapMapBuilder::Len1(Default::default()),
                    20 => RawBitmapMapBuilder::Len20(Default::default()),
                    32 => RawBitmapMapBuilder::Len32(Default::default()),
                    _ => {
                        return Err(SegmentGroupError)
                            .attach_printable("failed to deserialize bitmap")
                            .attach_printable_lazy(|| format!("invalid key size: {}", key_size))
                    }
                });

                match (bitmap, index) {
                    (RawArchivedBitmapMap::Len0(bitmap), RawBitmapMapBuilder::Len0(index)) => {
                        add_cursor_to_bitmap_map(&cursor, bitmap, index);
                    }
                    (RawArchivedBitmapMap::Len1(bitmap), RawBitmapMapBuilder::Len1(index)) => {
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
        let Some(first_block) = self.first_block else {
            return Err(SegmentGroupError)
                .attach_printable("segment group builder has no segments");
        };

        let mut index_group = IndexGroup::default();

        for (tag, index) in self.block_indexes {
            match index {
                RawBitmapMapBuilder::Len0(index) => {
                    let bitmap = index.into_bitmap_map().change_context(SegmentGroupError)?;
                    index_group
                        .add_index_raw(tag, 0, &bitmap, "index size 0")
                        .change_context(SegmentGroupError)?;
                }
                RawBitmapMapBuilder::Len1(index) => {
                    let bitmap = index.into_bitmap_map().change_context(SegmentGroupError)?;
                    index_group
                        .add_index_raw(tag, 1, &bitmap, "index size 1")
                        .change_context(SegmentGroupError)?;
                }
                RawBitmapMapBuilder::Len20(index) => {
                    let bitmap = index.into_bitmap_map().change_context(SegmentGroupError)?;
                    index_group
                        .add_index_raw(tag, 20, &bitmap, "index size 20")
                        .change_context(SegmentGroupError)?;
                }
                RawBitmapMapBuilder::Len32(index) => {
                    let bitmap = index.into_bitmap_map().change_context(SegmentGroupError)?;
                    index_group
                        .add_index_raw(tag, 32, &bitmap, "index size 32")
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
    Len20(BitmapMapBuilder<[u8; 20]>),
    Len32(BitmapMapBuilder<[u8; 32]>),
}

enum RawArchivedBitmapMap<'a> {
    Len0(&'a ArchivedBitmapMap<[u8; 0]>),
    Len1(&'a ArchivedBitmapMap<[u8; 1]>),
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
    use rkyv::{Archive, Deserialize, Serialize};

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
    #[archive(check_bytes)]
    pub struct Byte20(pub [u8; 20]);

    #[derive(
        Archive, Serialize, Deserialize, Default, PartialEq, Clone, Copy, PartialOrd, Eq, Ord, Debug,
    )]
    #[archive(check_bytes)]
    pub struct Byte32(pub [u8; 32]);

    #[derive(
        Archive, Serialize, Deserialize, PartialEq, Clone, Copy, PartialOrd, Eq, Ord, Debug,
    )]
    #[archive(check_bytes)]
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
                cursor: Cursor::new(start_block + i as u64, Vec::new()),
                data: group,
            });
        }

        IndexSegment {
            first_block: Cursor::new(start_block, Vec::new()),
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

    #[test]
    fn test_segment_group_builder() {
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
        assert!(segment_group.index.tags.len() == 5);

        // Check key with size 20 works.
        {
            let index = segment_group.index.get_index::<IndexBytes20>().unwrap();

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
                .get_index::<IndexBytes20Again>()
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
            let index = segment_group.index.get_index::<IndexBytes32>().unwrap();

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
            let index = segment_group.index.get_index::<IndexDU>().unwrap();

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
            let index = segment_group.index.get_index::<IndexEmpty>().unwrap();

            // All blocks have values with this key.
            let bitmap = index.get_bitmap(&()).unwrap().unwrap();
            assert!(bitmap.len() == segment_size * 3);
        }
    }
}
