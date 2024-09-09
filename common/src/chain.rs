use std::collections::BTreeMap;

use error_stack::{Result, ResultExt};
use rkyv::{with::AsVec, Archive, Deserialize, Serialize};

use crate::{Cursor, Hash};

#[derive(Clone, PartialEq, Eq, Hash, Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct BlockInfo {
    pub number: u64,
    pub hash: Hash,
    pub parent: Hash,
}

impl BlockInfo {
    pub fn cursor(&self) -> Cursor {
        Cursor {
            number: self.number,
            hash: self.hash.clone(),
        }
    }
}

/// What action to take on reconnection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconnectAction {
    /// Continue from the provided cursor.
    Continue,
    /// There was a reorg while offline. The new head is provided.
    OfflineReorg(Cursor),
    /// The provided cursor is not part of the canonical chain or any reorg.
    Unknown,
}

pub type ReorgMap = BTreeMap<Hash, Cursor>;

#[derive(Clone, PartialEq, Eq, Hash, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub struct CanonicalBlock {
    pub hash: Hash,
    #[with(AsVec)]
    pub reorgs: ReorgMap,
}

#[derive(Clone, PartialEq, Eq, Hash, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub struct ExtraReorg {
    pub block_number: u64,
    #[with(AsVec)]
    pub reorgs: ReorgMap,
}

#[derive(Clone, PartialEq, Eq, Hash, Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct CanonicalChainSegmentInfo {
    pub first_block: Cursor,
    pub last_block: Cursor,
}

#[derive(Clone, PartialEq, Eq, Hash, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub struct CanonicalChainSegment {
    pub previous_segment: Option<CanonicalChainSegmentInfo>,
    pub info: CanonicalChainSegmentInfo,
    pub canonical: Vec<CanonicalBlock>,
    pub extra_reorgs: Vec<ExtraReorg>,
}

#[derive(Debug)]
pub enum CanonicalChainError {
    Builder,
    View,
}

#[derive(Clone)]
pub enum CanonicalChainBuilder {
    Empty,
    Building {
        previous_segment: Option<CanonicalChainSegmentInfo>,
        info: CanonicalChainSegmentInfo,
        canonical: Vec<Hash>,
        reorgs: BTreeMap<u64, ReorgMap>,
    },
}

impl CanonicalChainBuilder {
    pub fn new() -> Self {
        CanonicalChainBuilder::Empty
    }

    pub fn restore_from_segment(
        segment: CanonicalChainSegment,
    ) -> Result<Self, CanonicalChainError> {
        let previous_segment = segment.previous_segment;
        let info = segment.info;
        let mut canonical = Vec::with_capacity(segment.canonical.len());
        let mut reorgs: BTreeMap<u64, ReorgMap> = BTreeMap::new();
        for (offset, canonical_block) in segment.canonical.into_iter().enumerate() {
            let block_number = info.first_block.number + offset as u64;

            canonical.push(canonical_block.hash);
            reorgs.insert(block_number, canonical_block.reorgs);
        }

        Ok(Self::Building {
            previous_segment,
            info,
            canonical,
            reorgs,
        })
    }

    pub fn info(&self) -> Option<&CanonicalChainSegmentInfo> {
        match self {
            CanonicalChainBuilder::Empty => None,
            CanonicalChainBuilder::Building { info, .. } => Some(info),
        }
    }

    /// Returns the number of blocks in the segment.
    pub fn segment_size(&self) -> usize {
        match self {
            CanonicalChainBuilder::Empty => 0,
            CanonicalChainBuilder::Building { canonical, .. } => canonical.len(),
        }
    }

    pub fn can_grow(&self, block: &BlockInfo) -> bool {
        match self {
            CanonicalChainBuilder::Empty => true,
            CanonicalChainBuilder::Building { info, .. } => {
                let last_block = &info.last_block;
                if last_block.hash.is_zero() {
                    true
                } else {
                    last_block.number + 1 == block.number && last_block.hash == block.parent
                }
            }
        }
    }

    /// Add the given block to the segment.
    pub fn grow(&mut self, block: BlockInfo) -> Result<(), CanonicalChainError> {
        if !self.can_grow(&block) {
            return Err(CanonicalChainError::Builder)
                .attach_printable("block cannot be applied to the current segment");
        }

        match self {
            CanonicalChainBuilder::Empty => {
                // Initialize the segment builder.
                let cursor = block.cursor();

                *self = CanonicalChainBuilder::Building {
                    previous_segment: None,
                    info: CanonicalChainSegmentInfo {
                        first_block: cursor.clone(),
                        last_block: cursor,
                    },
                    canonical: vec![block.hash],
                    reorgs: BTreeMap::new(),
                };

                Ok(())
            }
            CanonicalChainBuilder::Building {
                canonical, info, ..
            } => {
                info.last_block = block.cursor();
                canonical.push(block.hash);

                Ok(())
            }
        }
    }

    // Shrink the current segment to the given block.
    //
    // Returns the removed blocks.
    // Notice that by design the genesis block cannot be removed.
    pub fn shrink(&mut self, new_head: Cursor) -> Result<Vec<Cursor>, CanonicalChainError> {
        let CanonicalChainBuilder::Building {
            canonical,
            info,
            reorgs,
            ..
        } = self
        else {
            return Err(CanonicalChainError::Builder)
                .attach_printable("tried to shrink an empty segment");
        };

        if new_head.number < info.first_block.number {
            return Err(CanonicalChainError::Builder)
                .attach_printable("tried to shrink a segment to a block that is not in the segment")
                .attach_printable_lazy(|| {
                    format!("first block number: {}", info.first_block.number)
                })
                .attach_printable_lazy(|| format!("new head number: {}", new_head.number));
        }

        if new_head.number > info.last_block.number {
            return Err(CanonicalChainError::Builder)
                .attach_printable("tried to shrink a segment to a block that is not ingested yet")
                .attach_printable_lazy(|| format!("last block number: {}", info.last_block.number))
                .attach_printable_lazy(|| format!("new head number: {}", new_head.number));
        }

        let new_head_index = (new_head.number - info.first_block.number) as usize;

        if new_head_index >= canonical.len() || canonical[new_head_index] != new_head.hash {
            return Err(CanonicalChainError::Builder)
                .attach_printable("inconsistent state: tried to shrink a segment to a block that is not in the segment");
        }

        // Nothing to remove.
        if new_head_index == canonical.len() - 1 {
            return Ok(Vec::new());
        }

        let mut removed = Vec::new();
        let first_removed_block_index = new_head_index + 1;

        for (offset, hash) in canonical[first_removed_block_index..].iter().enumerate() {
            let block_number =
                info.first_block.number + (first_removed_block_index + offset) as u64;

            removed.push(Cursor {
                number: block_number,
                hash: hash.clone(),
            });

            reorgs
                .entry(block_number)
                .or_default()
                .insert(hash.clone(), new_head.clone());
        }

        info.last_block = new_head.clone();
        canonical.truncate(new_head_index + 1);

        // Sanity check.
        assert!(canonical.len() == (info.last_block.number - info.first_block.number + 1) as usize);

        Ok(removed)
    }

    // Returns the current builder's state ready for serialization.
    pub fn current_segment(&self) -> Result<CanonicalChainSegment, CanonicalChainError> {
        let CanonicalChainBuilder::Building {
            canonical,
            info,
            reorgs,
            previous_segment,
        } = self
        else {
            return Err(CanonicalChainError::Builder)
                .attach_printable("tried to take an empty segment");
        };

        let mut segment_canonical = Vec::with_capacity(canonical.len());

        let starting_block_number = info.first_block.number;

        for (offset, hash) in canonical.iter().enumerate() {
            let cursor = Cursor {
                number: starting_block_number + offset as u64,
                hash: hash.clone(),
            };

            let reorgs_at_block = reorgs.get(&cursor.number).cloned().unwrap_or_default();

            segment_canonical.push(CanonicalBlock {
                hash: hash.clone(),
                reorgs: reorgs_at_block,
            });
        }

        let extra_reorgs = reorgs
            .iter()
            .flat_map(|(block_number, reorg)| {
                if *block_number > info.last_block.number {
                    Some(ExtraReorg {
                        block_number: *block_number,
                        reorgs: reorg.clone(),
                    })
                } else {
                    None
                }
            })
            .collect();

        Ok(CanonicalChainSegment {
            previous_segment: previous_segment.clone(),
            info: info.clone(),
            canonical: segment_canonical,
            extra_reorgs,
        })
    }

    /// Take the first `size` blocks from the current segment.
    pub fn take_segment(
        &mut self,
        size: usize,
    ) -> Result<CanonicalChainSegment, CanonicalChainError> {
        let CanonicalChainBuilder::Building {
            canonical,
            info,
            reorgs,
            previous_segment,
        } = self
        else {
            return Err(CanonicalChainError::Builder)
                .attach_printable("tried to take an empty segment");
        };

        if info.last_block.number - info.first_block.number < size as u64 {
            return Err(CanonicalChainError::Builder)
                .attach_printable("tried to take a segment that is too small");
        }

        let segment_last_block_cursor = {
            let hash = canonical[size - 1].clone();
            Cursor {
                number: info.first_block.number + size as u64 - 1,
                hash,
            }
        };

        let mut segment_canonical = Vec::with_capacity(size);
        let starting_block_number = info.first_block.number;
        for (offset, hash) in canonical.drain(..size).enumerate() {
            let cursor = Cursor {
                number: starting_block_number + offset as u64,
                hash: hash.clone(),
            };

            let reorgs_at_block = reorgs.remove(&cursor.number).unwrap_or_default();

            segment_canonical.push(CanonicalBlock {
                hash,
                reorgs: reorgs_at_block,
            });
        }

        let segment_info = CanonicalChainSegmentInfo {
            first_block: info.first_block.clone(),
            last_block: segment_last_block_cursor,
        };

        let segment_previous_segment = previous_segment.clone();

        *previous_segment = Some(segment_info.clone());

        info.first_block.number += size as u64;
        info.first_block.hash = canonical[0].clone();

        Ok(CanonicalChainSegment {
            previous_segment: segment_previous_segment,
            info: segment_info,
            canonical: segment_canonical,
            extra_reorgs: Vec::new(),
        })
    }
}

impl CanonicalChainSegment {
    pub fn canonical(&self, block_number: u64) -> Result<Cursor, CanonicalChainError> {
        if block_number < self.info.first_block.number {
            return Err(CanonicalChainError::View)
                .attach_printable("block number is before the first block")
                .attach_printable_lazy(|| format!("block number: {}", block_number))
                .attach_printable_lazy(|| format!("first block: {:?}", self.info.first_block));
        }

        if block_number > self.info.last_block.number {
            return Err(CanonicalChainError::View)
                .attach_printable("block number is after the last block")
                .attach_printable_lazy(|| format!("block number: {}", block_number))
                .attach_printable_lazy(|| format!("last block: {:?}", self.info.last_block));
        }

        let offset = block_number - self.info.first_block.number;

        let canonical = &self.canonical[offset as usize];
        let cursor = Cursor::new(block_number, canonical.hash.clone());

        Ok(cursor)
    }

    pub fn reconnect(&self, cursor: Cursor) -> Result<ReconnectAction, CanonicalChainError> {
        if cursor.number < self.info.first_block.number {
            return Err(CanonicalChainError::View)
                .attach_printable("cursor is before the first block")
                .attach_printable_lazy(|| format!("cursor: {cursor:?}"))
                .attach_printable_lazy(|| format!("first block: {:?}", self.info.first_block));
        }

        if cursor.number > self.info.last_block.number {
            // The block could have been reorged while the chain shrunk.
            let Some(reorgs) = self
                .extra_reorgs
                .iter()
                .find(|r| r.block_number == cursor.number)
            else {
                return Err(CanonicalChainError::View)
                    .attach_printable("cursor is after the last block")
                    .attach_printable_lazy(|| format!("cursor: {cursor:?}"))
                    .attach_printable_lazy(|| format!("last block: {:?}", self.info.last_block));
            };

            let Some(reorg_target) = reorgs.reorgs.get(&cursor.hash).cloned() else {
                return Ok(ReconnectAction::Unknown);
            };

            return Ok(ReconnectAction::OfflineReorg(reorg_target));
        }

        let offset = cursor.number - self.info.first_block.number;

        let canonical = &self.canonical[offset as usize];

        if canonical.hash == cursor.hash {
            return Ok(ReconnectAction::Continue);
        }

        let Some(reorg_target) = canonical.reorgs.get(&cursor.hash).cloned() else {
            return Ok(ReconnectAction::Unknown);
        };

        Ok(ReconnectAction::OfflineReorg(reorg_target))
    }
}

impl Default for CanonicalChainBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl error_stack::Context for CanonicalChainError {}

impl std::fmt::Display for CanonicalChainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CanonicalChainError::Builder => write!(f, "canonical chain builder error"),
            CanonicalChainError::View => write!(f, "canonical chain view error"),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{new_test_cursor, Hash};

    use super::{BlockInfo, CanonicalChainBuilder, ReconnectAction};

    fn genesis_block(chain: u8) -> BlockInfo {
        let c = new_test_cursor(1_000, chain);
        BlockInfo {
            number: c.number,
            hash: c.hash,
            parent: Hash::default(),
        }
    }

    fn next_block(block: &BlockInfo, chain: u8) -> BlockInfo {
        let c = new_test_cursor(block.number + 1, chain);
        BlockInfo {
            number: c.number,
            hash: c.hash,
            parent: block.hash.clone(),
        }
    }

    /*
     *
     *               1_006/1     1_040/1
     *                 o - - - - - o
     *               /
     *   o - - - - o - - - - o
     * 1_000/0   1_005/0   1_010/0
     */
    #[test]
    fn test_canonical_chain_builder() {
        let mut builder = CanonicalChainBuilder::new();

        let mut block = genesis_block(0);
        builder.grow(block.clone()).unwrap();

        for _ in 0..5 {
            block = next_block(&block, 0);
            builder.grow(block.clone()).unwrap();
        }

        let checkpoint = block.clone();

        for _ in 0..5 {
            block = next_block(&block, 0);
            builder.grow(block.clone()).unwrap();
        }

        assert_eq!(builder.segment_size(), 11);

        // Can't shrink to a block that is not in the segment.
        assert!(builder.shrink(new_test_cursor(999, 0)).is_err());
        assert!(builder.shrink(new_test_cursor(1_011, 0)).is_err());

        builder.shrink(checkpoint.cursor()).unwrap();
        assert_eq!(builder.segment_size(), 6);

        // Can't grow to a block if the head has been reorged.
        block = next_block(&block, 0);
        assert!(builder.grow(block.clone()).is_err());

        block = checkpoint.clone();
        for _ in 0..35 {
            block = next_block(&block, 1);
            builder.grow(block.clone()).unwrap();
        }

        assert_eq!(builder.segment_size(), 41);

        {
            let segment = builder.current_segment().unwrap();
            assert!(segment.previous_segment.is_none());
            assert_eq!(segment.info.first_block, new_test_cursor(1_000, 0));
            assert_eq!(segment.info.last_block, new_test_cursor(1_040, 1));
            assert_eq!(segment.canonical.len(), 41);

            for (offset, canon) in segment.canonical.iter().enumerate() {
                let block_number = 1_000 + offset as u64;
                if offset < 6 {
                    assert_eq!(canon.hash, new_test_cursor(block_number, 0).hash);
                    assert!(canon.reorgs.is_empty());
                } else {
                    if offset < 11 {
                        let old_cursor = new_test_cursor(block_number, 0);
                        let reorg_target = canon.reorgs.get(&old_cursor.hash).unwrap();
                        assert_eq!(*reorg_target, checkpoint.cursor());
                    } else {
                        assert!(canon.reorgs.is_empty());
                    }
                    assert_eq!(canon.hash, new_test_cursor(block_number, 1).hash);
                }
            }

            let action = segment.reconnect(new_test_cursor(1_005, 0)).unwrap();
            assert_eq!(action, ReconnectAction::Continue);

            let action = segment.reconnect(new_test_cursor(1_006, 1)).unwrap();
            assert_eq!(action, ReconnectAction::Continue);

            let action = segment.reconnect(new_test_cursor(1_006, 0)).unwrap();
            assert_eq!(
                action,
                ReconnectAction::OfflineReorg(new_test_cursor(1_005, 0))
            );
        }

        {
            let segment = builder.take_segment(25).unwrap();
            assert!(segment.previous_segment.is_none());
            assert_eq!(segment.info.first_block, new_test_cursor(1_000, 0));
            assert_eq!(segment.info.last_block, new_test_cursor(1_024, 1));
            assert_eq!(segment.canonical.len(), 25);

            let segment = builder.current_segment().unwrap();
            let previous = segment.previous_segment.unwrap();

            assert_eq!(previous.first_block, new_test_cursor(1_000, 0));
            assert_eq!(previous.last_block, new_test_cursor(1_024, 1));
            assert_eq!(segment.info.first_block, new_test_cursor(1_025, 1));
            assert_eq!(segment.info.last_block, new_test_cursor(1_040, 1));
            assert_eq!(segment.canonical.len(), 16);
        }
    }

    /*
     *
     *               1_004/2     1_013/2
     *                 o - - - - - o
     *                 /       1_006/1 1_007/1
     *                /         o - - - o
     *               /        /
     *   o - - - - o - - - - o - - - - o
     * 1_000/0   1_003/0   1_005/0   1_010/0
     */
    #[test]
    fn test_reorg_on_top_of_reorg() {
        let mut builder = CanonicalChainBuilder::new();

        let mut block = genesis_block(0);
        builder.grow(block.clone()).unwrap();

        for _ in 0..3 {
            block = next_block(&block, 0);
            builder.grow(block.clone()).unwrap();
        }

        let first_checkpoint = block.clone();
        assert_eq!(first_checkpoint.cursor(), new_test_cursor(1_003, 0));

        for _ in 0..2 {
            block = next_block(&block, 0);
            builder.grow(block.clone()).unwrap();
        }

        let second_checkpoint = block.clone();
        assert_eq!(second_checkpoint.cursor(), new_test_cursor(1_005, 0));

        for _ in 0..5 {
            block = next_block(&block, 0);
            builder.grow(block.clone()).unwrap();
        }

        {
            let segment = builder.current_segment().unwrap();
            assert!(segment.previous_segment.is_none());
            assert_eq!(segment.info.first_block, new_test_cursor(1_000, 0));
            assert_eq!(segment.info.last_block, new_test_cursor(1_010, 0));
        }

        builder.shrink(second_checkpoint.cursor()).unwrap();

        block = second_checkpoint.clone();
        for _ in 0..2 {
            block = next_block(&block, 1);
            builder.grow(block.clone()).unwrap();
        }

        {
            let segment = builder.current_segment().unwrap();
            assert!(segment.previous_segment.is_none());
            assert_eq!(segment.info.first_block, new_test_cursor(1_000, 0));
            assert_eq!(segment.info.last_block, new_test_cursor(1_007, 1));

            for i in 0..6 {
                let block_number = 1_000 + i as u64;
                let canon = &segment.canonical[i];
                assert_eq!(canon.hash, new_test_cursor(block_number, 0).hash);
                assert!(canon.reorgs.is_empty());
            }

            for i in 6..8 {
                let block_number = 1_000 + i as u64;
                let canon = &segment.canonical[i];
                assert_eq!(canon.hash, new_test_cursor(block_number, 1).hash);
                assert_eq!(canon.reorgs.len(), 1);
                let old_block = new_test_cursor(block_number, 0);
                let target = canon.reorgs.get(&old_block.hash).unwrap();
                assert_eq!(*target, second_checkpoint.cursor());
            }
        }

        builder.shrink(first_checkpoint.cursor()).unwrap();

        {
            let segment = builder.current_segment().unwrap();
            let action = segment.reconnect(new_test_cursor(1_010, 0)).unwrap();
            assert_eq!(
                action,
                ReconnectAction::OfflineReorg(second_checkpoint.cursor())
            );
        }

        block = first_checkpoint.clone();
        for _ in 0..10 {
            block = next_block(&block, 2);
            builder.grow(block.clone()).unwrap();
        }

        {
            let segment = builder.current_segment().unwrap();
            assert!(segment.previous_segment.is_none());
            assert_eq!(segment.info.first_block, new_test_cursor(1_000, 0));
            assert_eq!(segment.info.last_block, new_test_cursor(1_013, 2));

            // Before the first checkpoint.
            for i in 0..4 {
                let block_number = 1_000 + i as u64;
                let canon = &segment.canonical[i];
                assert_eq!(canon.hash, new_test_cursor(block_number, 0).hash);
                assert!(canon.reorgs.is_empty());
            }

            // Between the first and second checkpoints.
            // These blocks have been removed by the second reorg.
            for i in 4..6 {
                let block_number = 1_000 + i as u64;
                let canon = &segment.canonical[i];
                assert_eq!(canon.hash, new_test_cursor(block_number, 2).hash);
                assert_eq!(canon.reorgs.len(), 1);
                let old_block = new_test_cursor(block_number, 0);
                let target = canon.reorgs.get(&old_block.hash).unwrap();
                assert_eq!(*target, first_checkpoint.cursor());
            }

            // After the second checkpoint.
            // These blocks have been removed by the first and second reorg.
            for i in 6..8 {
                let block_number = 1_000 + i as u64;
                let canon = &segment.canonical[i];
                assert_eq!(canon.hash, new_test_cursor(block_number, 2).hash);
                assert_eq!(canon.reorgs.len(), 2);
                {
                    let old_block = new_test_cursor(block_number, 0);
                    let target = canon.reorgs.get(&old_block.hash).unwrap();
                    assert_eq!(*target, second_checkpoint.cursor());
                }
                {
                    let old_block = new_test_cursor(block_number, 1);
                    let target = canon.reorgs.get(&old_block.hash).unwrap();
                    assert_eq!(*target, first_checkpoint.cursor());
                }
            }

            // These blocks have been removed by the first reorg.
            for i in 8..11 {
                let block_number = 1_000 + i as u64;
                let canon = &segment.canonical[i];
                assert_eq!(canon.hash, new_test_cursor(block_number, 2).hash);
                let old_block = new_test_cursor(block_number, 0);
                let target = canon.reorgs.get(&old_block.hash).unwrap();
                assert_eq!(*target, second_checkpoint.cursor());
            }

            // These blocks have never been part of a reorg.
            for i in 11..14 {
                let block_number = 1_000 + i as u64;
                let canon = &segment.canonical[i];
                assert_eq!(canon.hash, new_test_cursor(block_number, 2).hash);
                assert!(canon.reorgs.is_empty());
            }
        }
    }
}
