//! Read a snapshot segment.
use std::{marker::PhantomData, ops::Range};

use reth_rlp::Decodable;

use super::{
    decompress::{Decompressor, DecompressorError, Getter},
    file::SnapshotFileInfo,
    index::{Index, IndexError},
};

pub trait SegmentItem: Sized {
    fn decode(buf: &[u8]) -> Result<Self, SegmentError>;
}

/// Represents a single segment.
pub struct Segment<T: SegmentItem> {
    decompressor: Decompressor,
    index: Index,
    range: Range<u64>,
    _phantom: PhantomData<T>,
}

pub struct SegmentReader<'a, T: SegmentItem> {
    getter: Getter<'a>,
    segment: &'a Segment<T>,
    buffer: Vec<u8>,
}

/// Error related to [Segment].
#[derive(Debug, thiserror::Error)]
pub enum SegmentError {
    #[error(transparent)]
    Decompressor(#[from] DecompressorError),
    #[error(transparent)]
    Index(#[from] IndexError),
    #[error("the given block is out of range")]
    IndexOutOfRange,
    #[error("failed to decode item: {0}")]
    Decode(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl<T> Segment<T>
where
    T: SegmentItem,
{
    pub fn new(info: SnapshotFileInfo) -> Result<Self, SegmentError> {
        let decompressor = Decompressor::new(info.segment_path())?;
        let index = Index::new(info.index_path())?;

        let segment = Self {
            decompressor,
            index,
            range: info.range(),
            _phantom: PhantomData::default(),
        };

        Ok(segment)
    }

    pub fn contains(&self, block_number: u64) -> bool {
        self.range.contains(&block_number)
    }

    pub fn new_reader(&self, buffer_size: usize) -> SegmentReader<T> {
        let getter = self.decompressor.getter();
        let buffer = vec![0u8; buffer_size];
        SegmentReader {
            getter,
            segment: self,
            buffer,
        }
    }

    fn ordinal_lookup(&self, block: u64) -> Option<u64> {
        // TODO: check if the block is in range
        let relative_index = block - self.index.base_data_id();
        self.index.ordinal_lookup(relative_index)
    }
}

impl<'a, T> SegmentReader<'a, T>
where
    T: SegmentItem,
{
    pub fn reset(&mut self, block: u64) -> Result<(), SegmentError> {
        let index = self
            .segment
            .ordinal_lookup(block)
            .ok_or(SegmentError::IndexOutOfRange)?;
        self.getter.reset(index as usize);

        Ok(())
    }

    pub fn read_next(&mut self) -> Result<Option<T>, SegmentError> {
        if let Some(size) = self.getter.next_word(&mut self.buffer) {
            let decoded = T::decode(&self.buffer[..size])?;
            Ok(Some(decoded))
        } else {
            Ok(None)
        }
    }
}

impl<'a, T> Iterator for SegmentReader<'a, T>
where
    T: SegmentItem,
{
    type Item = Result<T, SegmentError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read_next().transpose()
    }
}

impl<T> SegmentItem for T
where
    T: Decodable,
{
    fn decode(buffer: &[u8]) -> Result<T, SegmentError> {
        // first byte is the first byte of the block header hash.
        <T as Decodable>::decode(&mut &buffer[1..])
            .map_err(|err| SegmentError::Decode(Box::new(err)))
    }
}
