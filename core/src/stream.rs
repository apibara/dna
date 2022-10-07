use std::{fmt::Debug, ops::Range};

/// Unique id for an input stream.
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub struct StreamId(u64);

/// Stream message sequence number.
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Hash, Eq)]
pub struct Sequence(u64);

/// A range of sequence numbers. The range is non-inclusive.
#[derive(Debug, Clone, PartialEq, Hash, Eq)]
pub struct SequenceRange(Range<u64>);

impl StreamId {
    /// Create a `StreamId` from a `u64`.
    pub fn from_u64(id: u64) -> StreamId {
        StreamId(id)
    }

    /// Returns the stream id as `u64`.
    pub fn as_u64(&self) -> u64 {
        self.0
    }

    /// Returns the stream id as bytes.
    pub fn to_bytes(&self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

impl Sequence {
    /// Create a `Sequence` from a `u64`.
    pub fn from_u64(n: u64) -> Sequence {
        Sequence(n)
    }

    /// Returns the sequence number as `u64`.
    pub fn as_u64(&self) -> u64 {
        self.0
    }

    /// Returns the sequence number immediately after.
    pub fn successor(&self) -> Sequence {
        Sequence(self.0 + 1)
    }

    /// Returns the sequence number immediately before.
    ///
    /// Notice that this will panic if called on `Sequence(0)`.
    pub fn predecessor(&self) -> Sequence {
        Sequence(self.0 - 1)
    }
}

impl SequenceRange {
    /// Creates a new sequence range.
    pub fn new_from_u64(start_index: u64, end_index: u64) -> SequenceRange {
        SequenceRange(start_index..end_index)
    }

    /// Creates a new sequence range.
    pub fn new(start_index: &Sequence, end_index: &Sequence) -> SequenceRange {
        Self::new_from_u64(start_index.as_u64(), end_index.as_u64())
    }

    /// Returns the lower bound of the range, inclusive.
    pub fn start(&self) -> Sequence {
        Sequence::from_u64(self.0.start)
    }

    /// Returns the upper bound of the range, exclusive.
    pub fn end(&self) -> Sequence {
        Sequence::from_u64(self.0.end)
    }

    /// Returns true if the range contains no items.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Iterator for SequenceRange {
    type Item = Sequence;

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(Sequence::from_u64)
    }
}

pub trait MessageData: prost::Message + Default {}

impl<T> MessageData for T where T: prost::Message + Default {}

/// Message sent over the stream.
#[derive(Debug, Clone)]
pub enum StreamMessage<D: MessageData> {
    Invalidate { sequence: Sequence },
    Data { sequence: Sequence, data: D },
}

impl<D> StreamMessage<D>
where
    D: MessageData,
{
    /// Creates a new `Invalidate` message.
    pub fn new_invalidate(sequence: Sequence) -> Self {
        Self::Invalidate { sequence }
    }

    /// Creates a new `Data` message.
    pub fn new_data(sequence: Sequence, data: D) -> Self {
        Self::Data { sequence, data }
    }

    /// Returns the sequence number associated with the message.
    pub fn sequence(&self) -> &Sequence {
        match self {
            Self::Invalidate { sequence } => sequence,
            Self::Data { sequence, .. } => sequence,
        }
    }

    /// Returns true if it's a data message.
    pub fn is_data(&self) -> bool {
        matches!(self, Self::Data { .. })
    }

    /// Returns true if it's an invalidate message.
    pub fn is_invalidate(&self) -> bool {
        matches!(self, Self::Invalidate { .. })
    }
}
