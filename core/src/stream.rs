use std::{fmt::Debug, ops::RangeInclusive};

/// Unique id for an input stream.
#[derive(Debug, Copy, Clone)]
pub struct StreamId(u64);

/// Stream message sequence number.
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Hash, Eq)]
pub struct Sequence(u64);

/// A range of sequence numbers. The range is inclusive.
pub type SequenceRange = RangeInclusive<Sequence>;

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

pub trait MessageData: prost::Message + Default {}

/// Message sent over the stream.
#[derive(Debug)]
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
