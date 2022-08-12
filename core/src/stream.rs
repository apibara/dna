use std::ops::RangeInclusive;

/// Unique id for an input stream.
#[derive(Debug, Copy, Clone)]
pub struct StreamId(u64);

/// Stream message sequence number.
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd)]
pub struct Sequence(u64);

/// A range of sequence numbers. The range is inclusive.
#[derive(Debug, Clone)]
pub struct SequenceRange(RangeInclusive<Sequence>);

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
}

impl SequenceRange {
    pub fn new(start: Sequence, end: Sequence) -> SequenceRange {
        let inner = RangeInclusive::new(start, end);
        SequenceRange(inner)
    }
}
