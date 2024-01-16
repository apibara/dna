use std::mem;

/// A segment is a flat file containing a sequence of blocks.
///
/// Segments can built in-memory by appending blocks to a `SegmentBuilder`.
use crate::error::Result;

/// A segment is a flat structure containing a sequence of blocks.
///
/// Segments can be merged together to form a larger segment.
/// Notice that ordering must be preserved when merging segments
/// together and no gaps are allowed.
pub trait Segment: Default {
    /// Append the `other` segment to the current segment.
    fn append(&mut self, other: Self) -> Result<()>;
}

pub struct SegmentBuilder<S: Segment> {
    segment: S,
}

impl<S> SegmentBuilder<S>
where
    S: Segment,
{
    /// Creates a new [SegmentBuilder].
    pub fn new() -> Self {
        Self {
            segment: S::default(),
        }
    }

    /// Append a segment to the current [Segment].
    pub fn append(&mut self, segment: S) -> Result<()> {
        self.segment.append(segment)?;
        Ok(())
    }

    /// Returns the current [Segment] and resets the builder.
    pub fn flush(&mut self) -> Result<S> {
        let current = mem::take(&mut self.segment);
        Ok(current)
    }
}
