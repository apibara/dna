use std::io::Cursor;

use apibara_dna_common::{
    error::{DnaError, Result},
    segment::SegmentOptions,
    storage::StorageBackend,
};
use error_stack::ResultExt;
use flatbuffers::{Follow, Verifiable};

use crate::segment::store;

/// Read a block header segment.
pub struct BlockHeaderSegmentReader<S: StorageBackend>(SegmentReader<S>);

/// Read a log segment.
pub struct LogSegmentReader<S: StorageBackend>(SegmentReader<S>);

impl<S> BlockHeaderSegmentReader<S>
where
    S: StorageBackend,
    <S as StorageBackend>::Reader: Unpin,
{
    pub fn new(storage: S, segment_options: SegmentOptions, buffer_size: usize) -> Self {
        let inner = SegmentReader::new(storage, segment_options, buffer_size);
        Self(inner)
    }

    pub async fn read<'a>(
        &'a mut self,
        segment_start: u64,
    ) -> Result<store::BlockHeaderSegment<'a>> {
        self.0
            .read::<store::BlockHeaderSegment>(segment_start, "header")
            .await
    }
}

impl<S> LogSegmentReader<S>
where
    S: StorageBackend,
    <S as StorageBackend>::Reader: Unpin,
{
    pub fn new(storage: S, segment_options: SegmentOptions, buffer_size: usize) -> Self {
        let inner = SegmentReader::new(storage, segment_options, buffer_size);
        Self(inner)
    }

    pub async fn read<'a>(&'a mut self, segment_start: u64) -> Result<store::LogSegment<'a>> {
        self.0.read::<store::LogSegment>(segment_start, "log").await
    }
}

pub struct SegmentReader<S: StorageBackend> {
    storage: S,
    segment_options: SegmentOptions,
    buffer: Vec<u8>,
}

impl<S> SegmentReader<S>
where
    S: StorageBackend,
    <S as StorageBackend>::Reader: Unpin,
{
    pub fn new(storage: S, segment_options: SegmentOptions, buffer_size: usize) -> Self {
        let buffer = vec![0; buffer_size];
        Self {
            storage,
            segment_options,
            buffer,
        }
    }

    pub async fn read<'a, T>(
        &'a mut self,
        segment_start: u64,
        filename: &str,
    ) -> Result<<T as Follow>::Inner>
    where
        T: 'a + Follow<'a> + Verifiable,
    {
        let segment_name = self.segment_options.format_segment_name(segment_start);

        let mut reader = self
            .storage
            .get(format!("segment/{segment_name}"), filename)
            .await?;

        let len = {
            let mut cursor = Cursor::new(&mut self.buffer[..]);
            tokio::io::copy(&mut reader, &mut cursor)
                .await
                .change_context(DnaError::Io)? as usize
        };

        let segment = flatbuffers::root::<T>(&self.buffer[..len])
            .change_context(DnaError::Fatal)
            .attach_printable_lazy(|| format!("failed to read segment {}", segment_name))?;

        Ok(segment)
    }
}
