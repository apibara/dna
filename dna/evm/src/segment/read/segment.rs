use std::io::Cursor;

use apibara_dna_common::{
    error::{DnaError, Result},
    segment::SegmentOptions,
    storage::StorageBackend,
};
use error_stack::ResultExt;
use flatbuffers::{Follow, Verifiable, VerifierOptions};

use crate::segment::store;

/// Read a block header segment.
pub struct BlockHeaderSegmentReader<S: StorageBackend>(SegmentReader<S>);

/// Read a log segment.
pub struct LogSegmentReader<S: StorageBackend>(SegmentReader<S>);

/// Read a transaction segment.
pub struct TransactionSegmentReader<S: StorageBackend>(SegmentReader<S>);

/// Read a transaction receipt segment.
pub struct ReceiptSegmentReader<S: StorageBackend>(SegmentReader<S>);

impl<S> BlockHeaderSegmentReader<S>
where
    S: StorageBackend,
    <S as StorageBackend>::Reader: Unpin,
{
    pub fn new(storage: S, segment_options: SegmentOptions, buffer_size: usize) -> Self {
        let inner = SegmentReader::new(storage, segment_options, buffer_size);
        Self(inner)
    }

    pub async fn read(&mut self, segment_start: u64) -> Result<store::BlockHeaderSegment<'_>> {
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
        let inner = SegmentReader::new(storage, segment_options, buffer_size)
            .with_verifier_options(VerifierOptions {
                max_tables: 10_000_000,
                ..Default::default()
            });
        Self(inner)
    }

    pub async fn read(&mut self, segment_start: u64) -> Result<store::LogSegment<'_>> {
        self.0.read::<store::LogSegment>(segment_start, "log").await
    }
}

impl<S> TransactionSegmentReader<S>
where
    S: StorageBackend,
    <S as StorageBackend>::Reader: Unpin,
{
    pub fn new(storage: S, segment_options: SegmentOptions, buffer_size: usize) -> Self {
        let inner = SegmentReader::new(storage, segment_options, buffer_size);
        Self(inner)
    }

    pub async fn read(&mut self, segment_start: u64) -> Result<store::TransactionSegment<'_>> {
        self.0
            .read::<store::TransactionSegment>(segment_start, "transaction")
            .await
    }
}

impl<S> ReceiptSegmentReader<S>
where
    S: StorageBackend,
    <S as StorageBackend>::Reader: Unpin,
{
    pub fn new(storage: S, segment_options: SegmentOptions, buffer_size: usize) -> Self {
        let inner = SegmentReader::new(storage, segment_options, buffer_size);
        Self(inner)
    }

    pub async fn read(&mut self, segment_start: u64) -> Result<store::ReceiptSegment<'_>> {
        self.0
            .read::<store::ReceiptSegment>(segment_start, "receipt")
            .await
    }
}

pub struct SegmentReader<S: StorageBackend> {
    storage: S,
    segment_options: SegmentOptions,
    buffer: Vec<u8>,
    verifies_options: VerifierOptions,
}

impl<S> SegmentReader<S>
where
    S: StorageBackend,
    <S as StorageBackend>::Reader: Unpin,
{
    pub fn new(storage: S, segment_options: SegmentOptions, buffer_size: usize) -> Self {
        let buffer = vec![0; buffer_size];
        let verifies_options = VerifierOptions::default();
        Self {
            storage,
            segment_options,
            buffer,
            verifies_options,
        }
    }

    pub fn with_verifier_options(mut self, verifies_options: VerifierOptions) -> Self {
        self.verifies_options = verifies_options;
        self
    }

    pub async fn read<'a, T>(&'a mut self, segment_start: u64, filename: &str) -> Result<T>
    where
        T: 'a + Follow<'a, Inner = T> + Verifiable,
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

        let segment = flatbuffers::root_with_opts::<T>(&self.verifies_options, &self.buffer[..len])
            .change_context(DnaError::Fatal)
            .attach_printable_lazy(|| format!("failed to read segment {}", segment_name))?;

        Ok(segment)
    }
}
