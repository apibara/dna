use std::marker::PhantomData;

use error_stack::{Result, ResultExt};
use memmap2::Mmap;
use rkyv::validation::validators::DefaultValidator;

use crate::storage::{segment_prefix, CachedStorage, StorageBackend};

use super::{options, SegmentOptions};

#[derive(Debug)]
pub struct SegmentReaderError;

pub trait SegmentInfo {
    fn segment_start(&self, block_number: u64) -> u64;
    fn segment_prefix(&self, segment_start: u64) -> String;
    fn segment_filename(&self, segment_start: u64) -> String;
}

/// New type to implement [SegmentInfo] for segment groups.
#[derive(Debug, Clone)]
pub struct SegmentGroupOptions(pub SegmentOptions);

/// New type to implement [SegmentInfo] for data segment.
#[derive(Debug, Clone)]
pub struct SegmentDataOptions(pub SegmentOptions, pub String);

#[derive(Debug, Clone)]
pub struct LazySegmentReaderOptions {
    /// Check bytes before deserializing. This is ~10x slower.
    pub check_bytes: bool,
}

/// Lazily read a segment from storage.
pub struct LazySegmentReader<S, I, T>
where
    S: StorageBackend + Send,
    I: SegmentInfo,
{
    storage: CachedStorage<S>,
    segment_info: I,
    current_segment_start: Option<u64>,
    mmap: Option<Mmap>,
    options: LazySegmentReaderOptions,
    data: PhantomData<T>,
}

impl SegmentInfo for SegmentGroupOptions {
    fn segment_start(&self, block_number: u64) -> u64 {
        self.0.segment_group_start(block_number)
    }

    fn segment_prefix(&self, _segment_start: u64) -> String {
        "group".to_string()
    }

    fn segment_filename(&self, segment_start: u64) -> String {
        self.0.format_segment_group_name(segment_start)
    }
}

impl SegmentInfo for SegmentDataOptions {
    fn segment_start(&self, block_number: u64) -> u64 {
        self.0.segment_start(block_number)
    }

    fn segment_prefix(&self, segment_start: u64) -> String {
        let segment_name = self.0.format_segment_name(segment_start);
        segment_prefix(segment_name)
    }

    fn segment_filename(&self, _segment_start: u64) -> String {
        self.1.clone()
    }
}

impl<S, I, T> LazySegmentReader<S, I, T>
where
    S: StorageBackend + Send,
    S::Reader: Unpin + Send,
    I: SegmentInfo,
    T: rkyv::Archive,
    // <T as rkyv::Archive>::Archived: rkyv::CheckBytes<DefaultValidator<'static>>,
{
    pub fn new(
        storage: CachedStorage<S>,
        segment_info: I,
        options: LazySegmentReaderOptions,
    ) -> Self {
        Self {
            storage,
            segment_info,
            current_segment_start: None,
            mmap: None,
            options,
            data: Default::default(),
        }
    }

    pub async fn read<'a>(
        &'a mut self,
        block_number: u64,
    ) -> Result<&<T as rkyv::Archive>::Archived, SegmentReaderError>
    where
        <T as rkyv::Archive>::Archived: rkyv::CheckBytes<DefaultValidator<'a>>,
    {
        // Read segment if any of these two conditions are met:
        // - The segment is not loaded yet.
        // - The segment is loaded but the block number is different.
        let segment_start = self.segment_info.segment_start(block_number);

        let mmap = match (self.mmap.take(), self.current_segment_start) {
            (Some(mmap), Some(current_segment_start)) if current_segment_start == segment_start => {
                mmap
            }
            _ => {
                let prefix = self.segment_info.segment_prefix(segment_start);
                let filename = self.segment_info.segment_filename(segment_start);

                self.storage
                    .mmap(&prefix, &filename)
                    .await
                    .change_context(SegmentReaderError)
                    .attach_printable("failed to mmap segment")
                    .attach_printable_lazy(|| format!("prefix: {prefix}, filename: {filename}"))?
            }
        };

        self.mmap = Some(mmap);
        self.current_segment_start = Some(segment_start);

        let bytes = self.mmap.as_ref().expect("mmapped bytes");
        let archived = if self.options.check_bytes {
            rkyv::check_archived_root::<T>(bytes).or_else(|err| {
                Err(SegmentReaderError)
                    .attach_printable("failed to validate and deserialize segment")
                    .attach_printable(format!("message: {err}"))
                    .attach_printable(format!("block_number: {block_number}"))
                    .attach_printable(format!("segment_start: {segment_start}"))
            })?
        } else {
            unsafe { rkyv::archived_root::<T>(bytes) }
        };

        Ok(archived)
    }
}

impl From<SegmentOptions> for SegmentGroupOptions {
    fn from(segment_options: SegmentOptions) -> Self {
        Self(segment_options)
    }
}

impl From<(SegmentOptions, String)> for SegmentDataOptions {
    fn from((segment_options, segment_name): (SegmentOptions, String)) -> Self {
        Self(segment_options, segment_name)
    }
}

impl error_stack::Context for SegmentReaderError {}

impl std::fmt::Display for SegmentReaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Segment reader error")
    }
}

impl Default for LazySegmentReaderOptions {
    fn default() -> Self {
        Self { check_bytes: true }
    }
}
