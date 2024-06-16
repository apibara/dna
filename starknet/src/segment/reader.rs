use std::marker::PhantomData;

use apibara_dna_common::{
    error::{DnaError, Result},
    segment::SegmentOptions,
    storage::{segment_prefix, CachedStorage, StorageBackend},
};
use error_stack::ResultExt;
use memmap2::Mmap;
use rkyv::{de::deserializers::SharedDeserializeMap, validation::validators::DefaultValidator};
use tracing::debug;

pub struct LazySegment<S: StorageBackend + Send, T> {
    storage: CachedStorage<S>,
    filename: String,
    segment_options: SegmentOptions,
    current_segment_start: Option<u64>,
    mmap: Option<Mmap>,
    data: PhantomData<T>,
}

impl<S, T> LazySegment<S, T>
where
    S: StorageBackend + Send,
    S::Reader: Unpin + Send,
    T: rkyv::Archive,
{
    pub fn new(
        storage: CachedStorage<S>,
        segment_options: SegmentOptions,
        filename: impl Into<String>,
    ) -> Self {
        Self {
            storage,
            filename: filename.into(),
            segment_options,
            current_segment_start: None,
            mmap: None,
            data: Default::default(),
        }
    }

    pub async fn read<'a>(&'a mut self, block_number: u64) -> Result<T>
    where
        <T as rkyv::Archive>::Archived:
            rkyv::Deserialize<T, SharedDeserializeMap> + rkyv::CheckBytes<DefaultValidator<'a>>,
    {
        // Read segment if any of these two conditions are met:
        // - The segment is not loaded yet.
        // - The segment is loaded but the block number is different.
        let segment_start = self.segment_options.segment_start(block_number);

        let bytes = match (self.mmap.take(), self.current_segment_start) {
            (Some(mmap), Some(current_segment_start)) if current_segment_start == segment_start => {
                mmap
            }
            (Some(_), _) | (None, _) => {
                let segment_name = self.segment_options.format_segment_name(segment_start);
                debug!(filename = self.filename, segment_name, "mmap segment");
                let mmap = self
                    .storage
                    .mmap(segment_prefix(segment_name), &self.filename)
                    .await?;
                mmap
            }
        };

        self.mmap = Some(bytes);
        self.current_segment_start = Some(segment_start);
        let bytes = self.mmap.as_ref().expect("mmapped bytes");

        let segment = rkyv::from_bytes::<'_, T>(&bytes)
            .map_err(|_| DnaError::Io)
            .attach_printable_lazy(|| {
                format!("failed to deserialize segment: {}", self.filename)
            })?;

        Ok(segment)
    }
}

pub struct LazySegmentGroup<S: StorageBackend + Send, T> {
    storage: CachedStorage<S>,
    segment_options: SegmentOptions,
    current_segment_group_start: Option<u64>,
    mmap: Option<Mmap>,
    data: PhantomData<T>,
}

impl<S, T> LazySegmentGroup<S, T>
where
    S: StorageBackend + Send,
    S::Reader: Unpin + Send,
    T: rkyv::Archive,
{
    pub fn new(storage: CachedStorage<S>, segment_options: SegmentOptions) -> Self {
        Self {
            storage,
            segment_options,
            current_segment_group_start: None,
            mmap: None,
            data: Default::default(),
        }
    }

    pub async fn read<'a>(&'a mut self, block_number: u64) -> Result<T>
    where
        <T as rkyv::Archive>::Archived:
            rkyv::Deserialize<T, SharedDeserializeMap> + rkyv::CheckBytes<DefaultValidator<'a>>,
    {
        // Read segment if any of these two conditions are met:
        // - The segment is not loaded yet.
        // - The segment is loaded but the block number is different.
        let segment_group_start = self.segment_options.segment_group_start(block_number);

        let bytes = match (self.mmap.take(), self.current_segment_group_start) {
            (Some(mmap), Some(current_segment_start))
                if current_segment_start == segment_group_start =>
            {
                mmap
            }
            (Some(_), _) | (None, _) => {
                let segment_group_name = self
                    .segment_options
                    .format_segment_group_name(segment_group_start);
                debug!(segment_group_name, "mmap segment group");
                let mmap = self.storage.mmap("group", segment_group_name).await?;
                mmap
            }
        };

        self.mmap = Some(bytes);
        self.current_segment_group_start = Some(segment_group_start);
        let bytes = self.mmap.as_ref().expect("mmapped bytes");

        let segment = rkyv::from_bytes::<'_, T>(&bytes)
            .map_err(|_| DnaError::Io)
            .attach_printable_lazy(|| {
                format!("failed to deserialize segment: {}", segment_group_start)
            })?;

        Ok(segment)
    }
}
