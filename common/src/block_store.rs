use bytes::Bytes;
use error_stack::{Result, ResultExt};

use crate::{
    file_cache::{FileCache, Mmap},
    object_store::{GetOptions, ObjectETag, ObjectStore, PutOptions},
    rkyv::Serializable,
    store::{group::SegmentGroup, segment::SerializedSegment},
    Cursor,
};

static BLOCK_PREFIX: &str = "block";
static SEGMENT_PREFIX: &str = "segment";
static GROUP_PREFIX: &str = "group";

#[derive(Debug)]
pub struct BlockStoreError;

/// Download blocks from the object store with a local cache.
#[derive(Clone)]
pub struct BlockStoreReader {
    client: ObjectStore,
    file_cache: FileCache,
}

/// Upload blocks to the object store.
#[derive(Clone)]
pub struct BlockStoreWriter {
    client: ObjectStore,
}

impl BlockStoreReader {
    pub fn new(client: ObjectStore, file_cache: FileCache) -> Self {
        Self { client, file_cache }
    }

    #[tracing::instrument(
        name = "block_store_get_block",
        skip_all,
        err(Debug),
        fields(cache_hit)
    )]
    pub async fn get_block(&self, cursor: &Cursor) -> Result<Mmap, BlockStoreError> {
        let current_span = tracing::Span::current();
        let key = format_block_key(cursor);

        if let Some(existing) = self.file_cache.get(&key).await {
            current_span.record("cache_hit", 1);

            Ok(existing)
        } else {
            current_span.record("cache_hit", 0);

            let response = self
                .client
                .get(&key, GetOptions::default())
                .await
                .change_context(BlockStoreError)
                .attach_printable("failed to get block")?;

            self.file_cache
                .insert(&key, response.body)
                .await
                .change_context(BlockStoreError)
                .attach_printable("failed to insert block into cache")
                .attach_printable_lazy(|| format!("key: {key}"))
        }
    }

    pub async fn get_index_segment(&self, first_cursor: &Cursor) -> Result<Mmap, BlockStoreError> {
        self.get_segment(first_cursor, "index").await
    }

    #[tracing::instrument(
        name = "block_store_get_segment",
        skip_all,
        err(Debug),
        fields(name, cache_hit)
    )]
    pub async fn get_segment(
        &self,
        first_cursor: &Cursor,
        name: impl Into<String>,
    ) -> Result<Mmap, BlockStoreError> {
        let current_span = tracing::Span::current();
        let name = name.into();
        let key = format_segment_key(first_cursor, &name);

        current_span.record("name", &name);

        if let Some(existing) = self.file_cache.get(&key).await {
            current_span.record("cache_hit", 1);

            Ok(existing)
        } else {
            current_span.record("cache_hit", 0);

            let response = self
                .client
                .get(&key, GetOptions::default())
                .await
                .change_context(BlockStoreError)
                .attach_printable("failed to get segment")
                .attach_printable_lazy(|| format!("key: {key}"))?;

            self.file_cache
                .insert(&key, response.body)
                .await
                .change_context(BlockStoreError)
                .attach_printable("failed to insert segment into cache")
                .attach_printable_lazy(|| format!("key: {key}"))
        }
    }

    #[tracing::instrument(
        name = "block_store_get_group",
        skip_all,
        err(Debug),
        fields(cache_hit)
    )]
    pub async fn get_group(&self, cursor: &Cursor) -> Result<Mmap, BlockStoreError> {
        let current_span = tracing::Span::current();
        let key = format_group_key(cursor);

        if let Some(existing) = self.file_cache.get(&key).await {
            current_span.record("cache_hit", 1);

            Ok(existing)
        } else {
            current_span.record("cache_hit", 0);

            let response = self
                .client
                .get(&key, GetOptions::default())
                .await
                .change_context(BlockStoreError)
                .attach_printable("failed to get group")
                .attach_printable_lazy(|| format!("key: {key}"))?;

            self.file_cache
                .insert(&key, response.body)
                .await
                .change_context(BlockStoreError)
                .attach_printable("failed to insert group into cache")
                .attach_printable_lazy(|| format!("key: {key}"))
        }
    }
}

impl BlockStoreWriter {
    pub fn new(client: ObjectStore) -> Self {
        Self { client }
    }

    pub async fn put_block<B>(
        &self,
        cursor: &Cursor,
        block: &B,
    ) -> Result<ObjectETag, BlockStoreError>
    where
        B: for<'a> Serializable<'a>,
    {
        let serialized = rkyv::to_bytes(block)
            .change_context(BlockStoreError)
            .attach_printable("failed to serialize block")?;

        let bytes = Bytes::copy_from_slice(serialized.as_slice());

        let response = self
            .client
            .put(&format_block_key(cursor), bytes, PutOptions::default())
            .await
            .change_context(BlockStoreError)
            .attach_printable("failed to put block")
            .attach_printable_lazy(|| format!("cursor: {}", cursor))?;

        Ok(response.etag)
    }

    pub async fn put_segment(
        &self,
        first_cursor: &Cursor,
        segment: &SerializedSegment,
    ) -> Result<ObjectETag, BlockStoreError> {
        let bytes = Bytes::copy_from_slice(segment.data.as_slice());

        let response = self
            .client
            .put(
                &format_segment_key(first_cursor, &segment.name),
                bytes,
                PutOptions::default(),
            )
            .await
            .change_context(BlockStoreError)
            .attach_printable("failed to put segment")
            .attach_printable_lazy(|| format!("cursor: {}", first_cursor))
            .attach_printable_lazy(|| format!("segment name: {}", segment.name))?;

        Ok(response.etag)
    }

    pub async fn put_group(
        &self,
        first_cursor: &Cursor,
        group: &SegmentGroup,
    ) -> Result<ObjectETag, BlockStoreError> {
        let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(group)
            .change_context(BlockStoreError)
            .attach_printable("failed to serialize segment group")?;

        let bytes = Bytes::copy_from_slice(serialized.as_slice());

        let response = self
            .client
            .put(
                &format_group_key(first_cursor),
                bytes,
                PutOptions::default(),
            )
            .await
            .change_context(BlockStoreError)
            .attach_printable("failed to put segment group")
            .attach_printable_lazy(|| format!("cursor: {}", first_cursor))?;

        Ok(response.etag)
    }
}

fn format_block_key(cursor: &Cursor) -> String {
    format!("{}/{:0>10}/{}", BLOCK_PREFIX, cursor.number, cursor.hash)
}

fn format_segment_key(first_block: &Cursor, name: &str) -> String {
    format!("{}/{:0>10}/{}", SEGMENT_PREFIX, first_block.number, name)
}

fn format_group_key(first_block: &Cursor) -> String {
    format!("{}/{:0>10}/index", GROUP_PREFIX, first_block.number)
}

impl error_stack::Context for BlockStoreError {}

impl std::fmt::Display for BlockStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "block store error")
    }
}
