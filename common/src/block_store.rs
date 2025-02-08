use anyhow::anyhow;
use bytes::Bytes;
use error_stack::{Result, ResultExt};
use foyer::FetchState;

use crate::{
    chain::PendingBlockInfo,
    file_cache::{FileCache, FileFetch},
    fragment,
    object_store::{GetOptions, ObjectETag, ObjectStore, PutOptions},
    segment::{SegmentGroup, SerializedSegment},
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

/// Download blocks from the object store without a local cache.
#[derive(Clone)]
pub struct UncachedBlockStoreReader {
    client: ObjectStore,
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

    #[tracing::instrument(name = "block_store_get_block", skip_all, fields(cache_hit))]
    pub fn get_block(&self, cursor: &Cursor) -> FileFetch {
        let current_span = tracing::Span::current();
        let key = format_block_key(cursor);

        let fetch_block = {
            let key = key.clone();
            move || {
                let client = self.client.clone();
                async move {
                    match client.get(&key, GetOptions::default()).await {
                        Ok(response) => Ok(response.body),
                        Err(err) => Err(anyhow!(err)),
                    }
                }
            }
        };
        let entry = self.file_cache.fetch(key, fetch_block);

        match entry.state() {
            FetchState::Miss => current_span.record("cache_hit", 0),
            _ => current_span.record("cache_hit", 1),
        };

        entry
    }

    #[tracing::instrument(name = "block_store_get_pending_block", skip_all, fields(cache_hit))]
    pub fn get_pending_block(&self, cursor: &Cursor, generation: u64) -> FileFetch {
        let current_span = tracing::Span::current();
        let key = format_pending_block_key(cursor.number, generation);

        let fetch_block = {
            let key = key.clone();
            move || {
                let client = self.client.clone();
                async move {
                    match client.get(&key, GetOptions::default()).await {
                        Ok(response) => Ok(response.body),
                        Err(err) => Err(anyhow!(err)),
                    }
                }
            }
        };
        let entry = self.file_cache.fetch(key, fetch_block);

        match entry.state() {
            FetchState::Miss => current_span.record("cache_hit", 0),
            _ => current_span.record("cache_hit", 1),
        };

        entry
    }

    pub fn get_index_segment(&self, first_cursor: &Cursor) -> FileFetch {
        self.get_segment(first_cursor, "index")
    }

    #[tracing::instrument(name = "block_store_get_segment", skip_all, fields(name, cache_hit))]
    pub fn get_segment(&self, first_cursor: &Cursor, name: impl Into<String>) -> FileFetch {
        let current_span = tracing::Span::current();
        let name = name.into();
        let key = format_segment_key(first_cursor, &name);

        current_span.record("name", &name);

        let fetch_segment = {
            let key = key.clone();
            move || {
                let client = self.client.clone();
                async move {
                    match client.get(&key, GetOptions::default()).await {
                        Ok(response) => Ok(response.body),
                        Err(err) => Err(anyhow!(err)),
                    }
                }
            }
        };

        let entry = self.file_cache.fetch(key, fetch_segment);

        match entry.state() {
            FetchState::Miss => current_span.record("cache_hit", 0),
            _ => current_span.record("cache_hit", 1),
        };

        entry
    }

    #[tracing::instrument(name = "block_store_get_group", skip_all, fields(cache_hit))]
    pub fn get_group(&self, cursor: &Cursor) -> FileFetch {
        let current_span = tracing::Span::current();
        let key = format_group_key(cursor);

        let fetch_group = {
            let key = key.clone();
            move || {
                let client = self.client.clone();
                async move {
                    match client.get(&key, GetOptions::default()).await {
                        Ok(response) => Ok(response.body),
                        Err(err) => Err(anyhow!(err)),
                    }
                }
            }
        };
        let entry = self.file_cache.fetch(key, fetch_group);

        match entry.state() {
            FetchState::Miss => current_span.record("cache_hit", 0),
            _ => current_span.record("cache_hit", 1),
        };

        entry
    }
}

impl UncachedBlockStoreReader {
    pub fn new(client: ObjectStore) -> Self {
        Self { client }
    }

    #[tracing::instrument(name = "uncached_block_store_get_block", skip_all)]
    pub async fn get_block(&self, cursor: &Cursor) -> Result<Bytes, BlockStoreError> {
        let key = format_block_key(cursor);
        let response = self
            .client
            .get(&key, GetOptions::default())
            .await
            .change_context(BlockStoreError)
            .attach_printable("failed to get block")
            .attach_printable_lazy(|| format!("cursor: {}", cursor))?;

        Ok(response.body)
    }

    #[tracing::instrument(name = "uncached_block_store_get_block", skip_all)]
    pub async fn get_block_and_cursor(
        &self,
        cursor: Cursor,
    ) -> Result<(Cursor, Bytes), BlockStoreError> {
        let key = format_block_key(&cursor);
        let response = self
            .client
            .get(&key, GetOptions::default())
            .await
            .change_context(BlockStoreError)
            .attach_printable("failed to get block")
            .attach_printable_lazy(|| format!("cursor: {}", cursor))?;

        Ok((cursor, response.body))
    }

    pub async fn get_index_segment(&self, first_cursor: &Cursor) -> Result<Bytes, BlockStoreError> {
        self.get_segment(first_cursor, "index").await
    }

    pub async fn get_index_segment_and_cursor(
        &self,
        first_cursor: Cursor,
    ) -> Result<(Cursor, Bytes), BlockStoreError> {
        let segment = self.get_segment(&first_cursor, "index").await?;
        Ok((first_cursor, segment))
    }

    #[tracing::instrument(name = "uncached_block_store_get_segment", skip_all, fields(name))]
    pub async fn get_segment(
        &self,
        first_cursor: &Cursor,
        name: impl Into<String>,
    ) -> Result<Bytes, BlockStoreError> {
        let current_span = tracing::Span::current();
        let name = name.into();
        let key = format_segment_key(first_cursor, &name);

        current_span.record("name", &name);

        let response = self
            .client
            .get(&key, GetOptions::default())
            .await
            .change_context(BlockStoreError)
            .attach_printable("failed to get segment")
            .attach_printable_lazy(|| format!("cursor: {}", first_cursor))
            .attach_printable_lazy(|| format!("name: {}", name))?;

        Ok(response.body)
    }

    #[tracing::instrument(name = "uncached_block_store_get_group", skip_all)]
    pub async fn get_group(&self, cursor: &Cursor) -> Result<Bytes, BlockStoreError> {
        let key = format_group_key(cursor);
        let response = self
            .client
            .get(&key, GetOptions::default())
            .await
            .change_context(BlockStoreError)
            .attach_printable("failed to get group")
            .attach_printable_lazy(|| format!("cursor: {}", cursor))?;

        Ok(response.body)
    }
}

impl BlockStoreWriter {
    pub fn new(client: ObjectStore) -> Self {
        Self { client }
    }

    pub async fn put_block(
        &self,
        cursor: &Cursor,
        block: &fragment::Block,
    ) -> Result<ObjectETag, BlockStoreError> {
        let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(block)
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

    pub async fn put_pending_block(
        &self,
        block_info: &PendingBlockInfo,
        block: &fragment::Block,
    ) -> Result<ObjectETag, BlockStoreError> {
        let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(block)
            .change_context(BlockStoreError)
            .attach_printable("failed to serialize pending block")?;

        let bytes = Bytes::copy_from_slice(serialized.as_slice());

        let response = self
            .client
            .put(
                &format_pending_block_key(block_info.number, block_info.generation),
                bytes,
                PutOptions::default(),
            )
            .await
            .change_context(BlockStoreError)
            .attach_printable("failed to put pending block")
            .attach_printable_lazy(|| format!("info: {:?}", block_info))?;

        Ok(response.etag)
    }

    pub async fn put_segment(
        &self,
        first_cursor: &Cursor,
        segment: SerializedSegment,
    ) -> Result<ObjectETag, BlockStoreError> {
        let response = self
            .client
            .put(
                &format_segment_key(first_cursor, &segment.name),
                segment.data,
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

fn format_pending_block_key(number: u64, generation: u64) -> String {
    format!(
        "{}/{:0>10}/pending-{:0>4}",
        BLOCK_PREFIX, number, generation
    )
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
