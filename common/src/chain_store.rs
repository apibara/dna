use bytes::Bytes;
use error_stack::{Result, ResultExt};

use crate::{
    chain::CanonicalChainSegment,
    file_cache::{FileCache, FileCacheError},
    object_store::{GetOptions, ObjectETag, ObjectStore, ObjectStoreResultExt, PutOptions},
};

static CANONICAL_PREFIX: &str = "canon";
static RECENT_CHAIN_SEGMENT_NAME: &str = "recent";

#[derive(Debug)]
pub struct ChainStoreError;

#[derive(Clone)]
pub struct ChainStore {
    cache: FileCache,
    client: ObjectStore,
}

impl ChainStore {
    pub fn new(client: ObjectStore, cache: FileCache) -> Self {
        Self { client, cache }
    }

    pub async fn get(
        &self,
        first_block_number: u64,
    ) -> Result<Option<CanonicalChainSegment>, ChainStoreError> {
        let filename = self.segment_filename(first_block_number);
        self.get_impl(&filename, None, false).await
    }

    pub async fn put(
        &self,
        segment: &CanonicalChainSegment,
    ) -> Result<ObjectETag, ChainStoreError> {
        let filename = self.segment_filename(segment.info.first_block.number);
        self.put_impl(&filename, segment).await
    }

    pub async fn put_recent(
        &self,
        segment: &CanonicalChainSegment,
    ) -> Result<ObjectETag, ChainStoreError> {
        self.put_impl(RECENT_CHAIN_SEGMENT_NAME, segment).await
    }

    pub async fn get_recent(
        &self,
        etag: Option<ObjectETag>,
    ) -> Result<Option<CanonicalChainSegment>, ChainStoreError> {
        self.get_impl(RECENT_CHAIN_SEGMENT_NAME, etag, true).await
    }

    async fn put_impl(
        &self,
        name: &str,
        segment: &CanonicalChainSegment,
    ) -> Result<ObjectETag, ChainStoreError> {
        let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(segment)
            .change_context(ChainStoreError)
            .attach_printable("failed to serialize chain segment")?;

        let bytes = Bytes::copy_from_slice(serialized.as_slice());

        let response = self
            .client
            .put(&self.format_key(name), bytes, PutOptions::default())
            .await
            .change_context(ChainStoreError)
            .attach_printable("failed to put chain segment")
            .attach_printable_lazy(|| format!("name: {}", name))?;

        Ok(response.etag)
    }

    async fn get_impl(
        &self,
        name: &str,
        etag: Option<ObjectETag>,
        skip_cache: bool,
    ) -> Result<Option<CanonicalChainSegment>, ChainStoreError> {
        let key = self.format_key(name);

        if skip_cache {
            let Some(bytes) = self.get_as_bytes(&key, etag).await? else {
                return Ok(None);
            };

            let segment = rkyv::from_bytes::<_, rkyv::rancor::Error>(&bytes)
                .change_context(ChainStoreError)
                .attach_printable("failed to deserialize chain segment")
                .attach_printable_lazy(|| format!("name: {}", name))?;

            return Ok(Some(segment));
        }

        if let Some(existing) = self
            .cache
            .general
            .get(&key)
            .await
            .map_err(FileCacheError::Foyer)
            .change_context(ChainStoreError)?
        {
            let segment = rkyv::from_bytes::<_, rkyv::rancor::Error>(existing.value())
                .change_context(ChainStoreError)
                .attach_printable("failed to deserialize chain segment")
                .attach_printable_lazy(|| format!("name: {}", name))?;

            Ok(Some(segment))
        } else {
            let Some(bytes) = self.get_as_bytes(&key, etag).await? else {
                return Ok(None);
            };

            let entry = self.cache.general.insert(key, bytes);

            let segment = rkyv::from_bytes::<_, rkyv::rancor::Error>(entry.value())
                .change_context(ChainStoreError)
                .attach_printable("failed to deserialize chain segment")
                .attach_printable_lazy(|| format!("name: {}", name))?;

            Ok(Some(segment))
        }
    }

    async fn get_as_bytes(
        &self,
        key: &str,
        etag: Option<ObjectETag>,
    ) -> Result<Option<Bytes>, ChainStoreError> {
        match self.client.get(key, GetOptions { etag }).await {
            Ok(response) => Ok(Some(response.body)),
            Err(err) if err.is_not_found() => Ok(None),
            Err(err) => Err(err).change_context(ChainStoreError),
        }
    }

    fn format_key(&self, key: &str) -> String {
        format!("{}/{}", CANONICAL_PREFIX, key)
    }

    fn segment_filename(&self, first_block: u64) -> String {
        format!("z-{:0>10}", first_block)
    }
}

impl error_stack::Context for ChainStoreError {}

impl std::fmt::Display for ChainStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "chain store error")
    }
}
