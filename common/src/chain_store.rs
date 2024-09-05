use bytes::Bytes;
use error_stack::{Result, ResultExt};

use crate::{
    chain::CanonicalChainSegment,
    object_store::{GetOptions, ObjectETag, ObjectStore, ObjectStoreResultExt, PutOptions},
};

static CANONICAL_PREFIX: &str = "canon";
static RECENT_CHAIN_SEGMENT_NAME: &str = "recent";

#[derive(Debug)]
pub struct ChainStoreError;

#[derive(Clone)]
pub struct ChainStore {
    client: ObjectStore,
}

impl ChainStore {
    pub fn new(client: ObjectStore) -> Self {
        Self { client }
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

    pub async fn get_recent(&self) -> Result<Option<CanonicalChainSegment>, ChainStoreError> {
        self.get(RECENT_CHAIN_SEGMENT_NAME).await
    }

    async fn put_impl(
        &self,
        name: &str,
        segment: &CanonicalChainSegment,
    ) -> Result<ObjectETag, ChainStoreError> {
        let serialized = rkyv::to_bytes::<_, 0>(segment)
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

    async fn get(&self, name: &str) -> Result<Option<CanonicalChainSegment>, ChainStoreError> {
        match self
            .client
            .get(&self.format_key(name), GetOptions::default())
            .await
        {
            Ok(response) => {
                let bytes = response
                    .body
                    .collect()
                    .await
                    .change_context(ChainStoreError)
                    .attach_printable("failed to collect chain segment bytes")
                    .attach_printable_lazy(|| format!("name: {}", name))?
                    .into_bytes();

                let segment = rkyv::from_bytes::<CanonicalChainSegment>(&bytes).or_else(|err| {
                    Err(ChainStoreError)
                        .attach_printable("failed to deserialize chain segment")
                        .attach_printable_lazy(|| format!("error: {}", err))
                })?;
                Ok(Some(segment))
            }
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
