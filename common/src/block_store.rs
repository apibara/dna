use bytes::Bytes;
use error_stack::{Result, ResultExt};

use crate::{
    file_cache::{FileCache, Mmap},
    object_store::{GetOptions, ObjectETag, ObjectStore, PutOptions},
    rkyv::Serializable,
    Cursor,
};

static BLOCK_PREFIX: &str = "block";

#[derive(Debug)]
pub struct BlockStoreError;

/// Download blocks from the object store with a local cache.
pub struct BlockStoreReader<B> {
    client: ObjectStore,
    file_cache: FileCache,
    _block_phantom: std::marker::PhantomData<B>,
}

/// Upload blocks to the object store.
pub struct BlockStoreWriter<B> {
    client: ObjectStore,
    _phantom: std::marker::PhantomData<B>,
}

impl<B> BlockStoreReader<B>
where
    B: for<'a> Serializable<'a>,
{
    pub fn new(client: ObjectStore, file_cache: FileCache) -> Self {
        Self {
            client,
            file_cache,
            _block_phantom: Default::default(),
        }
    }

    pub async fn get_block(&self, cursor: &Cursor) -> Result<Mmap, BlockStoreError> {
        let key = format_block_key(cursor);

        if let Some(existing) = self.file_cache.get(&key).await {
            Ok(existing)
        } else {
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
}

impl<B> BlockStoreWriter<B>
where
    B: for<'a> Serializable<'a>,
{
    pub fn new(client: ObjectStore) -> Self {
        Self {
            client,
            _phantom: Default::default(),
        }
    }

    pub async fn put_block(
        &self,
        cursor: &Cursor,
        block: &B,
    ) -> Result<ObjectETag, BlockStoreError> {
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
}

fn format_block_key(cursor: &Cursor) -> String {
    format!("{}/{:0>10}/{}", BLOCK_PREFIX, cursor.number, cursor.hash)
}

impl error_stack::Context for BlockStoreError {}

impl std::fmt::Display for BlockStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "block store error")
    }
}

impl<B> Clone for BlockStoreWriter<B> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            _phantom: Default::default(),
        }
    }
}

impl<B> Clone for BlockStoreReader<B> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            file_cache: self.file_cache.clone(),
            _block_phantom: Default::default(),
        }
    }
}
