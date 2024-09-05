use bytes::Bytes;
use error_stack::{Result, ResultExt};

use crate::{
    object_store::{ObjectETag, ObjectStore, PutOptions},
    rkyv::Serializable,
    Cursor,
};

static BLOCK_PREFIX: &str = "block";

#[derive(Debug)]
pub struct BlockStoreError;

pub struct BlockStore<B> {
    client: ObjectStore,
    _phantom: std::marker::PhantomData<B>,
}

impl<B> BlockStore<B>
where
    B: for<'a> Serializable<'a>,
{
    pub fn new(client: ObjectStore) -> Self {
        Self {
            client,
            _phantom: Default::default(),
        }
    }

    pub async fn put(&self, cursor: &Cursor, block: &B) -> Result<ObjectETag, BlockStoreError> {
        let serialized = rkyv::to_bytes(block)
            .change_context(BlockStoreError)
            .attach_printable("failed to serialize block")?;

        let bytes = Bytes::copy_from_slice(serialized.as_slice());

        let response = self
            .client
            .put(&self.format_key(cursor), bytes, PutOptions::default())
            .await
            .change_context(BlockStoreError)
            .attach_printable("failed to put block")
            .attach_printable_lazy(|| format!("cursor: {}", cursor))?;

        Ok(response.etag)
    }

    fn format_key(&self, cursor: &Cursor) -> String {
        format!("{}/{:0>10}/{}", BLOCK_PREFIX, cursor.number, cursor.hash)
    }
}

impl error_stack::Context for BlockStoreError {}

impl std::fmt::Display for BlockStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "block store error")
    }
}

impl<B> Clone for BlockStore<B> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            _phantom: Default::default(),
        }
    }
}
