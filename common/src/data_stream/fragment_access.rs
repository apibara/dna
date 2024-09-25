use error_stack::{Result, ResultExt};

use crate::{
    block_store::BlockStoreReader,
    core::Cursor,
    fragment::{
        Block, BodyFragment, FragmentId, HeaderFragment, IndexFragment, IndexGroupFragment,
        HEADER_FRAGMENT_NAME, INDEX_FRAGMENT_NAME,
    },
    segment::Segment,
};

#[derive(Debug)]
pub struct FragmentAccessError;

pub struct FragmentAccess {
    inner: InnerAccess,
}

enum InnerAccess {
    Block {
        store: BlockStoreReader,
        block_cursor: Cursor,
    },
    Segment {
        store: BlockStoreReader,
        segment_cursor: Cursor,
        offset: usize,
    },
}

impl FragmentAccess {
    pub fn new_in_block(store: BlockStoreReader, block_cursor: Cursor) -> Self {
        FragmentAccess {
            inner: InnerAccess::Block {
                store,
                block_cursor,
            },
        }
    }

    pub fn new_in_segment(store: BlockStoreReader, segment_cursor: Cursor, offset: usize) -> Self {
        FragmentAccess {
            inner: InnerAccess::Segment {
                store,
                segment_cursor,
                offset,
            },
        }
    }

    pub async fn get_fragment_indexes(
        &self,
        fragment_id: FragmentId,
    ) -> Result<IndexFragment, FragmentAccessError> {
        self.inner.get_fragment_indexes(fragment_id).await
    }

    pub async fn get_header_fragment(&self) -> Result<HeaderFragment, FragmentAccessError> {
        self.inner.get_header_fragment().await
    }

    pub async fn get_body_fragment(
        &self,
        fragment_id: FragmentId,
    ) -> Result<BodyFragment, FragmentAccessError> {
        self.inner.get_body_fragment(fragment_id).await
    }
}

impl InnerAccess {
    async fn get_fragment_indexes(
        &self,
        fragment_id: FragmentId,
    ) -> Result<IndexFragment, FragmentAccessError> {
        match self {
            InnerAccess::Block {
                store,
                block_cursor,
            } => {
                let block = store
                    .get_block(block_cursor)
                    .await
                    .change_context(FragmentAccessError)?;

                let block = rkyv::access::<rkyv::Archived<Block>, rkyv::rancor::Error>(&block)
                    .change_context(FragmentAccessError)?;

                let Some(pos) = block
                    .index
                    .indexes
                    .iter()
                    .position(|f| f.fragment_id == fragment_id)
                else {
                    return Err(FragmentAccessError)
                        .attach_printable("index for fragment not found")
                        .attach_printable_lazy(|| format!("fragment id: {}", fragment_id));
                };

                rkyv::deserialize::<_, rkyv::rancor::Error>(&block.index.indexes[pos])
                    .change_context(FragmentAccessError)
                    .attach_printable("failed to deserialize index")
            }
            InnerAccess::Segment {
                store,
                segment_cursor,
                offset,
            } => {
                let segment = store
                    .get_segment(segment_cursor, INDEX_FRAGMENT_NAME)
                    .await
                    .change_context(FragmentAccessError)?;

                let segment = rkyv::access::<
                    rkyv::Archived<Segment<IndexGroupFragment>>,
                    rkyv::rancor::Error,
                >(&segment)
                .change_context(FragmentAccessError)?;

                let block_index = &segment.data[*offset as usize];

                let Some(pos) = block_index
                    .data
                    .indexes
                    .iter()
                    .position(|f| f.fragment_id == fragment_id)
                else {
                    return Err(FragmentAccessError)
                        .attach_printable("index for fragment not found")
                        .attach_printable_lazy(|| format!("fragment id: {}", fragment_id));
                };

                rkyv::deserialize::<_, rkyv::rancor::Error>(&block_index.data.indexes[pos])
                    .change_context(FragmentAccessError)
                    .attach_printable("failed to deserialize index")
            }
        }
    }

    async fn get_body_fragment(
        &self,
        fragment_id: FragmentId,
    ) -> Result<BodyFragment, FragmentAccessError> {
        match self {
            InnerAccess::Block {
                store,
                block_cursor,
            } => {
                let block = store
                    .get_block(block_cursor)
                    .await
                    .change_context(FragmentAccessError)?;

                let block = rkyv::access::<rkyv::Archived<Block>, rkyv::rancor::Error>(&block)
                    .change_context(FragmentAccessError)?;

                let Some(pos) = block.body.iter().position(|f| f.fragment_id == fragment_id) else {
                    return Err(FragmentAccessError)
                        .attach_printable("body for fragment not found")
                        .attach_printable_lazy(|| format!("fragment id: {}", fragment_id));
                };

                rkyv::deserialize::<_, rkyv::rancor::Error>(&block.body[pos])
                    .change_context(FragmentAccessError)
                    .attach_printable("failed to deserialize body")
            }
            InnerAccess::Segment {
                store,
                segment_cursor,
                offset,
            } => {
                let name = if fragment_id == 2 {
                    "transaction"
                } else if fragment_id == 3 {
                    "receipt"
                } else if fragment_id == 4 {
                    "event"
                } else if fragment_id == 5 {
                    "message"
                } else {
                    todo!();
                };

                let segment = store
                    .get_segment(segment_cursor, name)
                    .await
                    .change_context(FragmentAccessError)?;

                let segment = rkyv::access::<
                    rkyv::Archived<Segment<BodyFragment>>,
                    rkyv::rancor::Error,
                >(&segment)
                .change_context(FragmentAccessError)?;

                let block = &segment.data[*offset as usize];

                rkyv::deserialize::<_, rkyv::rancor::Error>(&block.data)
                    .change_context(FragmentAccessError)
                    .attach_printable("failed to deserialize body")
            }
        }
    }

    async fn get_header_fragment(&self) -> Result<HeaderFragment, FragmentAccessError> {
        match self {
            InnerAccess::Block {
                store,
                block_cursor,
            } => {
                let block = store
                    .get_block(block_cursor)
                    .await
                    .change_context(FragmentAccessError)?;

                let block = rkyv::access::<rkyv::Archived<Block>, rkyv::rancor::Error>(&block)
                    .change_context(FragmentAccessError)?;

                rkyv::deserialize::<_, rkyv::rancor::Error>(&block.header)
                    .change_context(FragmentAccessError)
                    .attach_printable("failed to deserialize header")
            }
            InnerAccess::Segment {
                store,
                segment_cursor,
                offset,
            } => {
                let segment = store
                    .get_segment(segment_cursor, HEADER_FRAGMENT_NAME)
                    .await
                    .change_context(FragmentAccessError)?;

                let segment = rkyv::access::<
                    rkyv::Archived<Segment<HeaderFragment>>,
                    rkyv::rancor::Error,
                >(&segment)
                .change_context(FragmentAccessError)?;

                let block = &segment.data[*offset as usize];

                rkyv::deserialize::<_, rkyv::rancor::Error>(&block.data)
                    .change_context(FragmentAccessError)
                    .attach_printable("failed to deserialize header")
            }
        }
    }
}

impl error_stack::Context for FragmentAccessError {}

impl std::fmt::Display for FragmentAccessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "fragment access error")
    }
}
