use bytes::Bytes;
use error_stack::{Result, ResultExt};

use crate::{
    block_store::BlockStoreReader,
    core::Cursor,
    fragment::{
        Block, BodyFragment, FragmentId, HeaderFragment, IndexFragment, IndexGroupFragment,
        JoinFragment, JoinGroupFragment, HEADER_FRAGMENT_ID, HEADER_FRAGMENT_NAME,
        INDEX_FRAGMENT_NAME, JOIN_FRAGMENT_NAME,
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
    ) -> Result<Access<IndexFragment>, FragmentAccessError> {
        self.inner.get_fragment_indexes(fragment_id).await
    }

    pub async fn get_fragment_joins(
        &self,
        fragment_id: FragmentId,
    ) -> Result<Access<JoinFragment>, FragmentAccessError> {
        self.inner.get_fragment_joins(fragment_id).await
    }

    pub async fn get_header_fragment(&self) -> Result<Access<HeaderFragment>, FragmentAccessError> {
        self.inner.get_header_fragment().await
    }

    pub async fn get_body_fragment(
        &self,
        fragment_id: FragmentId,
        fragment_name: String,
    ) -> Result<Access<BodyFragment>, FragmentAccessError> {
        self.inner
            .get_body_fragment(fragment_id, fragment_name)
            .await
    }
}

impl InnerAccess {
    async fn get_fragment_indexes(
        &self,
        fragment_id: FragmentId,
    ) -> Result<Access<IndexFragment>, FragmentAccessError> {
        match self {
            InnerAccess::Block {
                store,
                block_cursor,
            } => {
                let inner = store
                    .get_block(block_cursor)
                    .await
                    .change_context(FragmentAccessError)?;

                Ok(Access::Block {
                    inner,
                    fragment_id,
                    _phantom: Default::default(),
                })
            }
            InnerAccess::Segment {
                store,
                segment_cursor,
                offset,
            } => {
                let inner = store
                    .get_segment(segment_cursor, INDEX_FRAGMENT_NAME)
                    .await
                    .change_context(FragmentAccessError)?;

                Ok(Access::Segment {
                    inner,
                    fragment_id,
                    offset: *offset,
                    _phantom: Default::default(),
                })
            }
        }
    }

    async fn get_fragment_joins(
        &self,
        fragment_id: FragmentId,
    ) -> Result<Access<JoinFragment>, FragmentAccessError> {
        match self {
            InnerAccess::Block {
                store,
                block_cursor,
            } => {
                let inner = store
                    .get_block(block_cursor)
                    .await
                    .change_context(FragmentAccessError)?;

                Ok(Access::Block {
                    inner,
                    fragment_id,
                    _phantom: Default::default(),
                })
            }
            InnerAccess::Segment {
                store,
                segment_cursor,
                offset,
            } => {
                let inner = store
                    .get_segment(segment_cursor, JOIN_FRAGMENT_NAME)
                    .await
                    .change_context(FragmentAccessError)?;

                Ok(Access::Segment {
                    inner,
                    fragment_id,
                    offset: *offset,
                    _phantom: Default::default(),
                })
            }
        }
    }

    async fn get_body_fragment(
        &self,
        fragment_id: FragmentId,
        fragment_name: String,
    ) -> Result<Access<BodyFragment>, FragmentAccessError> {
        match self {
            InnerAccess::Block {
                store,
                block_cursor,
            } => {
                let inner = store
                    .get_block(block_cursor)
                    .await
                    .change_context(FragmentAccessError)?;

                Ok(Access::Block {
                    inner,
                    fragment_id,
                    _phantom: Default::default(),
                })
            }
            InnerAccess::Segment {
                store,
                segment_cursor,
                offset,
            } => {
                let inner = store
                    .get_segment(segment_cursor, fragment_name)
                    .await
                    .change_context(FragmentAccessError)?;

                Ok(Access::Segment {
                    inner,
                    fragment_id: HEADER_FRAGMENT_ID,
                    offset: *offset,
                    _phantom: Default::default(),
                })
            }
        }
    }

    async fn get_header_fragment(&self) -> Result<Access<HeaderFragment>, FragmentAccessError> {
        match self {
            InnerAccess::Block {
                store,
                block_cursor,
            } => {
                let inner = store
                    .get_block(block_cursor)
                    .await
                    .change_context(FragmentAccessError)?;

                Ok(Access::Block {
                    inner,
                    fragment_id: HEADER_FRAGMENT_ID,
                    _phantom: Default::default(),
                })
            }
            InnerAccess::Segment {
                store,
                segment_cursor,
                offset,
            } => {
                let inner = store
                    .get_segment(segment_cursor, HEADER_FRAGMENT_NAME)
                    .await
                    .change_context(FragmentAccessError)?;

                Ok(Access::Segment {
                    inner,
                    fragment_id: HEADER_FRAGMENT_ID,
                    offset: *offset,
                    _phantom: Default::default(),
                })
            }
        }
    }
}

pub enum Access<T: rkyv::Archive> {
    Block {
        inner: Bytes,
        fragment_id: FragmentId,
        _phantom: std::marker::PhantomData<T>,
    },
    Segment {
        inner: Bytes,
        offset: usize,
        fragment_id: FragmentId,
        _phantom: std::marker::PhantomData<T>,
    },
}

impl Access<IndexFragment> {
    pub fn access(&self) -> Result<&rkyv::Archived<IndexFragment>, FragmentAccessError> {
        match self {
            Access::Block {
                inner, fragment_id, ..
            } => {
                let block = unsafe { rkyv::access_unchecked::<rkyv::Archived<Block>>(inner) };

                let Some(pos) = block
                    .index
                    .indexes
                    .iter()
                    .position(|f| f.fragment_id == *fragment_id)
                else {
                    return Err(FragmentAccessError)
                        .attach_printable("index for fragment not found")
                        .attach_printable_lazy(|| format!("fragment id: {}", fragment_id));
                };

                Ok(&block.index.indexes[pos])
            }
            Access::Segment {
                inner,
                offset,
                fragment_id,
                ..
            } => {
                let segment = unsafe {
                    rkyv::access_unchecked::<rkyv::Archived<Segment<IndexGroupFragment>>>(inner)
                };

                let block_index = &segment.data[*offset];

                let Some(pos) = block_index
                    .data
                    .indexes
                    .iter()
                    .position(|f| f.fragment_id == *fragment_id)
                else {
                    return Err(FragmentAccessError)
                        .attach_printable("index for fragment not found")
                        .attach_printable_lazy(|| format!("fragment id: {}", fragment_id));
                };

                Ok(&block_index.data.indexes[pos])
            }
        }
    }
}

impl Access<JoinFragment> {
    pub fn access(&self) -> Result<&rkyv::Archived<JoinFragment>, FragmentAccessError> {
        match self {
            Access::Block {
                inner, fragment_id, ..
            } => {
                let block = unsafe { rkyv::access_unchecked::<rkyv::Archived<Block>>(inner) };

                let Some(pos) = block
                    .join
                    .joins
                    .iter()
                    .position(|f| f.fragment_id == *fragment_id)
                else {
                    return Err(FragmentAccessError)
                        .attach_printable("joins for fragment not found")
                        .attach_printable_lazy(|| format!("fragment id: {}", fragment_id));
                };

                Ok(&block.join.joins[pos])
            }
            Access::Segment {
                inner,
                offset,
                fragment_id,
                ..
            } => {
                let segment = unsafe {
                    rkyv::access_unchecked::<rkyv::Archived<Segment<JoinGroupFragment>>>(inner)
                };

                let block_index = &segment.data[*offset];

                let Some(pos) = block_index
                    .data
                    .joins
                    .iter()
                    .position(|f| f.fragment_id == *fragment_id)
                else {
                    return Err(FragmentAccessError)
                        .attach_printable("joins for fragment not found")
                        .attach_printable_lazy(|| format!("fragment id: {}", fragment_id));
                };

                Ok(&block_index.data.joins[pos])
            }
        }
    }
}

impl Access<HeaderFragment> {
    pub fn access(&self) -> Result<&rkyv::Archived<HeaderFragment>, FragmentAccessError> {
        match self {
            Access::Block { inner, .. } => {
                let block = unsafe { rkyv::access_unchecked::<rkyv::Archived<Block>>(inner) };
                Ok(&block.header)
            }
            Access::Segment { inner, offset, .. } => {
                let segment = unsafe {
                    rkyv::access_unchecked::<rkyv::Archived<Segment<HeaderFragment>>>(inner)
                };

                Ok(&segment.data[*offset].data)
            }
        }
    }
}

impl Access<BodyFragment> {
    pub fn access(&self) -> Result<&rkyv::Archived<BodyFragment>, FragmentAccessError> {
        match self {
            Access::Block {
                inner, fragment_id, ..
            } => {
                let block = unsafe { rkyv::access_unchecked::<rkyv::Archived<Block>>(inner) };

                let Some(pos) = block
                    .body
                    .iter()
                    .position(|f| f.fragment_id == *fragment_id)
                else {
                    return Err(FragmentAccessError)
                        .attach_printable("body for fragment not found")
                        .attach_printable_lazy(|| format!("fragment id: {}", fragment_id));
                };

                Ok(&block.body[pos])
            }
            Access::Segment { inner, offset, .. } => {
                let segment = unsafe {
                    rkyv::access_unchecked::<rkyv::Archived<Segment<BodyFragment>>>(inner)
                };

                Ok(&segment.data[*offset].data)
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
