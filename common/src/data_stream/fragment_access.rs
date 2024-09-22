use error_stack::{Result, ResultExt};
use rkyv::Portable;

use crate::{
    block_store::BlockStoreReader,
    core::Cursor,
    file_cache::Mmap,
    rkyv::Checked,
    store::{fragment::Fragment, segment::Segment, Block},
};

#[derive(Debug)]
pub struct FragmentAccessError;

pub struct FragmentAccess {
    inner: InnerAccess,
}

pub struct FragmentReference<F: Fragment> {
    data: Mmap,
    offset: Option<usize>,
    _phantom: std::marker::PhantomData<F>,
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

    pub async fn get_fragment<F>(&self) -> Result<FragmentReference<F>, FragmentAccessError>
    where
        F: Fragment,
    {
        self.inner.get_fragment::<F>().await
    }
}

impl<F> FragmentReference<F>
where
    F: Fragment,
    F::Archived: Portable + for<'a> Checked<'a>,
{
    pub fn access(&self) -> Result<&F::Archived, FragmentAccessError> {
        if let Some(offset) = self.offset {
            let segment = rkyv::access::<rkyv::Archived<Segment>, rkyv::rancor::Error>(&self.data)
                .change_context(FragmentAccessError)?;

            if offset >= segment.data.len() {
                return Err(FragmentAccessError)
                    .attach_printable("fragment size mismatch")
                    .attach_printable_lazy(|| format!("offset: {}", offset))
                    .attach_printable_lazy(|| format!("size: {}", segment.data.len()))?;
            }

            let fragment_data = &segment.data[offset];

            let fragment = rkyv::access::<F::Archived, rkyv::rancor::Error>(&fragment_data.data)
                .change_context(FragmentAccessError)?;

            Ok(fragment)
        } else {
            let block = rkyv::access::<rkyv::Archived<Block>, rkyv::rancor::Error>(&self.data)
                .change_context(FragmentAccessError)?;

            block
                .access_fragment::<F>()
                .change_context(FragmentAccessError)
        }
    }
}

impl InnerAccess {
    async fn get_fragment<F>(&self) -> Result<FragmentReference<F>, FragmentAccessError>
    where
        F: Fragment,
    {
        match self {
            InnerAccess::Block {
                store,
                block_cursor,
            } => {
                let segment = store
                    .get_block(block_cursor)
                    .await
                    .change_context(FragmentAccessError)?;

                Ok(FragmentReference {
                    data: segment,
                    offset: None,
                    _phantom: std::marker::PhantomData,
                })
            }
            InnerAccess::Segment {
                store,
                segment_cursor,
                offset,
            } => {
                let segment = store
                    .get_segment(segment_cursor, F::name())
                    .await
                    .change_context(FragmentAccessError)?;

                Ok(FragmentReference {
                    data: segment,
                    offset: Some(*offset),
                    _phantom: std::marker::PhantomData,
                })
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
