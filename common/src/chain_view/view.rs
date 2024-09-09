use std::sync::Arc;

use error_stack::Result;
use tokio::sync::RwLock;

use crate::Cursor;

use super::{error::ChainViewError, full::FullCanonicalChain, CanonicalCursor};

/// Provides a read-only view of the canonical chain.
#[derive(Clone)]
pub struct ChainView(Arc<RwLock<ChainViewInner>>);

pub(crate) struct ChainViewInner {
    finalized: u64,
    canonical: FullCanonicalChain,
}

impl ChainView {
    pub(crate) fn new(finalized: u64, canonical: FullCanonicalChain) -> Self {
        let inner = ChainViewInner {
            finalized,
            canonical,
        };

        Self(Arc::new(RwLock::new(inner)))
    }

    pub async fn get_canonical(
        &self,
        block_number: u64,
    ) -> Result<CanonicalCursor, ChainViewError> {
        let inner = self.0.read().await;
        inner.canonical.get_canonical(block_number).await
    }

    pub async fn get_head(&self) -> Result<Cursor, ChainViewError> {
        let inner = self.0.read().await;
        inner.canonical.get_head().await
    }

    pub async fn get_starting_cursor(&self) -> Result<Cursor, ChainViewError> {
        let inner = self.0.read().await;
        let starting_block = inner.canonical.starting_block;
        match inner.canonical.get_canonical(starting_block).await? {
            CanonicalCursor::Canonical(cursor) => Ok(cursor),
            _ => Ok(Cursor::new_finalized(starting_block)),
        }
    }

    pub async fn get_finalized_cursor(&self) -> Result<Cursor, ChainViewError> {
        let inner = self.0.read().await;
        match inner.canonical.get_canonical(inner.finalized).await? {
            CanonicalCursor::Canonical(cursor) => Ok(cursor),
            _ => Ok(Cursor::new_finalized(inner.finalized)),
        }
    }

    pub(crate) async fn set_finalized_block(&self, block: u64) {
        let mut inner = self.0.write().await;
        inner.finalized = block;
    }

    pub(crate) async fn refresh_recent(&self) -> Result<(), ChainViewError> {
        let mut inner = self.0.write().await;
        inner.canonical.refresh_recent().await
    }
}
