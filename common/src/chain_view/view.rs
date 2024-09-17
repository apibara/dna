use std::sync::Arc;

use error_stack::Result;
use tokio::sync::{Notify, RwLock};

use crate::Cursor;

use super::{
    error::ChainViewError,
    full::{FullCanonicalChain, NextCursor},
    CanonicalCursor,
};

/// Provides a read-only view of the canonical chain.
#[derive(Clone)]
pub struct ChainView(Arc<RwLock<ChainViewInner>>);

pub(crate) struct ChainViewInner {
    finalized: u64,
    segmented: Option<u64>,
    grouped: Option<u64>,
    canonical: FullCanonicalChain,
    head_notify: Arc<Notify>,
    finalized_notify: Arc<Notify>,
    segmented_notify: Arc<Notify>,
}

impl ChainView {
    pub(crate) fn new(
        finalized: u64,
        segmented: Option<u64>,
        grouped: Option<u64>,
        canonical: FullCanonicalChain,
    ) -> Self {
        let inner = ChainViewInner {
            finalized,
            segmented,
            grouped,
            canonical,
            head_notify: Arc::new(Notify::new()),
            finalized_notify: Arc::new(Notify::new()),
            segmented_notify: Arc::new(Notify::new()),
        };

        Self(Arc::new(RwLock::new(inner)))
    }

    pub async fn get_next_cursor(
        &self,
        cursor: &Option<Cursor>,
    ) -> Result<NextCursor, ChainViewError> {
        let inner = self.0.read().await;
        inner.canonical.get_next_cursor(cursor).await
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

    pub async fn head_changed(&self) {
        let notify = {
            let inner = self.0.read().await;
            inner.head_notify.clone()
        };
        notify.notified().await;
    }

    pub async fn finalized_changed(&self) {
        let notify = {
            let inner = self.0.read().await;
            inner.finalized_notify.clone()
        };
        notify.notified().await;
    }

    pub async fn segmented_changed(&self) {
        let notify = {
            let inner = self.0.read().await;
            inner.segmented_notify.clone()
        };
        notify.notified().await;
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

    pub async fn get_grouped_cursor(&self) -> Result<Option<Cursor>, ChainViewError> {
        let inner = self.0.read().await;
        let Some(grouped) = inner.grouped else {
            return Ok(None);
        };

        match inner.canonical.get_canonical(grouped).await? {
            CanonicalCursor::Canonical(cursor) => Ok(cursor.into()),
            _ => Ok(Cursor::new_finalized(grouped).into()),
        }
    }

    pub async fn get_segmented_cursor(&self) -> Result<Option<Cursor>, ChainViewError> {
        let inner = self.0.read().await;
        let Some(segmented) = inner.segmented else {
            return Ok(None);
        };

        match inner.canonical.get_canonical(segmented).await? {
            CanonicalCursor::Canonical(cursor) => Ok(cursor.into()),
            _ => Ok(Cursor::new_finalized(segmented).into()),
        }
    }

    pub(crate) async fn set_finalized_block(&self, block: u64) {
        let mut inner = self.0.write().await;
        inner.finalized = block;
        inner.finalized_notify.notify_waiters();
    }

    pub(crate) async fn set_segmented_block(&self, block: u64) {
        let mut inner = self.0.write().await;
        inner.segmented = Some(block);
        inner.segmented_notify.notify_waiters();
    }

    pub(crate) async fn set_grouped_block(&self, block: u64) {
        let mut inner = self.0.write().await;
        inner.grouped = Some(block);
    }

    pub(crate) async fn refresh_recent(&self) -> Result<(), ChainViewError> {
        let mut inner = self.0.write().await;

        let prev_head = inner.canonical.get_head().await?;
        inner.canonical.refresh_recent().await?;
        let new_head = inner.canonical.get_head().await?;

        if prev_head != new_head {
            inner.head_notify.notify_waiters();
        }

        Ok(())
    }
}
