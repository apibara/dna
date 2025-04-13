use std::sync::Arc;

use error_stack::Result;
use tokio::sync::{Notify, RwLock};
use tracing::debug;

use crate::Cursor;

use super::{
    error::ChainViewError,
    full::{FullCanonicalChain, NextCursor, ValidatedCursor},
    metrics::ChainViewMetrics,
    CanonicalCursor,
};

/// Provides a read-only view of the canonical chain.
#[derive(Clone)]
pub struct ChainView(Arc<RwLock<ChainViewInner>>);

pub(crate) struct ChainViewInner {
    finalized: u64,
    pending_generation: Option<u64>,
    segmented: Option<u64>,
    grouped: Option<u64>,
    canonical: FullCanonicalChain,
    segment_size: u64,
    group_size: u64,
    pending_notify: Arc<Notify>,
    head_notify: Arc<Notify>,
    finalized_notify: Arc<Notify>,
    segmented_notify: Arc<Notify>,
    metrics: ChainViewMetrics,
}

impl ChainView {
    pub(crate) fn new(
        finalized: u64,
        segmented: Option<u64>,
        grouped: Option<u64>,
        segment_size: u64,
        group_size: u64,
        canonical: FullCanonicalChain,
    ) -> Self {
        let inner = ChainViewInner {
            finalized,
            segmented,
            pending_generation: None,
            grouped,
            canonical,
            segment_size,
            group_size,
            pending_notify: Arc::new(Notify::new()),
            head_notify: Arc::new(Notify::new()),
            finalized_notify: Arc::new(Notify::new()),
            segmented_notify: Arc::new(Notify::new()),
            metrics: ChainViewMetrics::default(),
        };

        Self(Arc::new(RwLock::new(inner)))
    }

    pub async fn get_segment_size(&self) -> u64 {
        self.0.read().await.segment_size
    }

    pub async fn get_group_size(&self) -> u64 {
        self.0.read().await.group_size
    }

    pub async fn get_segment_start_block(&self, block: u64) -> u64 {
        let inner = self.0.read().await;
        inner.get_segment_start_block(block)
    }

    pub async fn get_segment_end_block(&self, block: u64) -> u64 {
        let inner = self.0.read().await;
        inner.get_segment_end_block(block)
    }

    pub async fn has_segment_for_block(&self, block: u64) -> bool {
        let inner = self.0.read().await;
        inner.has_segment_for_block(block)
    }

    pub async fn has_group_for_block(&self, block: u64) -> bool {
        let inner = self.0.read().await;
        inner.has_group_for_block(block)
    }

    pub async fn get_group_start_block(&self, block: u64) -> u64 {
        let inner = self.0.read().await;
        inner.get_group_start_block(block)
    }

    pub async fn get_group_end_block(&self, block: u64) -> u64 {
        let inner = self.0.read().await;
        inner.get_group_end_block(block)
    }

    pub async fn get_blocks_in_group(&self) -> u64 {
        let inner = self.0.read().await;
        inner.group_size * inner.segment_size
    }

    pub async fn get_next_cursor(
        &self,
        cursor: &Option<Cursor>,
    ) -> Result<NextCursor, ChainViewError> {
        let inner = self.0.read().await;
        inner.canonical.get_next_cursor(cursor).await
    }

    pub async fn validate_cursor(
        &self,
        cursor: &Cursor,
    ) -> Result<ValidatedCursor, ChainViewError> {
        let inner = self.0.read().await;
        inner.canonical.validate_cursor(cursor).await
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

    pub async fn pending_changed(&self) {
        let notify = {
            let inner = self.0.read().await;
            inner.pending_notify.clone()
        };
        notify.notified().await;
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

    pub async fn get_pending_generation(&self) -> Option<u64> {
        let inner = self.0.read().await;
        inner.pending_generation
    }

    pub(crate) async fn set_finalized_block(&self, block: u64) {
        let mut inner = self.0.write().await;
        inner.metrics.finalized.record(block, &[]);
        inner.finalized = block;
        inner.finalized_notify.notify_waiters();
    }

    pub(crate) async fn set_pending_generation(&self, generation: Option<u64>) {
        let mut inner = self.0.write().await;
        inner.pending_generation = generation;
        inner.pending_notify.notify_waiters();
    }

    pub(crate) async fn set_segmented_block(&self, block: u64) {
        let mut inner = self.0.write().await;
        inner.metrics.segmented.record(block, &[]);
        inner.segmented = Some(block);
        inner.segmented_notify.notify_waiters();
    }

    pub(crate) async fn set_grouped_block(&self, block: u64) {
        let mut inner = self.0.write().await;
        inner.metrics.grouped.record(block, &[]);
        inner.grouped = Some(block);
    }

    pub(crate) async fn refresh_recent(&self) -> Result<(), ChainViewError> {
        let mut inner = self.0.write().await;

        inner.canonical.refresh_recent().await?;
        let new_head = inner.canonical.get_head().await?;
        debug!(?new_head, "refresh recent head");

        inner.metrics.head.record(new_head.number, &[]);
        inner.head_notify.notify_waiters();

        Ok(())
    }

    pub async fn record_starting_metrics(&self) -> Result<(), ChainViewError> {
        let inner = self.0.read().await;
        let head = inner.canonical.get_head().await?;

        inner.metrics.up.record(1, &[]);
        inner.metrics.head.record(head.number, &[]);
        inner.metrics.finalized.record(inner.finalized, &[]);
        if let Some(segmented) = inner.segmented {
            inner.metrics.segmented.record(segmented, &[]);
        }
        if let Some(grouped) = inner.grouped {
            inner.metrics.grouped.record(grouped, &[]);
        }

        Ok(())
    }

    pub async fn record_is_down(&self) -> Result<(), ChainViewError> {
        let inner = self.0.write().await;
        inner.metrics.up.record(0, &[]);
        Ok(())
    }

    pub async fn record_is_up(&self) -> Result<(), ChainViewError> {
        let inner = self.0.write().await;
        inner.metrics.up.record(1, &[]);
        Ok(())
    }
}

impl ChainViewInner {
    pub fn get_segment_start_block(&self, block: u64) -> u64 {
        let starting_block = self.canonical.starting_block;
        let blocks = block - starting_block;
        let segment_count = blocks / self.segment_size;
        starting_block + segment_count * self.segment_size
    }

    pub fn get_segment_end_block(&self, block: u64) -> u64 {
        self.get_segment_start_block(block) + self.segment_size - 1
    }

    pub fn has_segment_for_block(&self, block: u64) -> bool {
        let Some(segmented) = self.segmented else {
            return false;
        };

        let segment_end = self.get_segment_end_block(block);
        segment_end <= segmented
    }

    pub fn has_group_for_block(&self, block: u64) -> bool {
        let Some(grouped) = self.grouped else {
            return false;
        };

        let group_end = self.get_group_end_block(block);
        group_end <= grouped
    }

    pub fn get_group_end_block(&self, block: u64) -> u64 {
        let blocks_in_group = self.group_size * self.segment_size;
        self.get_group_start_block(block) + blocks_in_group - 1
    }

    pub fn get_group_start_block(&self, block: u64) -> u64 {
        let starting_block = self.canonical.starting_block;
        let blocks_in_group = self.group_size * self.segment_size;
        let group_count = (block - starting_block) / blocks_in_group;
        starting_block + group_count * blocks_in_group
    }
}
