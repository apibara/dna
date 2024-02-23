use std::time::Instant;

use apibara_core::node::v1alpha2::Cursor;
use error_stack::Result;
use serde_json::Value;
use tracing::{info, warn};

use crate::{error::SinkError, sink::Context, CursorAction};

#[derive(Debug)]
pub struct Buffer {
    pub start_at: Instant,
    pub start_cursor: Cursor,
    pub end_cursor: Cursor,
    pub data: Vec<Value>,
}

impl Default for Buffer {
    fn default() -> Self {
        Self::new()
    }
}

impl Buffer {
    pub fn new() -> Self {
        Self {
            start_at: Instant::now(),
            start_cursor: Cursor::default(),
            end_cursor: Cursor::default(),
            data: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn to_vec(&self) -> Vec<Value> {
        self.data.to_vec()
    }

    pub fn extend(&mut self, data: Vec<Value>) {
        self.data.extend(data);
    }

    pub fn clear(&mut self) {
        self.start_at = Instant::now();
        self.start_cursor = Cursor::default();
        self.end_cursor = Cursor::default();
        self.data.clear();
    }
}

pub struct Batcher {
    pub batch_size: u64,
    pub batch_seconds: u64,
    pub buffer: Buffer,
}

impl Batcher {
    pub fn by_seconds(batch_seconds: u64) -> Self {
        Self {
            batch_size: 0,
            batch_seconds,
            buffer: Buffer::new(),
        }
    }

    pub fn by_size(batch_size: u64) -> Self {
        Self {
            batch_size,
            batch_seconds: 0,
            buffer: Buffer::new(),
        }
    }

    pub fn is_batching_by_seconds(&self) -> bool {
        self.batch_seconds != 0
    }

    pub fn is_batching_by_size(&self) -> bool {
        self.batch_size != 0
    }

    pub fn is_batching(&self) -> bool {
        self.is_batching_by_seconds() || self.is_batching_by_size()
    }

    /// Check if the batch is already added to the buffer
    pub fn is_already_added(&self, batch: &[Value]) -> bool {
        // TODO: This operation can be expensive because contains on a list does
        // a linear search. We should fix it by tracking `context.cursor` from
        // `handle_batch` and `handle_invalidate``.
        batch
            .iter()
            .map(|element| self.buffer.data.contains(element))
            .all(|x| x)
    }

    pub fn should_flush(&self) -> bool {
        let batch_by_size_reached = (self.buffer.end_cursor.order_key
            - self.buffer.start_cursor.order_key)
            >= self.batch_size;

        let batch_by_seconds_reached =
            self.buffer.start_at.elapsed().as_secs() >= self.batch_seconds;

        (self.is_batching_by_size() && batch_by_size_reached)
            || (self.is_batching_by_seconds() && batch_by_seconds_reached)
    }

    pub fn clear(&mut self) {
        self.buffer.clear()
    }

    pub fn is_flushed(&self) -> bool {
        self.buffer.is_empty()
    }

    pub async fn add_data(&mut self, ctx: &Context, batch: &[Value]) {
        if self.buffer.start_cursor == Cursor::default() {
            let zero = 0;
            let zero_cursor = Cursor {
                order_key: zero,
                unique_key: zero.to_be_bytes().to_vec(),
            };
            self.buffer.start_cursor = ctx.cursor.clone().unwrap_or(zero_cursor);
        }
        self.buffer.end_cursor = ctx.end_cursor.clone();
        self.buffer.extend(batch.to_vec());
    }

    pub async fn handle_data(
        &mut self,
        ctx: &Context,
        batch: &[Value],
    ) -> Result<(CursorAction, Option<(Cursor, Vec<Value>)>), SinkError> {
        if batch.is_empty() {
            warn!("data is empty or not an array of objects, skipping");
            // Only skip persistence if the buffer is still not flushed
            if self.is_batching() && !self.is_flushed() {
                return Ok((CursorAction::Skip, None));
            }

            return Ok((CursorAction::Persist, None));
        }

        if !self.is_batching() {
            return Ok((
                CursorAction::Persist,
                Some((ctx.end_cursor.clone(), batch.to_vec())),
            ));
        }

        // `SinkWithBackoff` will retry the same batch if it fails,
        // we have to check if the batch is already added before adding it again
        // This makes the batcher idempotent
        if !self.is_already_added(batch) {
            self.add_data(ctx, batch).await;
        }

        if self.should_flush() {
            info!(
                "handling batch of {} elements after {} seconds: {:?}",
                self.buffer.len(),
                self.buffer.start_at.elapsed().as_secs(),
                self.buffer.data,
            );

            return Ok((
                CursorAction::PersistAt(self.buffer.end_cursor.clone()),
                Some((self.buffer.end_cursor.clone(), self.buffer.to_vec())),
            ));
        }

        Ok((CursorAction::Skip, None))
    }
}
