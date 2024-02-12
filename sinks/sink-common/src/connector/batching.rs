use std::time::Instant;

use apibara_core::node::v1alpha2::{Cursor, DataFinality};
use error_stack::Result;
use serde_json::Value;
use tracing::{info, warn};

use crate::{error::SinkError, sink::Context, CursorAction, ValueExt};

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
    pub batch_secs: u64,
    pub buffer: Buffer,
}

impl Batcher {
    pub fn by_secs(batch_secs: u64) -> Self {
        Self {
            batch_size: 0,
            batch_secs,
            buffer: Buffer::new(),
        }
    }

    pub fn by_size(batch_size: u64) -> Self {
        Self {
            batch_size,
            batch_secs: 0,
            buffer: Buffer::new(),
        }
    }

    pub fn is_batching_by_secs(&self) -> bool {
        self.batch_secs != 0
    }

    pub fn is_batching_by_size(&self) -> bool {
        self.batch_size != 0
    }

    pub fn is_batching(&self) -> bool {
        self.is_batching_by_secs() || self.is_batching_by_size()
    }

    pub fn should_flush(&self) -> bool {
        let batch_by_size_reached = (self.buffer.end_cursor.order_key
            - self.buffer.start_cursor.order_key)
            >= self.batch_size;

        let batch_by_secs_reached = self.buffer.start_at.elapsed().as_secs() >= self.batch_secs;

        (self.is_batching_by_size() && batch_by_size_reached)
            || (self.is_batching_by_secs() && batch_by_secs_reached)
    }

    pub fn flush(&mut self) -> (Cursor, Vec<Value>) {
        let batch = self.buffer.to_vec();
        let end_cursor = self.buffer.end_cursor.clone();

        self.buffer.clear();

        (end_cursor, batch)
    }

    pub fn clear(&mut self) {
        self.buffer.clear()
    }

    pub fn is_flushed(&self) -> bool {
        self.buffer.is_empty()
    }

    pub async fn add_data(&mut self, ctx: &Context, batch: &Value) {
        // TODO: do something about it ? Why do we have to do this here again ?
        // we already do this in the `handle_data` function
        let batch = batch
            .as_array_of_objects()
            .unwrap_or(&Vec::<Value>::new())
            .to_vec();

        if self.buffer.start_cursor == Cursor::default() {
            self.buffer.start_cursor = ctx.cursor.clone().unwrap_or_default();
        }
        self.buffer.end_cursor = ctx.end_cursor.clone();
        self.buffer.extend(batch);
    }

    pub async fn handle_data(
        &mut self,
        ctx: &Context,
        batch: &Value,
    ) -> Result<(CursorAction, Option<(Cursor, Vec<Value>)>), SinkError> {
        let Some(batch) = batch.as_array_of_objects() else {
            warn!("data is not an array of objects, skipping");
            // Skip persistence in case the buffer is still not flushed
            if self.is_batching() && !self.is_flushed() {
                return Ok((CursorAction::Skip, None));
            } else {
                return Ok((CursorAction::Persist, None));
            }
        };

        if batch.is_empty() {
            warn!("data is empty, skipping");
            // Skip persistence if the buffer is still not flushed
            if self.is_batching() && !self.is_flushed() {
                return Ok((CursorAction::Skip, None));
            } else {
                return Ok((CursorAction::Persist, None));
            }
        }

        if !self.is_batching() || (ctx.finality != DataFinality::DataStatusFinalized) {
            return Ok((
                CursorAction::Persist,
                Some((ctx.end_cursor.clone(), batch.to_vec())),
            ));
        }

        if self.should_flush() {
            info!(
                "handling batch of {} elements after {} seconds: {:?}",
                self.buffer.len(),
                self.buffer.start_at.elapsed().as_secs(),
                self.buffer.data,
            );

            return Ok((
                // Persist the cursor of the end of the buffer, since the current
                // batch is not added the buffer
                // This is to make the function idempotent because it'll be called
                // multiple times with the same data by [`SinkWithBackoff`]
                // if the sink fails
                CursorAction::PersistAt(self.buffer.end_cursor.clone()),
                Some((self.buffer.end_cursor.clone(), self.buffer.to_vec())),
            ));
        }

        Ok((CursorAction::Skip, None))
    }
}
