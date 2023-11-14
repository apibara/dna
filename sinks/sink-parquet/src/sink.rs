use std::fmt;
use std::{fs::File, sync::Arc};

use apibara_core::node::v1alpha2::Cursor;
use apibara_sink_common::{Context, CursorAction, Sink, ValueExt};
use arrow::json::reader::{infer_json_schema_from_iterator, Decoder, ReaderBuilder};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use error_stack::{Result, ResultExt};
use parquet::arrow::ArrowWriter;
use serde_json::Value;
use tokio::sync::Mutex;
use tracing::{debug, info, instrument, warn};

use crate::configuration::{SinkParquetConfiguration, SinkParquetOptions};

#[derive(Debug)]
pub struct SinkParquetError;
impl error_stack::Context for SinkParquetError {}

impl fmt::Display for SinkParquetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("parquet sink operation failed")
    }
}

pub struct ParquetSink {
    config: SinkParquetConfiguration,
    /// Sync state.
    state: Option<State>,
}

/// Sink state.
///
/// This is used to keep track of the file schema, starting and end blocks.
struct State {
    /// JSON to arrow data decoder.
    /// Notice that [Decoder] is not `Sync` so we need to wrap it in a mutex.
    pub decoder: Mutex<Decoder>,
    /// How many blocks to include in a single parquet file.
    pub batch_size: usize,
    /// The first block (inclusive) in the current batch.
    pub starting_block_number: u64,
    /// The last block (exclusive) in the current batch.
    pub end_block_number: u64,
}

impl ParquetSink {
    pub fn new(config: SinkParquetConfiguration) -> Self {
        Self {
            config,
            state: None,
        }
    }

    /// Write a record batch to a parquet file.
    fn write_batch(
        &mut self,
        batch: RecordBatch,
        filename: String,
    ) -> Result<(), SinkParquetError> {
        debug!(
            size = batch.num_rows(),
            filename = filename,
            "writing batch to file"
        );

        let output_file = self.config.output_dir.join(filename);
        let mut file = File::create(&output_file)
            .change_context(SinkParquetError)
            .attach_printable_lazy(|| format!("failed to create output file at {output_file:?}"))?;

        let mut writer = ArrowWriter::try_new(&mut file, batch.schema(), None)
            .change_context(SinkParquetError)
            .attach_printable("failed to create Arrow writer")?;

        writer
            .write(&batch)
            .change_context(SinkParquetError)
            .attach_printable("failed to write batch")?;

        writer
            .close()
            .change_context(SinkParquetError)
            .attach_printable("failed to close parquet file")?;

        Ok(())
    }
}

#[async_trait]
impl Sink for ParquetSink {
    type Options = SinkParquetOptions;
    type Error = SinkParquetError;

    async fn from_options(options: Self::Options) -> Result<Self, Self::Error> {
        let config = options.to_parquet_configuration()?;
        Ok(Self::new(config))
    }

    #[instrument(skip_all, err(Debug))]
    async fn handle_data(
        &mut self,
        ctx: &Context,
        batch: &Value,
    ) -> Result<CursorAction, Self::Error> {
        let Some(batch) = batch.as_array_of_objects() else {
            warn!("data is not an array of objects, skipping");
            // Skip persistence in case the buffer is still not flushed
            return Ok(CursorAction::Skip);
        };

        if batch.is_empty() {
            // Skip persistence in case the buffer is still not flushed
            return Ok(CursorAction::Skip);
        }

        info!(ctx = %ctx, "handling data");

        let mut state = match self.state.take() {
            Some(state) => state,
            None => {
                State::new_from_batch(self.config.batch_size, &ctx.cursor, &ctx.end_cursor, batch)?
            }
        };

        let mut cursor_action = CursorAction::Skip;

        if let Some((batch, filename)) = state
            .handle_batch(&ctx.cursor, &ctx.end_cursor, batch)
            .await?
        {
            self.write_batch(batch, filename)?;
            cursor_action = CursorAction::Persist;
        }

        self.state = Some(state);
        Ok(cursor_action)
    }

    #[instrument(skip(self, _cursor), err(Debug))]
    async fn handle_invalidate(&mut self, _cursor: &Option<Cursor>) -> Result<(), Self::Error> {
        Ok(())
    }

    #[instrument(skip(self), err(Debug))]
    async fn handle_heartbeat(&mut self) -> Result<(), Self::Error> {
        // TODO: write to incomplete file.
        Ok(())
    }

    async fn cleanup(&mut self) -> Result<(), Self::Error> {
        // TODO: flush to incomplete file.
        Ok(())
    }
}

impl State {
    /// Initialize state from the first batch of data.
    pub fn new_from_batch(
        batch_size: usize,
        cursor: &Option<Cursor>,
        end_cursor: &Cursor,
        batch: &[Value],
    ) -> Result<Self, SinkParquetError> {
        let schema = infer_json_schema_from_iterator(batch.iter().map(std::result::Result::Ok))
            .change_context(SinkParquetError)
            .attach_printable("failed to infer json schema")?;
        debug!(schema = ?schema, "inferred schema from batch");
        let decoder = ReaderBuilder::new(Arc::new(schema))
            .build_decoder()
            .change_context(SinkParquetError)
            .attach_printable("failed to create reader")?;
        let starting_block_number = cursor.as_ref().map(|c| c.order_key).unwrap_or(0);
        let end_block_number = end_cursor.order_key;

        Ok(State {
            decoder: Mutex::new(decoder),
            starting_block_number,
            batch_size,
            end_block_number,
        })
    }

    pub async fn handle_batch(
        &mut self,
        _cursor: &Option<Cursor>,
        end_cursor: &Cursor,
        batch: &[Value],
    ) -> Result<Option<(RecordBatch, String)>, SinkParquetError> {
        debug!(size = batch.len(), "handling batch");
        let mut decoder = self.decoder.lock().await;
        (*decoder)
            .serialize(batch)
            .change_context(SinkParquetError)
            .attach_printable("failed to serialize batch data")?;

        self.end_block_number = end_cursor.order_key;
        if !self.should_flush() {
            return Ok(None);
        }

        debug!("flushing batch");
        let batch_with_filename = (*decoder)
            .flush()
            .change_context(SinkParquetError)?
            .map(|batch| (batch, self.get_filename()));

        self.starting_block_number = self.end_block_number;
        Ok(batch_with_filename)
    }

    fn should_flush(&self) -> bool {
        let num_blocks = self.end_block_number - self.starting_block_number;
        num_blocks >= self.batch_size as u64
    }

    fn get_filename(&self) -> String {
        format!(
            "{:0>10}_{:0>10}.parquet",
            self.starting_block_number, self.end_block_number
        )
    }
}
