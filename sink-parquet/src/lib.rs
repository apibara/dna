use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};

use apibara_core::node::v1alpha2::{Cursor, DataFinality};
use apibara_sink_common::{DisplayCursor, Sink};
use arrow::json::reader::{infer_json_schema_from_iterator, Decoder, ReaderBuilder};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use parquet::arrow::ArrowWriter;
use serde_json::Value;
use tokio::sync::Mutex;
use tracing::{info, instrument, warn, debug};

#[derive(Debug, thiserror::Error)]
pub enum ParquetError {
    #[error("arrow operation error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("parquet operation error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub struct ParquetSink {
    /// Directory where parquet files are written.
    output_dir: PathBuf,
    /// How many blocks to include in a single parquet file.
    batch_size: usize,
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
    pub fn new(output_dir: impl AsRef<Path>, batch_size: usize) -> Result<Self, ParquetError> {
        let batch_size = batch_size.clamp(100, 5_000);

        Ok(Self {
            output_dir: PathBuf::from(output_dir.as_ref()),
            batch_size,
            state: None,
        })
    }

    /// Write a record batch to a parquet file.
    fn write_batch(&mut self, batch: RecordBatch, filename: String) -> Result<(), ParquetError> {
        debug!(size = batch.num_rows(), filename = filename, "writing batch to file");
        let mut file = File::create(self.output_dir.join(filename))?;
        let mut writer = ArrowWriter::try_new(&mut file, batch.schema(), None)?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    }
}

#[async_trait]
impl Sink for ParquetSink {
    type Error = ParquetError;

    #[instrument(skip(self, cursor, end_cursor, finality, batch), err(Debug))]
    async fn handle_data(
        &mut self,
        cursor: &Option<Cursor>,
        end_cursor: &Cursor,
        finality: &DataFinality,
        batch: &Value,
    ) -> Result<(), Self::Error> {
        let Some(batch) = batch.as_array() else {
            warn!("received non-array data");
            return Ok(())
        };

        info!(
            cursor = %DisplayCursor(cursor),
            end_cursor = %end_cursor,
            finality = ?finality,
            "handling data"
        );

        let mut state = match self.state.take() {
            Some(state) => state,
            None => State::new_from_batch(self.batch_size, cursor, end_cursor, batch)?,
        };

        if let Some((batch, filename)) = state.handle_batch(cursor, end_cursor, batch).await? {
            self.write_batch(batch, filename)?;
        }

        self.state = Some(state);

        Ok(())
    }

    #[instrument(skip(self, cursor), err(Debug))]
    async fn handle_invalidate(&mut self, cursor: &Option<Cursor>) -> Result<(), Self::Error> {
        info!(
            cursor = %DisplayCursor(cursor),
            "handling invalidate"
        );
        Ok(())
    }

    #[instrument(skip(self), err(Debug))]
    async fn handle_heartbeat(&mut self) -> Result<(), Self::Error> {
        // TODO: write to incomplete file.
        todo!()
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
    ) -> Result<Self, ParquetError> {
        let schema = infer_json_schema_from_iterator(batch.iter().map(Result::Ok))?;
        let decoder = ReaderBuilder::new(Arc::new(schema)).build_decoder()?;
        let starting_block_number = cursor.as_ref().map(|c| c.order_key + 1).unwrap_or(0);
        let end_block_number = end_cursor.order_key + 1;

        Ok(State {
            decoder: Mutex::new(decoder),
            batch_size,
            starting_block_number,
            end_block_number,
        })
    }

    pub async fn handle_batch(
        &mut self,
        _cursor: &Option<Cursor>,
        end_cursor: &Cursor,
        batch: &[Value],
    ) -> Result<Option<(RecordBatch, String)>, ParquetError> {
        debug!(size = batch.len(), "handling batch");
        let mut decoder = self.decoder.lock().await;
        (*decoder).serialize(batch)?;

        self.end_block_number = end_cursor.order_key;
        if !self.should_flush() {
            return Ok(None);
        }

        debug!("flushing batch");
        let batch_with_filename = (*decoder)
            .flush()?
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

#[cfg(test)]
mod tests {
    use apibara_core::node::v1alpha2::{Cursor, DataFinality};
    use apibara_sink_common::Sink;
    use serde_json::{json, Value};
    use tempdir::TempDir;

    use super::ParquetSink;

    fn new_sink(size: usize) -> ParquetSink {
        let data_dir = TempDir::new("sink_parquet_test").unwrap();
        println!("data_dir: {:?}", data_dir);
        ParquetSink::new(data_dir.into_path(), size).unwrap()
    }

    fn new_batch(size: usize) -> Value {
        let mut batch = Vec::new();
        for i in 0..size {
            batch.push(json!({
                "batchNum": i,
                "batchStr": format!("batch_{}", i),
            }));
        }
        json!(batch)
    }

    fn new_cursor(order_key: u64) -> Cursor {
        Cursor {
            order_key,
            unique_key: order_key.to_be_bytes().to_vec(),
        }
    }

    #[tokio::test]
    async fn test_handle_data() {
        let mut sink = new_sink(10);

        let finality = DataFinality::DataStatusFinalized;

        let mut start_cursor = None;
        let batch = new_batch(10);
        for i in 0..40 {
            let end_cursor = new_cursor(i * 10);
            sink.handle_data(&start_cursor, &end_cursor, &finality, &batch)
                .await
                .unwrap();
            start_cursor = Some(end_cursor);
        }
    }
}
