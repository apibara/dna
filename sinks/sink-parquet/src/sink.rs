use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;

use std::sync::Arc;

use apibara_core::node::v1alpha2::Cursor;
use apibara_sink_common::{Context, CursorAction, Sink, ValueExt};
use arrow::json::reader::{infer_json_schema_from_iterator, Decoder, ReaderBuilder};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use aws_sdk_s3::Client;
use error_stack::{Result, ResultExt};
use parquet::arrow::ArrowWriter;
use serde_json::Value;
use tokio::sync::Mutex;

use tracing::{debug, info, instrument, warn};

use crate::configuration::{SinkParquetConfiguration, SinkParquetOptions};
use crate::parquet_writer::{FileParquetWriter, ParquetWriter, S3ParquetWriter};

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
    state: Option<State>,
    writer: Box<dyn ParquetWriter + Send + Sync>,
}

/// Sink state.
///
/// This is used to keep track of the file schema, starting and end blocks.
struct State {
    /// Whether to write to a single dataset.
    pub single_dataset: bool,
    /// The datasets to write to.
    pub datasets: HashMap<String, Dataset>,
    /// How many blocks to include in a single parquet file.
    pub batch_size: usize,
    /// The first block (inclusive) in the current batch.
    pub starting_block_number: u64,
    /// The last block (exclusive) in the current batch.
    pub end_block_number: u64,
}

struct Dataset {
    /// JSON to arrow data decoder.
    /// Notice that [Decoder] is not `Sync` so we need to wrap it in a mutex.
    pub decoder: Mutex<Decoder>,
}

struct DatasetBatch {
    pub name: String,
    pub filename: String,
    pub batch: RecordBatch,
}

impl DatasetBatch {
    pub async fn serialize(&self) -> Result<Vec<u8>, SinkParquetError> {
        let mut data = Vec::new();

        let mut writer = ArrowWriter::try_new(&mut data, self.batch.schema(), None)
            .change_context(SinkParquetError)
            .attach_printable("failed to create Arrow writer")?;

        writer
            .write(&self.batch)
            .change_context(SinkParquetError)
            .attach_printable("failed to write batch")?;

        writer
            .close()
            .change_context(SinkParquetError)
            .attach_printable("failed to close parquet file")?;

        Ok(data)
    }
}

impl ParquetSink {
    pub async fn new(config: SinkParquetConfiguration) -> Self {
        let writer: Box<dyn ParquetWriter + Send + Sync> = if config.output_dir.starts_with("s3://")
        {
            let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
            let client = Client::new(&config);
            Box::new(S3ParquetWriter { client })
        } else {
            Box::new(FileParquetWriter)
        };

        Self {
            config,
            writer,
            state: None,
        }
    }

    async fn write_batches(&mut self, batches: &[DatasetBatch]) -> Result<(), SinkParquetError> {
        for batch in batches {
            self.write_batch(batch).await?;
        }

        Ok(())
    }

    /// Write a `DatasetBatch` using the configured `ParquetSink::writer`,
    /// either to S3 if the path starts with `"s3://"` or to the local filesystem otherwise.
    async fn write_batch(&mut self, batch: &DatasetBatch) -> Result<(), SinkParquetError> {
        info!(
            size = batch.batch.num_rows(),
            dataset = batch.name,
            filename = batch.filename,
            "writing batch to path"
        );

        let path = self
            .config
            .output_dir
            .join(&batch.name)
            .join(&batch.filename);

        let data = batch.serialize().await?;

        self.writer.write_parquet(path, &data).await?;

        Ok(())
    }
}

#[async_trait]
impl Sink for ParquetSink {
    type Options = SinkParquetOptions;
    type Error = SinkParquetError;

    async fn from_options(options: Self::Options) -> Result<Self, Self::Error> {
        let config = options.to_parquet_configuration()?;
        Ok(Self::new(config).await)
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
            warn!("batch is empty, skipping");
            return Ok(CursorAction::Skip);
        }

        let mut state = match self.state.take() {
            Some(state) => state,
            None => State::new_from_batch(
                self.config.datasets.is_none(),
                self.config.batch_size,
                &ctx.cursor,
                &ctx.end_cursor,
            )?,
        };

        let mut cursor_action = CursorAction::Skip;

        if let Some(batches) = state
            .handle_batch(&ctx.cursor, &ctx.end_cursor, batch)
            .await?
        {
            self.write_batches(&batches).await?;
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
        single_dataset: bool,
        batch_size: usize,
        cursor: &Option<Cursor>,
        end_cursor: &Cursor,
    ) -> Result<Self, SinkParquetError> {
        let starting_block_number = cursor.as_ref().map(|c| c.order_key).unwrap_or(0);
        let end_block_number = end_cursor.order_key;

        Ok(State {
            single_dataset,
            datasets: HashMap::default(),
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
    ) -> Result<Option<Vec<DatasetBatch>>, SinkParquetError> {
        // Iterate over the data and split it into datasets.
        for item in batch {
            let (dataset_name, data) = if self.single_dataset {
                ("default", item)
            } else {
                let dataset_name = item
                    .get("dataset")
                    .and_then(Value::as_str)
                    .ok_or(SinkParquetError)
                    .attach_printable("item is missing required property 'dataset'")?;
                let data = item
                    .get("data")
                    .ok_or(SinkParquetError)
                    .attach_printable("item is missing required property 'data'")?;
                (dataset_name, data)
            };

            let dataset = match self.datasets.entry(dataset_name.to_string()) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    let schema = infer_json_schema_from_iterator(std::iter::once(Ok(data)))
                        .change_context(SinkParquetError)
                        .attach_printable("failed to infer json schema")?;
                    debug!(schema = ?schema, "inferred schema from item");
                    let decoder = ReaderBuilder::new(Arc::new(schema))
                        .build_decoder()
                        .change_context(SinkParquetError)
                        .attach_printable("failed to create reader")?;
                    let dataset = Dataset {
                        decoder: Mutex::new(decoder),
                    };
                    entry.insert(dataset)
                }
            };

            let mut decoder = dataset.decoder.lock().await;
            (*decoder)
                .serialize(&[data])
                .change_context(SinkParquetError)
                .attach_printable("failed to serialize batch item")?;
        }

        self.end_block_number = end_cursor.order_key;
        if !self.should_flush() {
            return Ok(None);
        }

        debug!("flushing batches");
        let mut batches = Vec::new();
        let filename = self.get_filename();
        for (dataset_name, dataset) in self.datasets.iter_mut() {
            let mut decoder = dataset.decoder.lock().await;
            if let Some(batch) = decoder.flush().change_context(SinkParquetError)? {
                batches.push(DatasetBatch {
                    name: dataset_name.to_string(),
                    filename: filename.clone(),
                    batch,
                });
            }
        }

        self.starting_block_number = self.end_block_number;
        Ok(Some(batches))
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
