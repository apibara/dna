use std::collections::hash_map::Entry;
use std::collections::HashMap;

use std::sync::Arc;

use apibara_core::node::v1alpha2::{Cursor, DataFinality};
use apibara_sink_common::batching::Batcher;
use apibara_sink_common::{Context, CursorAction, Sink, ValueExt};
use apibara_sink_common::{SinkError, SinkErrorResultExt};
use arrow::json::reader::{infer_json_schema_from_iterator, Decoder, ReaderBuilder};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use aws_sdk_s3::Client;
use error_stack::{Result, ResultExt};
use parquet::arrow::ArrowWriter;
use serde_json::Value;
use tokio::sync::Mutex;

use tracing::{debug, info, instrument};

use crate::configuration::{SinkParquetConfiguration, SinkParquetOptions};
use crate::parquet_writer::{FileParquetWriter, ParquetWriter, S3ParquetWriter};

pub struct ParquetSink {
    config: SinkParquetConfiguration,
    writer: Box<dyn ParquetWriter + Send + Sync>,
    batcher: Batcher,
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
    pub async fn serialize(&self) -> Result<Vec<u8>, SinkError> {
        let mut data = Vec::new();

        let mut writer = ArrowWriter::try_new(&mut data, self.batch.schema(), None)
            .runtime_error("failed to create Arrow writer")?;

        writer
            .write(&self.batch)
            .runtime_error("failed to write batch")?;

        writer
            .close()
            .runtime_error("failed to close parquet file")?;

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

        let batch_size = config.batch_size as u64;

        Self {
            config,
            writer,
            batcher: Batcher::by_size(batch_size),
        }
    }

    fn get_filename(&self) -> String {
        format!(
            "{:0>10}_{:0>10}.parquet",
            self.batcher.buffer.start_cursor.order_key, self.batcher.buffer.end_cursor.order_key
        )
    }

    /// Write a `DatasetBatch` using the configured `ParquetSink::writer`,
    /// either to S3 if the path starts with `"s3://"` or to the local filesystem otherwise.
    async fn write_dataset_batch(&mut self, batch: &DatasetBatch) -> Result<(), SinkError> {
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

    pub async fn insert_data(
        &mut self,
        _end_cursor: &Cursor,
        batch: &[Value],
    ) -> Result<(), SinkError> {
        // Iterate over the data and split it into datasets.
        let mut datasets = HashMap::<String, Dataset>::default();

        for item in batch {
            let (dataset_name, data) = if self.config.datasets.is_none() {
                ("default", item)
            } else {
                let dataset_name = item
                    .get("dataset")
                    .and_then(Value::as_str)
                    .runtime_error("item is missing required property 'dataset'")?;
                let data = item
                    .get("data")
                    .runtime_error("item is missing required property 'data'")?;
                (dataset_name, data)
            };

            let dataset = match datasets.entry(dataset_name.to_string()) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    let schema = infer_json_schema_from_iterator(std::iter::once(Ok(data)))
                        .runtime_error("failed to infer json schema")?;
                    debug!(schema = ?schema, "inferred schema from item");
                    let decoder = ReaderBuilder::new(Arc::new(schema))
                        .build_decoder()
                        .runtime_error("failed to create reader")?;
                    let dataset = Dataset {
                        decoder: Mutex::new(decoder),
                    };
                    entry.insert(dataset)
                }
            };

            let mut decoder = dataset.decoder.lock().await;
            (*decoder)
                .serialize(&[data])
                .runtime_error("failed to serialize batch item")?;
        }

        debug!("flushing dataset batches");
        let mut dataset_batches = Vec::new();
        let filename = self.get_filename();
        for (dataset_name, dataset) in datasets.iter_mut() {
            let mut decoder = dataset.decoder.lock().await;
            if let Some(record_batch) = decoder
                .flush()
                .runtime_error("failed to flush the parquet RecordBatch")?
            {
                dataset_batches.push(DatasetBatch {
                    name: dataset_name.to_string(),
                    filename: filename.clone(),
                    batch: record_batch,
                });
            }
        }

        for dataset_batch in dataset_batches {
            self.write_dataset_batch(&dataset_batch).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl Sink for ParquetSink {
    type Options = SinkParquetOptions;
    type Error = SinkError;

    async fn from_options(options: Self::Options) -> Result<Self, Self::Error> {
        let config = options.to_parquet_configuration()?;
        Ok(Self::new(config).await)
    }

    #[instrument(skip_all, err(Debug))]
    #[allow(clippy::blocks_in_conditions)]
    async fn handle_data(
        &mut self,
        ctx: &Context,
        batch: &Value,
    ) -> Result<CursorAction, Self::Error> {
        info!(ctx = %ctx, "handling data");
        let batch = batch
            .as_array_of_objects()
            .unwrap_or(&Vec::<Value>::new())
            .to_vec();

        if ctx.finality != DataFinality::DataStatusFinalized {
            self.insert_data(&ctx.end_cursor, &batch).await?;
            return Ok(CursorAction::Persist);
        }

        match self.batcher.handle_data(ctx, &batch).await {
            Ok((action, None)) => Ok(action),
            Ok((action, Some((end_cursor, batch)))) => {
                self.insert_data(&end_cursor, &batch).await?;
                self.batcher.buffer.clear();
                Ok(action)
            }
            Err(e) => Err(e).change_context(SinkError::Runtime),
        }
    }

    #[instrument(skip(self, _cursor), err(Debug))]
    #[allow(clippy::blocks_in_conditions)]
    async fn handle_invalidate(&mut self, _cursor: &Option<Cursor>) -> Result<(), Self::Error> {
        Ok(())
    }

    #[instrument(skip(self), err(Debug))]
    #[allow(clippy::blocks_in_conditions)]
    async fn handle_heartbeat(&mut self) -> Result<(), Self::Error> {
        // TODO: write to incomplete file.
        Ok(())
    }

    async fn cleanup(&mut self) -> Result<(), Self::Error> {
        // TODO: flush to incomplete file.
        Ok(())
    }
}
