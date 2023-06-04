use apibara_core::node::v1alpha2::{Cursor, DataFinality};
use apibara_sink_common::Sink;
use async_trait::async_trait;

use serde_json::Value;
use std::{
    fs::{remove_file, File},
    path::PathBuf,
};
use tracing::{info, warn};

use arrow2::{
    chunk::Chunk,
    datatypes::Schema,
    error::Result,
    io::parquet::write::{
        transverse, CompressionOptions, Encoding, FileWriter, RowGroupIterator, Version,
        WriteOptions,
    },
};
use serde_arrow::arrow2::{serialize_into_arrays, serialize_into_fields};

const INCOMPLETE_FILENAME: &str = "incomplete.parquet";

pub struct ParquetSink {
    output_dir: PathBuf,
    batch_size: usize,
    schema: Option<Schema>,
    // Keep track of the current buffer starting and end blocks
    starting_block: Option<u64>,
    end_block: Option<u64>,
    buffer: Vec<Value>,
}

impl ParquetSink {
    pub fn new(output_dir: String, batch_size: usize) -> Result<Self> {
        Ok(Self {
            output_dir: PathBuf::from(&output_dir),
            batch_size,
            buffer: Vec::with_capacity(batch_size),
            starting_block: None,
            end_block: None,
            schema: None,
        })
    }

    fn write_buffer(&mut self, path: PathBuf) -> Result<()> {
        // If the schema is not provided, infer it using the first batch of data
        if self.schema.is_none() {
            let fields = serialize_into_fields(&self.buffer[0], Default::default()).unwrap();
            self.schema = Some(Schema::from(fields));
        }

        let schema = self.schema.clone().unwrap();

        let options = WriteOptions {
            write_statistics: true,
            compression: CompressionOptions::Uncompressed,
            version: Version::V2,
            data_pagesize_limit: None,
        };

        let arrays = self
            .buffer
            .iter()
            .map(|batch| serialize_into_arrays(&schema.fields, batch).unwrap())
            .collect::<Vec<_>>();

        let chunks = arrays.into_iter().map(Chunk::new).collect::<Vec<_>>();

        let iter = chunks.into_iter().map(Ok);

        let encodings = schema
            .fields
            .iter()
            .map(|f| transverse(&f.data_type, |_| Encoding::Plain))
            .collect();

        let row_groups = RowGroupIterator::try_new(iter, &schema, options, encodings)?;

        let file = File::create(path)?;

        let mut writer = FileWriter::try_new(file, schema, options)?;

        for group in row_groups {
            writer.write(group?)?;
        }

        writer.end(None)?;
        Ok(())
    }

    fn flush_buffer(&mut self) -> Result<()> {
        let buffer_size = self.buffer.len();

        info!(
            buffer_size = ?buffer_size,
            start_block = ?self.starting_block,
            end_block = ?self.end_block,
            "parquet: flushing buffer: write data to parquet file"
        );

        if !self.buffer.is_empty() {
            let filename = format!(
                "{}_{}.parquet",
                self.starting_block.unwrap_or(0),
                self.end_block.unwrap()
            );

            let path = self.output_dir.join(filename);

            self.write_buffer(path)?;

            self.buffer.clear();
            self.starting_block = None;
            self.end_block = None;
        }

        let incomplete_path = self.output_dir.join(INCOMPLETE_FILENAME);

        if incomplete_path.is_file() {
            match remove_file(incomplete_path) {
                Err(err) => warn!(err = ?err, "cannot remove incomplete file"),
                Ok(()) => info!("incomplete file removed successfully"),
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Sink for ParquetSink {
    type Error = arrow2::error::Error;

    async fn handle_data(
        &mut self,
        cursor: &Option<Cursor>,
        end_cursor: &Cursor,
        finality: &DataFinality,
        batch: &Value,
    ) -> Result<()> {
        let cursor_str = cursor
            .clone()
            .map(|c| c.to_string())
            .unwrap_or("genesis".into());

        info!(
            cursor = %cursor_str,
            end_cursor = %end_cursor,
            finality = ?finality,
            "parquet: handling data"
        );

        let starting_block = cursor.as_ref().map(|c| c.order_key);
        if self.starting_block.is_none() {
            self.starting_block = starting_block;
        }

        self.end_block = Some(end_cursor.order_key);

        self.buffer.push(batch.clone());

        let starting_cursor = self.starting_block.unwrap_or(0);
        let end_cursor = self.end_block.unwrap();

        if end_cursor - starting_cursor >= self.batch_size as u64 {
            self.flush_buffer()?;
        }

        Ok(())
    }

    async fn handle_invalidate(&mut self, cursor: &Option<Cursor>) -> Result<()> {
        let cursor_str = cursor
            .clone()
            .map(|c| c.to_string())
            .unwrap_or("genesis".into());

        info!(cursor = %cursor_str, "parquet: handling invalidate");
        Ok(())
    }

    async fn handle_heartbeat(&mut self) -> Result<()> {
        info!("parquet: handling heartbeat: writing the buffer as is to {INCOMPLETE_FILENAME}");
        if !self.buffer.is_empty() {
            self.write_buffer(self.output_dir.join(INCOMPLETE_FILENAME))?
        }
        Ok(())
    }

    async fn cleanup(&mut self) -> Result<()> {
        self.flush_buffer()
    }
}
