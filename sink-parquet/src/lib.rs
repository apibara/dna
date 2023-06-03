use apibara_core::node::v1alpha2::{Cursor, DataFinality};
use apibara_sink_common::Sink;
use async_trait::async_trait;

use serde_json::Value;
use std::fs::File;
use tracing::info;

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

pub struct ParquetSink {
    output_dir: String,
    batch_size: usize,
    // Keep track of the current buffer starting and end blocks
    starting_block: Option<u64>,
    end_block: Option<u64>,
    buffer: Vec<Value>,
}

impl ParquetSink {
    pub fn new(output_dir: String, batch_size: usize) -> Result<Self> {
        Ok(Self {
            output_dir,
            batch_size,
            buffer: Vec::with_capacity(batch_size),
            starting_block: None,
            end_block: None,
        })
    }

    fn write_buffer(&self, schema: Schema) -> Result<()> {
        let options = WriteOptions {
            write_statistics: true,
            compression: CompressionOptions::Uncompressed,
            version: Version::V2,
            data_pagesize_limit: None,
        };

        let arrays = self.buffer
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

        // TODO: use path
        let path = format!(
            "{}/{}_{}.parquet",
            self.output_dir,
            self.starting_block.unwrap_or(0),
            self.end_block.unwrap()
        );
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

        if !self.buffer.is_empty() {
            // TODO: don't use the first element of the buffer, use it all
            let fields = serialize_into_fields(&self.buffer[0], Default::default()).unwrap();
            let schema = Schema::from(fields);

            // TODO: save the schema and don't pass it here
            self.write_buffer(schema)?;

            self.buffer.clear();
            self.starting_block = None;
            self.end_block = None;
        }

        // TODO: If there were an accepted file, delete it

        info!("Flushed buffer");
        info!(
            buffer_size = ? buffer_size,
            start_block = ?self.starting_block,
            end_block = ?self.end_block,
            "data written to parquet file"
        );

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
        info!(
            cursor = ?cursor,
            end_cursor = ?end_cursor,
            finality = ?finality,
            "handle data"
        );

        if let Some(staring_cursor) = cursor {
            if self.starting_block.is_none() {
                self.starting_block = Some(staring_cursor.order_key);
            }
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
        info!(cursor = ?cursor, "writing to parquet with invalidate");
        Ok(())
    }

    async fn cleanup(&mut self) -> Result<()> {
        self.flush_buffer()
    }
}
