

use apibara_core::node::v1alpha2::{Cursor, DataFinality};
use apibara_sink_common::Sink;
use async_trait::async_trait;



use serde_json::{Value};
use std::fs::File;
use tracing::{info};

use arrow2::{
    array::{Array},
    chunk::Chunk,
    datatypes::{Schema},
    error::Result,
    io::parquet::write::{
        transverse, CompressionOptions, Encoding, FileWriter, RowGroupIterator, Version,
        WriteOptions,
    },
};
use serde_arrow::arrow2::{serialize_into_arrays, serialize_into_fields};

pub struct ParquetSink {
    output_dir: String,
}

impl ParquetSink {
    pub fn new(output_dir: String) -> Result<Self> {
        Ok(Self { output_dir })
    }

    fn write_chunk(
        &self,
        schema: Schema,
        chunk: Chunk<Box<dyn Array>>,
        _cursor: &Option<Cursor>,
        end_cursor: &Cursor,
    ) -> Result<()> {
        let options = WriteOptions {
            write_statistics: true,
            compression: CompressionOptions::Uncompressed,
            version: Version::V2,
            data_pagesize_limit: None,
        };

        let iter = vec![Ok(chunk)];

        let encodings = schema
            .fields
            .iter()
            .map(|f| transverse(&f.data_type, |_| Encoding::Plain))
            .collect();

        let row_groups = RowGroupIterator::try_new(iter.into_iter(), &schema, options, encodings)?;

        let block_number = end_cursor.order_key;

        let path = self.output_dir.clone() + &String::from(format!("/{block_number}.parquet"));

        // Create a new empty file
        let file = File::create(path)?;

        let mut writer = FileWriter::try_new(file, schema, options)?;

        for group in row_groups {
            writer.write(group?)?;
        }
        let _size = writer.end(None)?;
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
            batch = ?batch,
            "writing to parquet with data"
        );

        let fields = serialize_into_fields(&batch, Default::default()).unwrap();
        let arrays = serialize_into_arrays(&fields, &batch).unwrap();

        let schema = Schema::from(fields);
        let chunk = Chunk::new(arrays);

        self.write_chunk(schema, chunk, cursor, end_cursor)?;

        Ok(())
    }

    async fn handle_invalidate(&mut self, cursor: &Option<Cursor>) -> Result<()> {
        info!(cursor = ?cursor, "writing to parquet with invalidate");
        Ok(())
    }
}
