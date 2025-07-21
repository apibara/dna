use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use error_stack::report;
use parquet::{
    arrow::ArrowWriter,
    file::{metadata::KeyValue, properties::WriterProperties},
};
use wings_metadata_core::{admin::TopicName, partition::PartitionValue};

use crate::{
    batch::WriteReplySender,
    error::{IngestorError, IngestorResult},
    types::{BatchContext, PartitionFolio, ReplyWithError},
};

const DEFAULT_BUFFER_CAPACITY: usize = 8 * 1024 * 1024;

/// Combines multiple partition batches into a single folio.
pub struct PartitionFolioWriter {
    writer: ArrowWriter<Vec<u8>>,
    batches: Vec<BatchContext>,
}

impl PartitionFolioWriter {
    /// Creates a new partition folio writer with the given schema.
    pub fn new(
        topic_name: TopicName,
        partition_value: Option<PartitionValue>,
        schema: SchemaRef,
    ) -> IngestorResult<Self> {
        // Add topic name and partition key to the metadata to help with debugging and troubleshooting.
        let partition_value = partition_value.map(|v| v.to_string());
        let kv_metadata = vec![
            KeyValue::new("WINGS:topic-name".to_string(), topic_name.to_string()),
            KeyValue::new("WINGS:partition-value".to_string(), partition_value),
        ];
        let write_properties = WriterProperties::builder()
            .set_key_value_metadata(kv_metadata.into())
            .build();

        let buffer = Vec::with_capacity(DEFAULT_BUFFER_CAPACITY);
        // The writer will only fail if the schema is unsupported
        let writer = ArrowWriter::try_new(buffer, schema, write_properties.into())
            .map_err(|err| IngestorError::Schema(err.to_string()))?;

        Ok(Self {
            writer,
            batches: Vec::new(),
        })
    }

    /// Writes a batch to the partition batcher.
    /// Returns the number of bytes written to the parquet buffer.
    ///
    /// On error, returns the reply channel and the error.
    pub fn write_batch(
        &mut self,
        batch: &RecordBatch,
        reply: WriteReplySender,
    ) -> std::result::Result<usize, ReplyWithError> {
        let initial_size = self.buffer_size();

        if let Err(err) = self.writer.write(batch) {
            let error = report!(IngestorError::Schema(err.to_string()));
            return Err(ReplyWithError { reply, error });
        };

        self.batches.push(BatchContext {
            reply,
            num_messages: batch.num_rows() as _,
        });

        let bytes_written = self.buffer_size() - initial_size;

        Ok(bytes_written)
    }

    /// Returns the current size of the buffer in bytes.
    pub fn buffer_size(&self) -> usize {
        self.writer.inner().len()
    }

    /// Closes the writer and returns the final parquet data with metadata.
    ///
    /// On error, returns all the reply channels in the batch, together with the error.
    pub fn finish(
        self,
        topic_name: TopicName,
        partition_value: Option<PartitionValue>,
    ) -> std::result::Result<PartitionFolio, Vec<ReplyWithError>> {
        match self.writer.into_inner() {
            Ok(data) => Ok(PartitionFolio {
                topic_name,
                partition_value,
                data,
                batches: self.batches,
            }),
            Err(err) => {
                let replies = self
                    .batches
                    .into_iter()
                    .map(|m| {
                        let error = report!(IngestorError::Internal(err.to_string()));
                        ReplyWithError {
                            reply: m.reply,
                            error,
                        }
                    })
                    .collect();
                Err(replies)
            }
        }
    }
}
