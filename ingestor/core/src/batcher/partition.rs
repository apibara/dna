use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use error_stack::Report;
use parquet::arrow::ArrowWriter;
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
    pub fn new(schema: SchemaRef) -> IngestorResult<Self> {
        let buffer = Vec::with_capacity(DEFAULT_BUFFER_CAPACITY);
        // The writer will only fail if the schema is unsupported
        let writer = ArrowWriter::try_new(buffer, schema, None)
            .map_err(|_| IngestorError::UnsupportedArrowSchema)?;

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
            let error = Report::new(err).change_context(IngestorError::ArrowSchemaMismatch);
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
                let err = Arc::new(err);
                let replies = self
                    .batches
                    .into_iter()
                    .map(|m| {
                        let err = Report::new(err.clone())
                            .change_context(IngestorError::ParquetWriteError);

                        ReplyWithError {
                            reply: m.reply,
                            error: err,
                        }
                    })
                    .collect();
                Err(replies)
            }
        }
    }
}
