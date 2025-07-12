use std::{fmt::Debug, sync::Arc};

use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use error_stack::Report;
use parquet::arrow::ArrowWriter;

use crate::{
    batch::WriteReplySender,
    error::{IngestorError, IngestorResult},
};

const DEFAULT_BUFFER_CAPACITY: usize = 8 * 1024 * 1024;

/// Result of finishing a partition batch, containing both data and metadata.
pub struct PartitionFolio {
    /// Serialized data of the partition batch.
    pub data: Vec<u8>,
    /// Size and reply channel for each batch component.
    pub batches: Vec<BatchMetadata>,
}

#[derive(Debug)]
pub struct FlushError {
    pub reply: WriteReplySender,
    pub error: Report<IngestorError>,
}

#[derive(Debug)]
pub struct BatchMetadata {
    pub reply: WriteReplySender,
    pub batch_size: usize,
}

/// Combines multiple partition batches into a single folio.
pub struct PartitionFolioWriter {
    writer: ArrowWriter<Vec<u8>>,
    batches: Vec<BatchMetadata>,
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
    ) -> std::result::Result<usize, (WriteReplySender, Report<IngestorError>)> {
        let initial_size = self.buffer_size();

        if let Err(err) = self.writer.write(batch) {
            let err = Report::new(err).change_context(IngestorError::ArrowSchemaMismatch);
            return Err((reply, err));
        };

        self.batches.push(BatchMetadata {
            reply,
            batch_size: batch.num_rows(),
        });

        let bytes_written = self.buffer_size() - initial_size;

        Ok(bytes_written)
    }

    /// Returns the current size of the buffer in bytes.
    pub fn buffer_size(&self) -> usize {
        self.writer.inner().len()
    }

    /// Returns the number of batches written.
    pub fn batch_count(&self) -> usize {
        self.batches.len()
    }

    /// Closes the writer and returns the final parquet data with metadata.
    ///
    /// On error, returns all the reply channels in the batch, together with the error.
    pub fn finish(self) -> std::result::Result<PartitionFolio, Vec<FlushError>> {
        match self.writer.into_inner() {
            Ok(data) => Ok(PartitionFolio {
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

                        FlushError {
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

impl FlushError {
    pub fn send_reply(self) {
        let _ = self.reply.send(Err(self.error));
    }
}

impl Debug for PartitionFolio {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let data_size = bytesize::ByteSize(self.data.len() as u64);
        f.debug_struct("PartitionBatch")
            .field("data", &format!("<{}>", data_size))
            .field("batches", &format!("<{} entries>", self.batches.len()))
            .finish()
    }
}
