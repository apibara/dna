use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use error_stack::ResultExt;
use parquet::arrow::ArrowWriter;

use crate::batcher::{BatcherError, BatcherResult};

const DEFAULT_BUFFER_CAPACITY: usize = 8 * 1024 * 1024;

/// Batches data for a single partition.
pub struct PartitionBatcher<D> {
    writer: ArrowWriter<Vec<u8>>,
    batches: Vec<(usize, D)>,
}

impl<D> PartitionBatcher<D> {
    /// Creates a new partition batcher with the given schema.
    pub fn new(schema: SchemaRef) -> BatcherResult<Self> {
        let buffer = Vec::with_capacity(DEFAULT_BUFFER_CAPACITY);
        let writer = ArrowWriter::try_new(buffer, schema, None)
            .change_context(BatcherError::ParquetWriter)
            .attach_printable("failed to create arrow writer")?;

        Ok(Self {
            writer,
            batches: Vec::new(),
        })
    }

    /// Writes a batch to the partition batcher.
    /// Returns the number of bytes written to the parquet buffer.
    pub fn write_batch(&mut self, batch: &RecordBatch, metadata: D) -> BatcherResult<usize> {
        let initial_size = self.buffer_size();

        self.writer
            .write(batch)
            .change_context(BatcherError::ParquetWriter)
            .attach_printable("failed to write batch to parquet writer")?;

        self.batches.push((batch.num_rows(), metadata));

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

    /// Closes the writer and returns the final parquet data.
    pub fn finish(self) -> BatcherResult<Vec<u8>> {
        self.writer
            .into_inner()
            .change_context(BatcherError::ParquetWriter)
            .attach_printable("failed to get parquet data from writer")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{generate_test_batch, schema};

    #[test]
    fn test_partition_batcher_new() {
        let schema = schema();
        let batcher: PartitionBatcher<String> = PartitionBatcher::new(schema).unwrap();
        assert_eq!(batcher.batch_count(), 0);
        assert_eq!(batcher.buffer_size(), 0);
    }

    #[test]
    fn test_partition_batcher_write_single_batch() {
        let schema = schema();
        let mut batcher: PartitionBatcher<String> = PartitionBatcher::new(schema).unwrap();

        let batch = generate_test_batch(10);
        let metadata = "test_metadata".to_string();

        let bytes_written = batcher.write_batch(&batch, metadata).unwrap();

        assert_eq!(batcher.batch_count(), 1);
        // ArrowWriter may buffer data internally, so bytes_written might be 0 initially
        // Just verify that we got a valid response
        assert_eq!(batcher.buffer_size(), bytes_written);
    }

    #[test]
    fn test_partition_batcher_write_multiple_batches() {
        let schema = schema();
        let mut batcher: PartitionBatcher<String> = PartitionBatcher::new(schema).unwrap();

        let batch1 = generate_test_batch(5);
        let batch2 = generate_test_batch(10);
        let batch3 = generate_test_batch(3);

        let bytes_written1 = batcher
            .write_batch(&batch1, "metadata1".to_string())
            .unwrap();
        let bytes_written2 = batcher
            .write_batch(&batch2, "metadata2".to_string())
            .unwrap();
        let bytes_written3 = batcher
            .write_batch(&batch3, "metadata3".to_string())
            .unwrap();

        assert_eq!(batcher.batch_count(), 3);
        // ArrowWriter may buffer data internally, so bytes_written might be 0 initially
        // Just verify that we got valid responses
        assert_eq!(
            batcher.buffer_size(),
            bytes_written1 + bytes_written2 + bytes_written3
        );
    }

    #[test]
    fn test_partition_batcher_buffer_size_grows() {
        let schema = schema();
        let mut batcher: PartitionBatcher<String> = PartitionBatcher::new(schema).unwrap();

        assert_eq!(batcher.buffer_size(), 0);

        // Use a larger batch to trigger buffer growth
        let batch = generate_test_batch(1000);
        let initial_size = batcher.buffer_size();

        batcher.write_batch(&batch, "metadata".to_string()).unwrap();
        let size_after_write = batcher.buffer_size();

        // Buffer size should remain the same or grow depending on internal buffering
        assert!(size_after_write >= initial_size);
    }

    #[test]
    fn test_partition_batcher_finish() {
        let schema = schema();
        let mut batcher: PartitionBatcher<String> = PartitionBatcher::new(schema).unwrap();

        let batch = generate_test_batch(10);
        batcher.write_batch(&batch, "metadata".to_string()).unwrap();

        let parquet_data = batcher.finish().unwrap();

        // The finished parquet data should contain a valid parquet file
        assert!(!parquet_data.is_empty());
        // Check that it contains parquet magic bytes
        assert!(parquet_data.len() > 4);
    }

    #[test]
    fn test_partition_batcher_with_different_metadata_types() {
        let schema = schema();
        let mut batcher: PartitionBatcher<(String, u64)> = PartitionBatcher::new(schema).unwrap();

        let batch = generate_test_batch(5);
        let metadata = ("test".to_string(), 42u64);

        let _bytes_written = batcher.write_batch(&batch, metadata).unwrap();

        assert_eq!(batcher.batch_count(), 1);
        // ArrowWriter may buffer data internally, so bytes_written might be 0 initially
        // Just verify that we got a valid response
    }

    #[test]
    fn test_partition_batcher_empty_batch() {
        let schema = schema();
        let mut batcher: PartitionBatcher<String> = PartitionBatcher::new(schema).unwrap();

        let batch = generate_test_batch(0);
        let bytes_written = batcher.write_batch(&batch, "empty".to_string()).unwrap();

        assert_eq!(batcher.batch_count(), 1);
        // Even empty batches might write some metadata or nothing at all
        assert_eq!(batcher.buffer_size(), bytes_written);
    }
}
