//! Data types for batch committer operations.

use crate::partition::PartitionValue;

/// A batch that needs to be committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchToCommit {
    /// The topic id of the batch to commit.
    pub topic_id: String,
    /// The partition value, if any.
    pub partition_value: Option<PartitionValue>,
    /// The batch size.
    pub batch_size: u32,
}

/// A batch that has been successfully committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedBatch {
    /// The topic id of the batch that was committed.
    pub topic_id: String,
    /// The partition value, if any.
    pub partition_value: Option<PartitionValue>,
    /// The first assigned offset of the batch.
    pub first_offset: u64,
    /// The batch size.
    pub batch_size: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_to_commit_creation() {
        let batch = BatchToCommit {
            topic_id: "test-topic".to_string(),
            partition_value: Some(PartitionValue::String("partition-1".to_string())),
            batch_size: 100,
        };

        assert_eq!(batch.topic_id, "test-topic");
        assert_eq!(
            batch.partition_value,
            Some(PartitionValue::String("partition-1".to_string()))
        );
        assert_eq!(batch.batch_size, 100);
    }

    #[test]
    fn test_batch_to_commit_with_partition() {
        let batch = BatchToCommit {
            topic_id: "test-topic".to_string(),
            partition_value: Some(PartitionValue::Int32(42)),
            batch_size: 250,
        };

        assert_eq!(batch.topic_id, "test-topic");
        assert_eq!(batch.partition_value, Some(PartitionValue::Int32(42)));
        assert_eq!(batch.batch_size, 250);
    }

    #[test]
    fn test_batch_to_commit_unpartitioned() {
        let batch = BatchToCommit {
            topic_id: "test-topic".to_string(),
            partition_value: None,
            batch_size: 150,
        };

        assert_eq!(batch.topic_id, "test-topic");
        assert_eq!(batch.partition_value, None);
        assert_eq!(batch.batch_size, 150);
    }

    #[test]
    fn test_batch_to_commit_with_different_partition_types() {
        let boolean_batch = BatchToCommit {
            topic_id: "topic1".to_string(),
            partition_value: Some(PartitionValue::Boolean(true)),
            batch_size: 100,
        };
        assert_eq!(boolean_batch.topic_id, "topic1");
        assert_eq!(
            boolean_batch.partition_value,
            Some(PartitionValue::Boolean(true))
        );

        let bytes_batch = BatchToCommit {
            topic_id: "topic2".to_string(),
            partition_value: Some(PartitionValue::Bytes(vec![1, 2, 3])),
            batch_size: 200,
        };
        assert_eq!(bytes_batch.topic_id, "topic2");
        assert_eq!(
            bytes_batch.partition_value,
            Some(PartitionValue::Bytes(vec![1, 2, 3]))
        );
    }

    #[test]
    fn test_committed_batch_creation() {
        let batch = CommittedBatch {
            topic_id: "test-topic".to_string(),
            partition_value: Some(PartitionValue::String("partition-1".to_string())),
            first_offset: 42,
            batch_size: 100,
        };

        assert_eq!(batch.topic_id, "test-topic");
        assert_eq!(
            batch.partition_value,
            Some(PartitionValue::String("partition-1".to_string()))
        );
        assert_eq!(batch.first_offset, 42);
        assert_eq!(batch.batch_size, 100);
    }

    #[test]
    fn test_committed_batch_unpartitioned() {
        let batch = CommittedBatch {
            topic_id: "test-topic".to_string(),
            partition_value: None,
            first_offset: 0,
            batch_size: 50,
        };

        assert_eq!(batch.topic_id, "test-topic");
        assert_eq!(batch.partition_value, None);
        assert_eq!(batch.first_offset, 0);
        assert_eq!(batch.batch_size, 50);
    }
}
