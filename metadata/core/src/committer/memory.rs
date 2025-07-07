//! In-memory implementation of the batch committer.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;

use crate::admin::NamespaceName;
use crate::committer::{
    BatchCommitter, BatchCommitterError, BatchCommitterResult, BatchToCommit, CommittedBatch,
};
use crate::partition::PartitionValue;

/// A partition key used to identify unique (topic, partition_value) combinations.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PartitionKey {
    topic_id: String,
    partition_value: Option<PartitionValue>,
}

impl PartitionKey {
    fn new(topic_id: String, partition_value: Option<PartitionValue>) -> Self {
        Self {
            topic_id,
            partition_value,
        }
    }
}

/// In-memory implementation of the batch committer.
///
/// This implementation stores offset counters in memory for each (topic, partition value) tuple.
/// It's primarily intended for testing and development purposes.
#[derive(Debug, Clone)]
pub struct InMemoryBatchCommitter {
    /// Maps namespace names to their partition offset counters.
    namespaces: Arc<DashMap<String, HashMap<PartitionKey, u64>>>,
}

impl InMemoryBatchCommitter {
    /// Create a new in-memory batch committer.
    pub fn new() -> Self {
        Self {
            namespaces: Arc::new(DashMap::new()),
        }
    }

    /// Get the current offset for a partition key in a namespace.
    pub fn get_offset(
        &self,
        namespace: &NamespaceName,
        topic_id: &str,
        partition_value: &Option<PartitionValue>,
    ) -> Option<u64> {
        let namespace_key = namespace.name();
        let partition_key = PartitionKey::new(topic_id.to_string(), partition_value.clone());

        self.namespaces
            .get(&namespace_key)
            .and_then(|partitions| partitions.get(&partition_key).cloned())
    }

    /// Get all partition keys for a namespace.
    pub fn get_partition_keys(
        &self,
        namespace: &NamespaceName,
    ) -> Vec<(String, Option<PartitionValue>)> {
        let namespace_key = namespace.name();

        self.namespaces
            .get(&namespace_key)
            .map(|partitions| {
                partitions
                    .keys()
                    .map(|key| (key.topic_id.clone(), key.partition_value.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Reset all offsets for a namespace.
    pub fn reset_namespace(&self, namespace: &NamespaceName) {
        let namespace_key = namespace.name();
        self.namespaces.remove(&namespace_key);
    }

    /// Reset all offsets.
    pub fn reset_all(&self) {
        self.namespaces.clear();
    }
}

impl Default for InMemoryBatchCommitter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl BatchCommitter for InMemoryBatchCommitter {
    async fn commit_batch(
        &self,
        namespace: NamespaceName,
        _file_ref: String,
        batches: &[BatchToCommit],
    ) -> BatchCommitterResult<Vec<CommittedBatch>> {
        if batches.is_empty() {
            return Err(error_stack::Report::new(
                BatchCommitterError::BatchValidation {
                    message: "cannot commit empty batch list".to_string(),
                },
            ));
        }

        let namespace_key = namespace.name();

        // Get or create the namespace partition map
        let mut partition_map = self.namespaces.entry(namespace_key).or_default();

        // Assign offsets to each batch
        let mut committed_batches = Vec::new();

        for batch in batches {
            if batch.batch_size == 0 {
                return Err(error_stack::Report::new(
                    BatchCommitterError::BatchValidation {
                        message: format!("batch for topic '{}' has zero size", batch.topic_id),
                    },
                ));
            }

            let partition_key =
                PartitionKey::new(batch.topic_id.clone(), batch.partition_value.clone());

            // Get current offset for this partition (starting from 0)
            let current_offset = partition_map.get(&partition_key).copied().unwrap_or(0);

            // Increment offset by batch size
            let new_offset = current_offset + batch.batch_size as u64;
            partition_map.insert(partition_key, new_offset);

            // Create committed batch with the first offset
            committed_batches.push(CommittedBatch {
                topic_id: batch.topic_id.clone(),
                partition_value: batch.partition_value.clone(),
                first_offset: current_offset,
                batch_size: batch.batch_size,
            });
        }

        Ok(committed_batches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::TenantName;

    fn create_test_namespace() -> NamespaceName {
        let tenant = TenantName::new("test-tenant");
        NamespaceName::new("test-namespace", tenant)
    }

    #[tokio::test]
    async fn test_new_committer() {
        let committer = InMemoryBatchCommitter::new();
        let namespace = create_test_namespace();

        // Initially no offsets
        assert_eq!(committer.get_offset(&namespace, "topic1", &None), None);
        assert_eq!(committer.get_partition_keys(&namespace), Vec::new());
    }

    #[tokio::test]
    async fn test_commit_single_batch() {
        let committer = InMemoryBatchCommitter::new();
        let namespace = create_test_namespace();

        let batch = BatchToCommit {
            topic_id: "topic1".to_string(),
            partition_value: None,
            batch_size: 10,
        };

        let result = committer
            .commit_batch(namespace.clone(), "file1".to_string(), &[batch])
            .await;
        assert!(result.is_ok());

        let committed_batches = result.unwrap();
        assert_eq!(committed_batches.len(), 1);
        assert_eq!(committed_batches[0].topic_id, "topic1");
        assert_eq!(committed_batches[0].partition_value, None);
        assert_eq!(committed_batches[0].first_offset, 0);
        assert_eq!(committed_batches[0].batch_size, 10);

        // Check that offset was assigned
        assert_eq!(committer.get_offset(&namespace, "topic1", &None), Some(10));
    }

    #[tokio::test]
    async fn test_commit_multiple_batches_same_partition() {
        let committer = InMemoryBatchCommitter::new();
        let namespace = create_test_namespace();

        let batch1 = BatchToCommit {
            topic_id: "topic1".to_string(),
            partition_value: None,
            batch_size: 5,
        };

        let batch2 = BatchToCommit {
            topic_id: "topic1".to_string(),
            partition_value: None,
            batch_size: 15,
        };

        // Commit first batch
        let result = committer
            .commit_batch(namespace.clone(), "file1".to_string(), &[batch1])
            .await;
        assert!(result.is_ok());
        let committed_batches = result.unwrap();
        assert_eq!(committed_batches.len(), 1);
        assert_eq!(committed_batches[0].first_offset, 0);
        assert_eq!(committer.get_offset(&namespace, "topic1", &None), Some(5));

        // Commit second batch
        let result = committer
            .commit_batch(namespace.clone(), "file2".to_string(), &[batch2])
            .await;
        assert!(result.is_ok());
        let committed_batches = result.unwrap();
        assert_eq!(committed_batches.len(), 1);
        assert_eq!(committed_batches[0].first_offset, 5);
        assert_eq!(committer.get_offset(&namespace, "topic1", &None), Some(20));
    }

    #[tokio::test]
    async fn test_commit_multiple_partitions() {
        let committer = InMemoryBatchCommitter::new();
        let namespace = create_test_namespace();

        let batch1 = BatchToCommit {
            topic_id: "topic1".to_string(),
            partition_value: Some(PartitionValue::String("partition-a".to_string())),
            batch_size: 10,
        };

        let batch2 = BatchToCommit {
            topic_id: "topic1".to_string(),
            partition_value: Some(PartitionValue::String("partition-b".to_string())),
            batch_size: 20,
        };

        let batch3 = BatchToCommit {
            topic_id: "topic1".to_string(),
            partition_value: None,
            batch_size: 30,
        };

        let result = committer
            .commit_batch(
                namespace.clone(),
                "file1".to_string(),
                &[batch1, batch2, batch3],
            )
            .await;
        assert!(result.is_ok());

        let committed_batches = result.unwrap();
        assert_eq!(committed_batches.len(), 3);

        // Check committed batch details
        assert_eq!(committed_batches[0].topic_id, "topic1");
        assert_eq!(
            committed_batches[0].partition_value,
            Some(PartitionValue::String("partition-a".to_string()))
        );
        assert_eq!(committed_batches[0].first_offset, 0);
        assert_eq!(committed_batches[0].batch_size, 10);

        assert_eq!(committed_batches[1].topic_id, "topic1");
        assert_eq!(
            committed_batches[1].partition_value,
            Some(PartitionValue::String("partition-b".to_string()))
        );
        assert_eq!(committed_batches[1].first_offset, 0);
        assert_eq!(committed_batches[1].batch_size, 20);

        assert_eq!(committed_batches[2].topic_id, "topic1");
        assert_eq!(committed_batches[2].partition_value, None);
        assert_eq!(committed_batches[2].first_offset, 0);
        assert_eq!(committed_batches[2].batch_size, 30);

        // Check offsets for different partitions
        assert_eq!(
            committer.get_offset(
                &namespace,
                "topic1",
                &Some(PartitionValue::String("partition-a".to_string()))
            ),
            Some(10)
        );
        assert_eq!(
            committer.get_offset(
                &namespace,
                "topic1",
                &Some(PartitionValue::String("partition-b".to_string()))
            ),
            Some(20)
        );
        assert_eq!(committer.get_offset(&namespace, "topic1", &None), Some(30));
    }

    #[tokio::test]
    async fn test_commit_multiple_topics() {
        let committer = InMemoryBatchCommitter::new();
        let namespace = create_test_namespace();

        let batch1 = BatchToCommit {
            topic_id: "topic1".to_string(),
            partition_value: None,
            batch_size: 100,
        };

        let batch2 = BatchToCommit {
            topic_id: "topic2".to_string(),
            partition_value: None,
            batch_size: 200,
        };

        let result = committer
            .commit_batch(namespace.clone(), "file1".to_string(), &[batch1, batch2])
            .await;
        assert!(result.is_ok());

        let committed_batches = result.unwrap();
        assert_eq!(committed_batches.len(), 2);
        assert_eq!(committed_batches[0].topic_id, "topic1");
        assert_eq!(committed_batches[1].topic_id, "topic2");

        // Check offsets for different topics
        assert_eq!(committer.get_offset(&namespace, "topic1", &None), Some(100));
        assert_eq!(committer.get_offset(&namespace, "topic2", &None), Some(200));
    }

    #[tokio::test]
    async fn test_commit_different_partition_types() {
        let committer = InMemoryBatchCommitter::new();
        let namespace = create_test_namespace();

        let batch1 = BatchToCommit {
            topic_id: "topic1".to_string(),
            partition_value: Some(PartitionValue::Int32(42)),
            batch_size: 10,
        };

        let batch2 = BatchToCommit {
            topic_id: "topic1".to_string(),
            partition_value: Some(PartitionValue::Boolean(true)),
            batch_size: 20,
        };

        let batch3 = BatchToCommit {
            topic_id: "topic1".to_string(),
            partition_value: Some(PartitionValue::Bytes(vec![1, 2, 3])),
            batch_size: 30,
        };

        let result = committer
            .commit_batch(
                namespace.clone(),
                "file1".to_string(),
                &[batch1, batch2, batch3],
            )
            .await;
        assert!(result.is_ok());

        // Check offsets for different partition value types
        assert_eq!(
            committer.get_offset(&namespace, "topic1", &Some(PartitionValue::Int32(42))),
            Some(10)
        );
        assert_eq!(
            committer.get_offset(&namespace, "topic1", &Some(PartitionValue::Boolean(true))),
            Some(20)
        );
        assert_eq!(
            committer.get_offset(
                &namespace,
                "topic1",
                &Some(PartitionValue::Bytes(vec![1, 2, 3]))
            ),
            Some(30)
        );
    }

    #[tokio::test]
    async fn test_commit_empty_batch_list() {
        let committer = InMemoryBatchCommitter::new();
        let namespace = create_test_namespace();

        let result = committer
            .commit_batch(namespace, "file1".to_string(), &[])
            .await;
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert!(matches!(
            error.current_context(),
            BatchCommitterError::BatchValidation { .. }
        ));
    }

    #[tokio::test]
    async fn test_commit_zero_size_batch() {
        let committer = InMemoryBatchCommitter::new();
        let namespace = create_test_namespace();

        let batch = BatchToCommit {
            topic_id: "topic1".to_string(),
            partition_value: None,
            batch_size: 0,
        };

        let result = committer
            .commit_batch(namespace, "file1".to_string(), &[batch])
            .await;
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert!(matches!(
            error.current_context(),
            BatchCommitterError::BatchValidation { .. }
        ));
    }

    #[tokio::test]
    async fn test_multiple_namespaces() {
        let committer = InMemoryBatchCommitter::new();

        let tenant1 = TenantName::new("tenant1");
        let namespace1 = NamespaceName::new("namespace1", tenant1);

        let tenant2 = TenantName::new("tenant2");
        let namespace2 = NamespaceName::new("namespace2", tenant2);

        let batch1 = BatchToCommit {
            topic_id: "topic1".to_string(),
            partition_value: None,
            batch_size: 100,
        };

        let batch2 = BatchToCommit {
            topic_id: "topic1".to_string(),
            partition_value: None,
            batch_size: 200,
        };

        // Commit to different namespaces
        let result1 = committer
            .commit_batch(namespace1.clone(), "file1".to_string(), &[batch1])
            .await;
        assert!(result1.is_ok());

        let result2 = committer
            .commit_batch(namespace2.clone(), "file2".to_string(), &[batch2])
            .await;
        assert!(result2.is_ok());

        // Check that offsets are separate per namespace
        assert_eq!(
            committer.get_offset(&namespace1, "topic1", &None),
            Some(100)
        );
        assert_eq!(
            committer.get_offset(&namespace2, "topic1", &None),
            Some(200)
        );
    }

    #[tokio::test]
    async fn test_get_partition_keys() {
        let committer = InMemoryBatchCommitter::new();
        let namespace = create_test_namespace();

        // Initially empty
        assert_eq!(committer.get_partition_keys(&namespace), Vec::new());

        // Add some partitions
        let batch1 = BatchToCommit {
            topic_id: "topic1".to_string(),
            partition_value: None,
            batch_size: 10,
        };

        let batch2 = BatchToCommit {
            topic_id: "topic1".to_string(),
            partition_value: Some(PartitionValue::String("partition-a".to_string())),
            batch_size: 20,
        };

        let batch3 = BatchToCommit {
            topic_id: "topic2".to_string(),
            partition_value: None,
            batch_size: 30,
        };

        let result = committer
            .commit_batch(
                namespace.clone(),
                "file1".to_string(),
                &[batch1, batch2, batch3],
            )
            .await;
        assert!(result.is_ok());

        let mut partition_keys = committer.get_partition_keys(&namespace);
        partition_keys.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

        let expected = vec![
            ("topic1".to_string(), None),
            (
                "topic1".to_string(),
                Some(PartitionValue::String("partition-a".to_string())),
            ),
            ("topic2".to_string(), None),
        ];

        assert_eq!(partition_keys, expected);
    }

    #[tokio::test]
    async fn test_reset_namespace() {
        let committer = InMemoryBatchCommitter::new();
        let namespace = create_test_namespace();

        let batch = BatchToCommit {
            topic_id: "topic1".to_string(),
            partition_value: None,
            batch_size: 10,
        };

        let result = committer
            .commit_batch(namespace.clone(), "file1".to_string(), &[batch])
            .await;
        assert!(result.is_ok());

        // Check that offset exists
        assert_eq!(committer.get_offset(&namespace, "topic1", &None), Some(10));

        // Reset namespace
        committer.reset_namespace(&namespace);

        // Check that offset is gone
        assert_eq!(committer.get_offset(&namespace, "topic1", &None), None);
        assert_eq!(committer.get_partition_keys(&namespace), Vec::new());
    }

    #[tokio::test]
    async fn test_reset_all() {
        let committer = InMemoryBatchCommitter::new();
        let namespace1 = create_test_namespace();

        let tenant2 = TenantName::new("tenant2");
        let namespace2 = NamespaceName::new("namespace2", tenant2);

        let batch1 = BatchToCommit {
            topic_id: "topic1".to_string(),
            partition_value: None,
            batch_size: 10,
        };

        let batch2 = BatchToCommit {
            topic_id: "topic2".to_string(),
            partition_value: None,
            batch_size: 20,
        };

        let result1 = committer
            .commit_batch(namespace1.clone(), "file1".to_string(), &[batch1])
            .await;
        assert!(result1.is_ok());

        let result2 = committer
            .commit_batch(namespace2.clone(), "file2".to_string(), &[batch2])
            .await;
        assert!(result2.is_ok());

        // Check that offsets exist
        assert_eq!(committer.get_offset(&namespace1, "topic1", &None), Some(10));
        assert_eq!(committer.get_offset(&namespace2, "topic2", &None), Some(20));

        // Reset all
        committer.reset_all();

        // Check that all offsets are gone
        assert_eq!(committer.get_offset(&namespace1, "topic1", &None), None);
        assert_eq!(committer.get_offset(&namespace2, "topic2", &None), None);
        assert_eq!(committer.get_partition_keys(&namespace1), Vec::new());
        assert_eq!(committer.get_partition_keys(&namespace2), Vec::new());
    }
}
