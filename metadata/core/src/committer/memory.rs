//! In-memory implementation of the batch committer.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;

use crate::admin::{NamespaceName, TopicName};
use crate::committer::{
    BatchCommitter, BatchCommitterError, BatchCommitterResult, BatchToCommit, CommittedBatch,
};
use crate::partition::PartitionValue;

/// A partition key used to identify unique (topic, partition_value) combinations.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PartitionKey {
    topic_id: TopicName,
    partition_value: Option<PartitionValue>,
}

impl PartitionKey {
    fn new(topic_id: TopicName, partition_value: Option<PartitionValue>) -> Self {
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
        topic_id: &TopicName,
        partition_value: &Option<PartitionValue>,
    ) -> Option<u64> {
        let namespace_key = namespace.name();
        let partition_key = PartitionKey::new(topic_id.clone(), partition_value.clone());

        self.namespaces
            .get(&namespace_key)
            .and_then(|partitions| partitions.get(&partition_key).cloned())
    }

    /// Get all partition keys for a namespace.
    pub fn get_partition_keys(
        &self,
        namespace: &NamespaceName,
    ) -> Vec<(TopicName, Option<PartitionValue>)> {
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

        let mut partition_map = self.namespaces.entry(namespace_key).or_default();

        // Assign offsets to each batch
        let mut committed_batches = Vec::new();

        for batch in batches {
            if batch.num_rows == 0 {
                return Err(error_stack::Report::new(
                    BatchCommitterError::BatchValidation {
                        message: format!("batch for topic '{}' has zero size", batch.topic_id),
                    },
                ));
            }

            let partition_key =
                PartitionKey::new(batch.topic_id.clone(), batch.partition_value.clone());

            let current_offset = partition_map.get(&partition_key).copied().unwrap_or(0);

            // Increment offset by batch size
            let new_offset = current_offset + batch.num_rows as u64;
            partition_map.insert(partition_key, new_offset);

            committed_batches.push(CommittedBatch {
                topic_id: batch.topic_id.clone(),
                partition_value: batch.partition_value.clone(),
                start_offset: current_offset,
                end_offset: new_offset - 1,
            });
        }

        Ok(committed_batches)
    }
}
