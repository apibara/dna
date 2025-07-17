//! In-memory implementation of the batch committer.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use error_stack::bail;

use crate::admin::{NamespaceName, TopicName};
use crate::offset_registry::{
    BatchToCommit, CommittedBatch, FolioLocation, OffsetLocation, OffsetRegistry,
    OffsetRegistryError, OffsetRegistryResult,
};
use crate::partition::PartitionValue;

/// A partition key used to identify unique (topic, partition_value) combinations.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PartitionKey {
    topic_id: TopicName,
    partition_value: Option<PartitionValue>,
}

/// In-memory implementation of the batch committer.
///
/// This implementation stores offset counters in memory for each (topic, partition value) tuple.
/// It's primarily intended for testing and development purposes.
/// State for a single namespace
#[derive(Debug, Clone)]
struct NamespaceOffsetState {
    /// Maps partition keys to their offset tracking
    partitions: HashMap<PartitionKey, PartitionOffsetState>,
}

/// State for tracking offsets in a partition
#[derive(Debug, Clone)]
struct PartitionOffsetState {
    /// Next offset to be assigned
    next_offset: u64,
    /// Maps start offset to batch information for lookup
    batches: BTreeMap<u64, BatchInfo>,
}

/// Information about a committed batch
#[derive(Debug, Clone)]
struct BatchInfo {
    pub file_ref: String,
    pub offset_bytes: u64,
    pub size_bytes: u64,
    pub end_offset: u64,
}

#[derive(Debug, Clone)]
pub struct InMemoryOffsetRegistry {
    /// Maps namespace names to their offset state
    namespaces: Arc<DashMap<NamespaceName, NamespaceOffsetState>>,
}

impl InMemoryOffsetRegistry {
    /// Create a new in-memory batch committer.
    pub fn new() -> Self {
        Self {
            namespaces: Arc::new(DashMap::new()),
        }
    }
}

#[async_trait]
impl OffsetRegistry for InMemoryOffsetRegistry {
    async fn commit_folio(
        &self,
        namespace: NamespaceName,
        file_ref: String,
        batches: &[BatchToCommit],
    ) -> OffsetRegistryResult<Vec<CommittedBatch>> {
        let mut namespace_state =
            self.namespaces
                .entry(namespace.clone())
                .or_insert_with(|| NamespaceOffsetState {
                    partitions: HashMap::new(),
                });

        let mut seen_partitions = HashSet::new();
        for batch in batches {
            if !seen_partitions.insert((batch.topic_name.clone(), batch.partition_value.clone())) {
                bail!(OffsetRegistryError::DuplicatePartitionValue(
                    batch.topic_name.clone(),
                    batch.partition_value.clone(),
                ));
            }
        }

        let mut committed_batches = Vec::with_capacity(batches.len());

        for batch in batches {
            let partition_key =
                PartitionKey::new(batch.topic_name.clone(), batch.partition_value.clone());

            let partition_state = namespace_state
                .partitions
                .entry(partition_key.clone())
                .or_insert_with(|| PartitionOffsetState {
                    next_offset: 0,
                    batches: BTreeMap::new(),
                });

            let start_offset = partition_state.next_offset;
            let end_offset = start_offset + batch.num_messages as u64 - 1;

            // Store batch information
            let batch_info = BatchInfo {
                file_ref: file_ref.clone(),
                offset_bytes: batch.offset_bytes,
                size_bytes: batch.batch_size_bytes,
                end_offset,
            };

            partition_state.batches.insert(start_offset, batch_info);
            partition_state.next_offset = end_offset + 1;

            committed_batches.push(CommittedBatch {
                topic_name: batch.topic_name.clone(),
                partition_value: batch.partition_value.clone(),
                start_offset,
                end_offset,
            });
        }

        Ok(committed_batches)
    }

    async fn offset_location(
        &self,
        topic: TopicName,
        partition_value: Option<PartitionValue>,
        offset: u64,
    ) -> OffsetRegistryResult<OffsetLocation> {
        let namespace_name = topic.parent().clone();

        let namespace_state = self
            .namespaces
            .get(&namespace_name)
            .ok_or_else(|| OffsetRegistryError::NamespaceNotFound(namespace_name.clone()))?;

        let partition_key = PartitionKey::new(topic.clone(), partition_value.clone());

        let Some(partition_state) = namespace_state.partitions.get(&partition_key) else {
            bail!(OffsetRegistryError::OffsetNotFound(
                topic,
                partition_value,
                offset
            ));
        };

        // Find the batch containing this offset
        let batch_start = partition_state.batches.range(..=offset).next_back();

        let Some((&start_offset, batch_info)) = batch_start else {
            bail!(OffsetRegistryError::OffsetNotFound(
                topic,
                partition_value,
                offset
            ));
        };

        if offset <= batch_info.end_offset {
            return Ok(OffsetLocation::Folio(FolioLocation {
                file_ref: batch_info.file_ref.clone(),
                offset_bytes: batch_info.offset_bytes,
                size_bytes: batch_info.size_bytes,
                start_offset,
                end_offset: batch_info.end_offset,
            }));
        }

        bail!(OffsetRegistryError::OffsetNotFound(
            topic,
            partition_value,
            offset
        ));
    }
}

impl PartitionKey {
    fn new(topic_id: TopicName, partition_value: Option<PartitionValue>) -> Self {
        Self {
            topic_id,
            partition_value,
        }
    }
}

impl Default for InMemoryOffsetRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    /// Helper to create a test namespace name
    fn test_namespace() -> NamespaceName {
        NamespaceName::from_str("tenants/test-tenant/namespaces/test-namespace").unwrap()
    }

    /// Helper to create a test topic name
    fn test_topic() -> TopicName {
        TopicName::from_str("tenants/test-tenant/namespaces/test-namespace/topics/test-topic")
            .unwrap()
    }

    /// Helper to create a simple partition value
    fn test_partition_value() -> Option<PartitionValue> {
        Some(PartitionValue::String("test-partition".to_string()))
    }

    /// Helper to create a batch to commit
    fn create_batch_to_commit(
        topic_id: TopicName,
        partition_value: Option<PartitionValue>,
        num_messages: u32,
        offset_bytes: u64,
        batch_size_bytes: u64,
    ) -> BatchToCommit {
        BatchToCommit {
            topic_name: topic_id,
            partition_value,
            num_messages,
            offset_bytes,
            batch_size_bytes,
        }
    }

    #[tokio::test]
    /// A comprehensive test for the InMemoryOffsetRegistry that covers all functionality in a single test.
    ///
    /// This test covers:
    /// 1. Empty registry error behavior
    /// 2. Committing a single batch
    /// 3. Querying offsets at start, middle, and end of a batch
    /// 4. Querying offsets outside of valid range
    /// 5. Sequential batch commits and proper offset continuation
    /// 6. Committing multiple batches with different topics/partitions in a single call
    /// 7. Error handling for duplicate partition values
    /// 8. Multiple namespace support
    /// 9. Non-existent topic and partition error handling
    /// 10. Edge case with single-message batch
    /// 11. Empty batch list behavior
    async fn test_in_memory_offset_registry() {
        // Test 1: Empty registry
        let registry = InMemoryOffsetRegistry::new();
        let topic = test_topic();
        let partition = test_partition_value();

        // Query against empty registry should fail
        let result = registry
            .offset_location(topic.clone(), partition.clone(), 0)
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(
            err.current_context(),
            OffsetRegistryError::NamespaceNotFound(_)
        ));

        // Test 2: Commit and query a single batch
        let namespace = test_namespace();

        // Create a batch with 10 messages
        let batch = create_batch_to_commit(
            topic.clone(),
            partition.clone(),
            10,
            100,  // offset bytes
            1000, // size bytes
        );

        // Commit the batch
        let result = registry
            .commit_folio(namespace.clone(), "file1.data".to_string(), &[batch])
            .await
            .unwrap();

        // Verify the committed batch has correct offsets
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].topic_name, topic);
        assert_eq!(result[0].partition_value, partition.clone());
        assert_eq!(result[0].start_offset, 0);
        assert_eq!(result[0].end_offset, 9); // 10 messages, 0-based

        // Test 3: Query offset at the start, middle, and end
        // Query offset at the start (0)
        let result = registry
            .offset_location(topic.clone(), partition.clone(), 0)
            .await
            .unwrap();
        match result {
            OffsetLocation::Folio(loc) => {
                assert_eq!(loc.file_ref, "file1.data");
                assert_eq!(loc.offset_bytes, 100);
                assert_eq!(loc.size_bytes, 1000);
                assert_eq!(loc.start_offset, 0);
                assert_eq!(loc.end_offset, 9);
            }
        }

        // Query offset in the middle (5)
        let result = registry
            .offset_location(topic.clone(), partition.clone(), 5)
            .await
            .unwrap();
        match result {
            OffsetLocation::Folio(loc) => {
                assert_eq!(loc.file_ref, "file1.data");
                assert_eq!(loc.offset_bytes, 100);
                assert_eq!(loc.size_bytes, 1000);
                assert_eq!(loc.start_offset, 0);
                assert_eq!(loc.end_offset, 9);
            }
        }

        // Query offset at the end (9)
        let result = registry
            .offset_location(topic.clone(), partition.clone(), 9)
            .await
            .unwrap();
        match result {
            OffsetLocation::Folio(loc) => {
                assert_eq!(loc.file_ref, "file1.data");
                assert_eq!(loc.offset_bytes, 100);
                assert_eq!(loc.size_bytes, 1000);
                assert_eq!(loc.start_offset, 0);
                assert_eq!(loc.end_offset, 9);
            }
        }

        // Test 4: Query offset out of range
        // Query offset after the end (10)
        let result = registry
            .offset_location(topic.clone(), partition.clone(), 10)
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(
            err.current_context(),
            OffsetRegistryError::OffsetNotFound(_, _, 10)
        ));

        // Query offset far after the end (100)
        let result = registry
            .offset_location(topic.clone(), partition.clone(), 100)
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(
            err.current_context(),
            OffsetRegistryError::OffsetNotFound(_, _, 100)
        ));

        // Test 5: Sequential batch commits
        // Create second batch with 5 more messages
        let batch2 = create_batch_to_commit(topic.clone(), partition.clone(), 5, 0, 500);

        // Commit the second batch
        let result2 = registry
            .commit_folio(namespace.clone(), "file2.data".to_string(), &[batch2])
            .await
            .unwrap();

        // Verify the second batch continues from the previous offset
        assert_eq!(result2[0].start_offset, 10);
        assert_eq!(result2[0].end_offset, 14);

        // Query first batch
        let result = registry
            .offset_location(topic.clone(), partition.clone(), 5)
            .await
            .unwrap();
        match result {
            OffsetLocation::Folio(loc) => {
                assert_eq!(loc.file_ref, "file1.data");
                assert_eq!(loc.start_offset, 0);
                assert_eq!(loc.end_offset, 9);
            }
        }

        // Query second batch
        let result = registry
            .offset_location(topic.clone(), partition.clone(), 12)
            .await
            .unwrap();
        match result {
            OffsetLocation::Folio(loc) => {
                assert_eq!(loc.file_ref, "file2.data");
                assert_eq!(loc.start_offset, 10);
                assert_eq!(loc.end_offset, 14);
            }
        }

        // Test 6: Committing multiple batches in a single call
        // Create a new registry for clarity
        let registry = InMemoryOffsetRegistry::new();

        // Create two topics
        let topic1 =
            TopicName::from_str("tenants/test-tenant/namespaces/test-namespace/topics/topic1")
                .unwrap();
        let topic2 =
            TopicName::from_str("tenants/test-tenant/namespaces/test-namespace/topics/topic2")
                .unwrap();

        // Create two different partition values
        let partition1 = Some(PartitionValue::String("partition1".to_string()));
        let partition2 = Some(PartitionValue::String("partition2".to_string()));

        // Create batches for each topic/partition
        let batch1 = create_batch_to_commit(topic1.clone(), partition1.clone(), 5, 0, 500);
        let batch2 = create_batch_to_commit(topic2.clone(), partition2.clone(), 10, 500, 1000);

        // Commit both batches in one call
        let result = registry
            .commit_folio(
                namespace.clone(),
                "multi-file.data".to_string(),
                &[batch1, batch2],
            )
            .await
            .unwrap();

        // Verify both committed batches
        assert_eq!(result.len(), 2);

        // Verify first batch
        assert_eq!(result[0].topic_name, topic1);
        assert_eq!(result[0].partition_value, partition1.clone());
        assert_eq!(result[0].start_offset, 0);
        assert_eq!(result[0].end_offset, 4); // 5 messages, 0-based

        // Verify second batch
        assert_eq!(result[1].topic_name, topic2);
        assert_eq!(result[1].partition_value, partition2.clone());
        assert_eq!(result[1].start_offset, 0);
        assert_eq!(result[1].end_offset, 9); // 10 messages, 0-based

        // Verify we can query both batches
        let result = registry
            .offset_location(topic1, partition1, 2)
            .await
            .unwrap();
        match result {
            OffsetLocation::Folio(loc) => {
                assert_eq!(loc.file_ref, "multi-file.data");
                assert_eq!(loc.offset_bytes, 0);
                assert_eq!(loc.size_bytes, 500);
                assert_eq!(loc.start_offset, 0);
                assert_eq!(loc.end_offset, 4);
            }
        }

        let result = registry
            .offset_location(topic2, partition2, 7)
            .await
            .unwrap();
        match result {
            OffsetLocation::Folio(loc) => {
                assert_eq!(loc.file_ref, "multi-file.data");
                assert_eq!(loc.offset_bytes, 500);
                assert_eq!(loc.size_bytes, 1000);
                assert_eq!(loc.start_offset, 0);
                assert_eq!(loc.end_offset, 9);
            }
        }

        // Test 7: Duplicate partition value error
        // Create two batches with the same topic and partition
        let batch1 = create_batch_to_commit(topic.clone(), partition.clone(), 5, 0, 500);
        let batch2 = create_batch_to_commit(topic.clone(), partition.clone(), 10, 500, 1000);

        // Commit both batches in one call - should fail with DuplicatePartitionValue
        let result = registry
            .commit_folio(
                namespace.clone(),
                "file.data".to_string(),
                &[batch1, batch2],
            )
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(
            err.current_context(),
            OffsetRegistryError::DuplicatePartitionValue(_, _)
        ));

        // Test 8: Multiple namespaces
        // Create two namespaces
        let namespace1 = NamespaceName::from_str("tenants/tenant1/namespaces/namespace1").unwrap();
        let namespace2 = NamespaceName::from_str("tenants/tenant2/namespaces/namespace2").unwrap();

        // Create topics in different namespaces
        let topic1 =
            TopicName::from_str("tenants/tenant1/namespaces/namespace1/topics/topic").unwrap();
        let topic2 =
            TopicName::from_str("tenants/tenant2/namespaces/namespace2/topics/topic").unwrap();

        let partition_value = Some(PartitionValue::String("partition".to_string()));

        // Create and commit a batch for each namespace
        let batch1 = create_batch_to_commit(topic1.clone(), partition_value.clone(), 10, 0, 1000);
        let batch2 = create_batch_to_commit(topic2.clone(), partition_value.clone(), 20, 0, 2000);

        // Commit to first namespace
        let _ = registry
            .commit_folio(namespace1, "ns1-file.data".to_string(), &[batch1])
            .await
            .unwrap();

        // Commit to second namespace
        let _ = registry
            .commit_folio(namespace2, "ns2-file.data".to_string(), &[batch2])
            .await
            .unwrap();

        // Verify we can query each namespace independently
        let result = registry
            .offset_location(topic1, partition_value.clone(), 5)
            .await
            .unwrap();
        match result {
            OffsetLocation::Folio(loc) => {
                assert_eq!(loc.file_ref, "ns1-file.data");
                assert_eq!(loc.start_offset, 0);
                assert_eq!(loc.end_offset, 9);
            }
        }

        let result = registry
            .offset_location(topic2, partition_value.clone(), 15)
            .await
            .unwrap();
        match result {
            OffsetLocation::Folio(loc) => {
                assert_eq!(loc.file_ref, "ns2-file.data");
                assert_eq!(loc.start_offset, 0);
                assert_eq!(loc.end_offset, 19);
            }
        }

        // Test 9: Query with non-existent topic and partition value
        let non_existent_topic = TopicName::from_str(
            "tenants/test-tenant/namespaces/test-namespace/topics/non-existent",
        )
        .unwrap();
        let non_existent_partition_value =
            Some(PartitionValue::String("non-existent-partition".to_string()));

        // Query with valid namespace but non-existent topic
        let result = registry
            .offset_location(non_existent_topic.clone(), partition_value.clone(), 0)
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(
            err.current_context(),
            OffsetRegistryError::OffsetNotFound(_, _, _)
        ));

        // Query with valid topic but non-existent partition
        let result = registry
            .offset_location(topic.clone(), non_existent_partition_value, 0)
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(
            err.current_context(),
            OffsetRegistryError::OffsetNotFound(_, _, _)
        ));

        // Test 10: Edge case with minimum values
        // Create a batch with just 1 message
        let batch_single = create_batch_to_commit(
            topic.clone(),
            partition_value.clone(),
            1,   // just one message
            123, // offset bytes
            10,  // very small size
        );

        // Commit the batch
        let result = registry
            .commit_folio(
                namespace.clone(),
                "edge-case.data".to_string(),
                &[batch_single],
            )
            .await
            .unwrap();

        // Verify the committed batch
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].topic_name, topic);
        assert_eq!(result[0].partition_value, partition_value);
        // The exact offset might vary depending on previous test execution,
        // so we just check that start_offset equals end_offset for a single message
        let start_offset = result[0].start_offset;
        assert_eq!(result[0].end_offset, start_offset); // Same as start for a single message

        // Query that exact offset
        let result = registry
            .offset_location(topic, partition_value, start_offset)
            .await
            .unwrap();
        match result {
            OffsetLocation::Folio(loc) => {
                assert_eq!(loc.file_ref, "edge-case.data");
                assert_eq!(loc.offset_bytes, 123);
                assert_eq!(loc.size_bytes, 10);
                assert_eq!(loc.start_offset, start_offset);
                assert_eq!(loc.end_offset, start_offset);
            }
        }

        // Test 11: Empty batch list
        // Commit with empty batch list should succeed but return empty result
        let result = registry
            .commit_folio(namespace, "empty.data".to_string(), &[])
            .await
            .unwrap();
        assert_eq!(result.len(), 0);
    }
}
