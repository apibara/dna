//! Data types for batch committer operations.

use crate::{admin::TopicName, partition::PartitionValue};

/// A batch that needs to be committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchToCommit {
    /// The topic id of the batch to commit.
    pub topic_id: TopicName,
    /// The partition value, if any.
    pub partition_value: Option<PartitionValue>,
    /// The number of rows in the batch.
    pub num_rows: u32,
    /// The start offset of the batch in the file.
    pub offset_bytes: u64,
    /// The batch size, in bytes.
    pub batch_size_bytes: u64,
}

/// A batch that has been successfully committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedBatch {
    /// The topic id of the batch that was committed.
    pub topic_id: TopicName,
    /// The partition value, if any.
    pub partition_value: Option<PartitionValue>,
    /// The first assigned offset of the batch.
    pub start_offset: u64,
    /// The last assigned offset of the batch.
    pub end_offset: u64,
}
