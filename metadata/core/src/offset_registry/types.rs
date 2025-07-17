//! Data types for the offset registry.

use crate::{admin::TopicName, partition::PartitionValue};

/// A batch that needs to be committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchToCommit {
    /// The topic id of the batch to commit.
    pub topic_name: TopicName,
    /// The partition value, if any.
    pub partition_value: Option<PartitionValue>,
    /// The number of messages in the batch.
    pub num_messages: u32,
    /// The start offset of the batch in the folio file.
    pub offset_bytes: u64,
    /// The batch size, in bytes.
    pub batch_size_bytes: u64,
}

/// A batch that has been successfully committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedBatch {
    /// The topic id of the batch that was committed.
    pub topic_name: TopicName,
    /// The partition value, if any.
    pub partition_value: Option<PartitionValue>,
    /// The first assigned offset of the batch.
    pub start_offset: u64,
    /// The last assigned offset of the batch.
    pub end_offset: u64,
}

/// Location of a specific offset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OffsetLocation {
    Folio(FolioLocation),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FolioLocation {
    /// Folio file name.
    pub file_ref: String,
    /// Offset within the folio file.
    pub offset_bytes: u64,
    /// Size of the partition data in the folio file.
    pub size_bytes: u64,
    /// First offset of the partition data in the folio file.
    pub start_offset: u64,
    /// Last offset of the partition data in the folio file.
    pub end_offset: u64,
}
