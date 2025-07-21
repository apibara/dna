//! This module contains types used by the ingestor.
//!
//! ## Data flow
//!
//! **Batcher**: [`Batch`] sequence -> [`NamespaceFolio`].
//!
//! **Uploader**: [`NamespaceFolio`] -> [`UploadedNamespaceFolioMetadata`].
//!
//! **Committer**: [`UploadedNamespaceFolioMetadata`] -> [`CommittedNamespaceFolioMetadata`].
use std::fmt::Debug;

use arrow::array::RecordBatch;
use error_stack::Report;
use tokio_util::time::delay_queue;
use wings_metadata_core::{
    admin::{NamespaceRef, TopicName, TopicRef},
    offset_registry::BatchToCommit,
    partition::PartitionValue,
};

use crate::{batch::WriteReplySender, error::IngestorError};

/// An error that should be returned to the client.
#[derive(Debug)]
pub struct ReplyWithError {
    /// The reply channel.
    pub reply: WriteReplySender,
    /// The error.
    pub error: Report<IngestorError>,
}

/// A batch of messages.
#[derive(Debug)]
pub struct Batch {
    /// The namespace.
    pub namespace: NamespaceRef,
    /// The topic.
    pub topic: TopicRef,
    /// The partition value.
    pub partition: Option<PartitionValue>,
    /// The records.
    pub records: RecordBatch,
}

/// The context for a batch of messages.
///
/// This is needed to carry around the reply channel and the original number of
/// messages in the batch.
#[derive(Debug)]
pub struct BatchContext {
    /// The reply sender for the batch.
    pub reply: WriteReplySender,
    /// The number of messages in the batch.
    pub num_messages: u32,
}

/// A folio of data for a partition.
pub struct PartitionFolio {
    /// The topic name for the partition.
    pub topic_name: TopicName,
    /// The partition value for the partition.
    pub partition_value: Option<PartitionValue>,
    /// The serialized data.
    pub data: Vec<u8>,
    /// The batches that contributed to the folio.
    pub batches: Vec<BatchContext>,
}

/// A folio of data for a namespace.
#[derive(Debug)]
pub struct NamespaceFolio {
    /// The namespace.
    pub namespace: NamespaceRef,
    /// The timer key used to periodically flush the folio.
    pub timer_key: delay_queue::Key,
    /// The partitions' data.
    pub partitions: Vec<PartitionFolio>,
}

/// Metadata for a serialized partition folio.
#[derive(Debug)]
pub struct SerializedPartitionFolioMetadata {
    /// The topic name for the partition.
    pub topic_name: TopicName,
    /// The partition value for the partition.
    pub partition_value: Option<PartitionValue>,
    /// The TOTAL number of messages in the folio.
    pub num_messages: u32,
    /// The start offset of the batch in the folio file.
    pub offset_bytes: u64,
    /// The size in bytes.
    pub size_bytes: u64,
    /// The batches that contributed to the folio.
    pub batches: Vec<BatchContext>,
}

/// Metadata for a namespace folio that was uploaded.
#[derive(Debug)]
pub struct UploadedNamespaceFolioMetadata {
    /// The namespace.
    pub namespace: NamespaceRef,
    /// The filename with the namespace folio.
    pub file_ref: String,
    /// The partitions' metadata.
    pub partitions: Vec<SerializedPartitionFolioMetadata>,
}

/// Metadata for a partition folio committed.
#[derive(Debug)]
pub struct CommittedPartitionFolioMetadata {
    /// The topic.
    pub topic_name: TopicName,
    /// The partition.
    pub partition_value: Option<PartitionValue>,
    /// The first offset in the committed messages.
    pub start_offset: u64,
    /// The last offset in the committed messages.
    pub end_offset: u64,
    /// The batches that contributed to the folio.
    pub batches: Vec<BatchContext>,
}

/// Metadata for a namespace folio that was committed.
#[derive(Debug)]
pub struct CommittedNamespaceFolioMetadata {
    /// The namespace.
    pub namespace: NamespaceRef,
    /// The partitions' metadata.
    pub partitions: Vec<CommittedPartitionFolioMetadata>,
}

impl ReplyWithError {
    pub fn send(self) {
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

impl SerializedPartitionFolioMetadata {
    pub fn into_batch_to_commit(self) -> (BatchToCommit, Vec<BatchContext>) {
        let commit = BatchToCommit {
            topic_name: self.topic_name,
            partition_value: self.partition_value,
            num_messages: self.num_messages,
            offset_bytes: self.offset_bytes,
            batch_size_bytes: self.size_bytes,
        };

        (commit, self.batches)
    }
}
