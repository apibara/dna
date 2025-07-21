use arrow::array::RecordBatch;
use error_stack::report;
use tokio::sync::oneshot;
use wings_metadata_core::{
    admin::{NamespaceRef, TopicRef},
    partition::PartitionValue,
};

use crate::error::{IngestorError, IngestorResult};

#[derive(Debug)]
pub struct Batch {
    pub namespace: NamespaceRef,
    pub topic: TopicRef,
    pub partition: Option<PartitionValue>,
    pub records: RecordBatch,
}

#[derive(Debug, Clone)]
pub struct WriteInfo {
    pub start_offset: u64,
    pub end_offset: u64,
}

pub type WriteReplySender = oneshot::Sender<IngestorResult<WriteInfo>>;

impl Batch {
    pub fn validate(&self) -> IngestorResult<()> {
        if self.topic.partition_key.is_none() && self.partition.is_some() {
            return Err(report!(IngestorError::Validation(format!(
                "topic {} does not specify a partition key but batch contains partition data",
                self.topic.name
            ))));
        }

        if self.topic.name.parent() != &self.namespace.name {
            return Err(report!(IngestorError::Validation(format!(
                "topic namespace {} does not match provided namespace {}",
                self.topic.name.parent(),
                self.namespace.name
            ))));
        }

        Ok(())
    }
}
