use thiserror::Error;

use crate::{
    admin::{NamespaceName, TopicName},
    partition::PartitionValue,
    resource::ResourceError,
};

/// Errors that can occur during batch committer operations.
#[derive(Error, Debug)]
pub enum OffsetRegistryError {
    #[error("duplicate partition value: {0} {1:?}")]
    DuplicatePartitionValue(TopicName, Option<PartitionValue>),
    #[error("namespace not found: {0}")]
    NamespaceNotFound(NamespaceName),
    #[error("offset not found for topic: {0}, partition: {1:?}, offset: {2}")]
    OffsetNotFound(TopicName, Option<PartitionValue>, u64),
    #[error("invalid offset range")]
    InvalidOffsetRange,
    #[error("internal error: {0}")]
    Internal(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("invalid resource name")]
    InvalidResourceName(#[from] ResourceError),
}

pub type OffsetRegistryResult<T> = error_stack::Result<T, OffsetRegistryError>;
