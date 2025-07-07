//! The batch committer trait and related types.

pub mod error;
pub mod memory;
pub mod types;

pub use error::{BatchCommitterError, BatchCommitterResult};
pub use memory::InMemoryBatchCommitter;
pub use types::*;

use crate::admin::NamespaceName;
use async_trait::async_trait;

/// The BatchCommitter trait provides methods for committing batches of messages.
///
/// This trait models batch commit operations and will be implemented by:
/// - `InMemoryBatchCommitter`: stores everything in memory for testing and development
/// - `RemoteBatchCommitter`: communicates with a remote batch committer service via gRPC
#[async_trait]
pub trait BatchCommitter: Send + Sync {
    /// Commit a batch of messages to the specified namespace.
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace where the batch should be committed
    /// * `file_ref` - Reference to the file containing the batch data
    /// * `batches` - The batches to commit
    ///
    /// # Returns
    ///
    /// Returns a `Vec<CommittedBatch>` on success, or a `BatchCommitterError` on failure.
    async fn commit_batch(
        &self,
        namespace: NamespaceName,
        file_ref: String,
        batches: &[BatchToCommit],
    ) -> BatchCommitterResult<Vec<CommittedBatch>>;
}
