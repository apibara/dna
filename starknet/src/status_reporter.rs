//! Compute indexing status.

use std::sync::Arc;

use apibara_core::node::v1alpha1::pb::{
    status_response, InputSyncingStatus, StatusResponse, SyncedStatus, SyncingStatus,
};
use apibara_node::{
    chain_tracker::{ChainTracker, ChainTrackerError},
    db::libmdbx::EnvironmentKind,
};

use crate::core::Block;

/// Information about the sync status.
pub struct StatusReporter<E: EnvironmentKind> {
    chain: Arc<ChainTracker<Block, E>>,
}

#[derive(Debug, thiserror::Error)]
pub enum StatusReporterError {
    #[error("error fetching chain information")]
    ChainTracker(#[from] ChainTrackerError),
}

pub type Result<T> = std::result::Result<T, StatusReporterError>;

impl<E> StatusReporter<E>
where
    E: EnvironmentKind,
{
    /// Creates a new status reporter.
    pub fn new(chain: Arc<ChainTracker<Block, E>>) -> Self {
        StatusReporter { chain }
    }

    pub fn status(&self) -> Result<StatusResponse> {
        let indexed = self.chain.latest_indexed_block()?;
        let head = self.chain.head_height()?;

        match (indexed, head) {
            (Some(indexed), Some(head)) => {
                if indexed.block_number == head {
                    let synced = SyncedStatus { sequence: head };
                    return Ok(StatusResponse {
                        message: Some(status_response::Message::Synced(synced)),
                    });
                }

                let input = InputSyncingStatus {
                    head,
                    indexed: indexed.block_number,
                };
                let syncing = SyncingStatus {
                    sequence: indexed.block_number,
                    inputs: vec![input],
                };
                Ok(StatusResponse {
                    message: Some(status_response::Message::Syncing(syncing)),
                })
            }
            (_, _) => {
                let not_started = apibara_core::node::v1alpha1::pb::NotStartedStatus {};
                Ok(StatusResponse {
                    message: Some(status_response::Message::NotStarted(not_started)),
                })
            }
        }
    }
}
