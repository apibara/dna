//! Provides unified access to the archive node data.
//!
//! The [Erigon] type abstracts away the details about data storage (snapshot vs db) and provides a
//! simple interface to access the data.

use croaring::Bitmap;
use reth_primitives::{Header, H160};

use crate::{
    erigon::{Forkchoice, GlobalBlockId},
    remote::{LogStream, RemoteDB, RemoteDBError},
    snapshot::reader::{SnapshotReader, SnapshotReaderError},
};

pub struct Erigon {
    snapshot: SnapshotReader,
    remote_db: RemoteDB,
}

#[derive(Debug, thiserror::Error)]
pub enum ErigonError {
    #[error(transparent)]
    Snapshot(#[from] SnapshotReaderError),
    #[error(transparent)]
    Remote(#[from] RemoteDBError),
}

impl Erigon {
    /// Creates a new erigon instance.
    pub fn new(snapshot: SnapshotReader, remote_db: RemoteDB) -> Self {
        Self {
            snapshot,
            remote_db,
        }
    }

    /// Returns the header for the given block number.
    pub async fn header_by_number(
        &mut self,
        block_number: u64,
    ) -> Result<Option<Header>, ErigonError> {
        if let Some(header) = self.snapshot.header_by_number(block_number)? {
            return Ok(Some(header));
        }
        let header = self.remote_db.header_by_number(block_number).await?;
        Ok(header)
    }

    pub async fn canonical_block_id(
        &mut self,
        block_number: u64,
    ) -> Result<Option<GlobalBlockId>, RemoteDBError> {
        Ok(self.remote_db.canonical_block_id(block_number).await?)
    }

    /// Returns the header for the given block number.
    pub async fn finalized_block(&mut self) -> Result<Option<GlobalBlockId>, ErigonError> {
        Ok(self
            .remote_db
            .forkchoice(Forkchoice::FinalizedBlockHash)
            .await?)
    }

    pub async fn log_bitmap_for_addresses(
        &mut self,
        addresses: impl Iterator<Item = H160>,
        from_block: u64,
        to_block: u64,
    ) -> Result<Bitmap, ErigonError> {
        Ok(self
            .remote_db
            .log_bitmap_for_addresses(addresses, from_block, to_block)
            .await?)
    }

    /// Returns a stream over the logs in the given block.
    pub async fn logs_by_block_number(
        &mut self,
        block_number: u64,
    ) -> Result<LogStream<'_>, ErigonError> {
        let stream = self.remote_db.logs_by_block_number(block_number).await?;
        Ok(stream)
    }
}
