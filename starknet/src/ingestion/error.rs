//! Ingestion error.
use apibara_node::db::libmdbx;
use std::error::Error;

use crate::core::{InvalidBlock, InvalidBlockHashSize};

#[derive(Debug, thiserror::Error)]
pub enum BlockIngestionError {
    #[error("failed to fetch provider data")]
    Provider(#[from] Box<dyn Error + Send + Sync + 'static>),
    #[error("failed to perform database operation")]
    Database(#[from] libmdbx::Error),
    #[error("block does not contain header")]
    MissingBlockHeader,
    #[error("block doesn't have hash")]
    MissingBlockHash,
    #[error("transaction is missing data")]
    MalformedTransaction,
    #[error("database is in an inconsistent state")]
    InconsistentDatabase,
    #[error("tried to access a block as canonical, but it's not")]
    BlockNotCanonical,
    #[error(transparent)]
    InvalidBlockHash(#[from] InvalidBlockHashSize),
    #[error(transparent)]
    InvalidBlock(#[from] InvalidBlock),
}

impl BlockIngestionError {
    pub(crate) fn provider<E>(err: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        BlockIngestionError::Provider(Box::new(err))
    }
}
