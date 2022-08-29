//! Store blocks produced by the node.

use std::sync::Arc;

use apibara_core::stream::Sequence;
use apibara_node::{
    db::libmdbx::{Environment, EnvironmentKind, Error as MdxbError},
    message_storage::{MessageStorage, MessageStorageError},
};
use tokio::sync::broadcast::{error::RecvError, Receiver};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::core::Block;

pub struct BlockStorage<E: EnvironmentKind> {
    storage: MessageStorage<E, Block>,
}

#[derive(Debug, thiserror::Error)]
pub enum BlockStorageError {
    #[error("database error")]
    Database(#[from] MdxbError),
    #[error("message storage error")]
    MessageStorage(#[from] MessageStorageError),
    #[error("error receiving message")]
    Broadcast(#[from] RecvError),
}

pub type Result<T> = std::result::Result<T, BlockStorageError>;

impl<E> BlockStorage<E>
where
    E: EnvironmentKind,
{
    /// Creates a new block storage service.
    pub fn new(db: Arc<Environment<E>>) -> Result<Self> {
        let storage = MessageStorage::new(db)?;
        Ok(BlockStorage { storage })
    }

    pub async fn start(&self, mut block_rx: Receiver<Block>, ct: CancellationToken) -> Result<()> {
        loop {
            tokio::select! {
                _ = ct.cancelled() => {
                    return Ok(())
                }
                maybe_block = block_rx.recv() => {
                    match maybe_block {
                        Ok(block) => {
                            let sequence = Sequence::from_u64(block.block_number);
                            self.storage.insert(&sequence, &block)?;
                        }
                        Err(err) => {
                            return Err(BlockStorageError::Broadcast(err))
                        }
                    }
                }
            }
        }
    }
}
