use std::sync::Arc;

use apibara_core::starknet::v1alpha2;
use apibara_node::db::libmdbx::{Environment, EnvironmentKind, Error as MdxError};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    core::GlobalBlockId,
    db::{DatabaseStorage, StorageWriter},
    provider::Provider,
};

#[derive(Debug, thiserror::Error)]
pub enum HealerError {
    #[error("channel was closed")]
    ChannelClosed,
    #[error("database error")]
    Database(#[from] MdxError),
}

#[derive(Debug, Clone)]
pub enum HealerMessage {
    /// The given block is expected to be finalized.
    StatusFinalizedExpected(GlobalBlockId),
}

/// A service that receives broken blocks and heals them.
pub struct Healer<G: Provider + Send, E: EnvironmentKind> {
    _provider: Arc<G>,
    storage: DatabaseStorage<E>,
    rx: Receiver<HealerMessage>,
}

#[derive(Clone)]
pub struct HealerClient {
    tx: Sender<HealerMessage>,
}

impl<G, E> Healer<G, E>
where
    G: Provider + Send,
    E: EnvironmentKind,
{
    pub fn new(provider: Arc<G>, db: Arc<Environment<E>>) -> (HealerClient, Self) {
        let storage = DatabaseStorage::new(db);
        let (tx, rx) = mpsc::channel(64);
        let healer = Healer {
            _provider: provider,
            storage,
            rx,
        };
        let client = HealerClient { tx };
        (client, healer)
    }

    pub async fn start(mut self, ct: CancellationToken) -> Result<(), HealerError> {
        loop {
            tokio::select! {
                _ = ct.cancelled() => {
                    return Ok(())
                }
                msg = self.rx.recv() => {
                    let msg = msg.ok_or(HealerError::ChannelClosed)?;
                    self.handle_message(msg)?;
                }
            }
        }
    }

    fn handle_message(&self, message: HealerMessage) -> Result<(), HealerError> {
        info!(message = ?message, "received healer message");
        match message {
            HealerMessage::StatusFinalizedExpected(cursor) => {
                self.handle_status_finalized_expected(cursor)
            }
        }
    }

    fn handle_status_finalized_expected(&self, cursor: GlobalBlockId) -> Result<(), HealerError> {
        let mut txn = self.storage.begin_txn()?;
        txn.write_status(&cursor, v1alpha2::BlockStatus::AcceptedOnL1)?;
        txn.commit()?;
        Ok(())
    }
}

impl HealerClient {
    pub fn status_finalized_expected(&self, cursor: GlobalBlockId) {
        self.send_message(HealerMessage::StatusFinalizedExpected(cursor))
    }

    fn send_message(&self, message: HealerMessage) {
        // healer is not critical so don't fail if it cannot send
        if let Err(err) = self.tx.try_send(message) {
            warn!(error = ?err, "failed to send healer message");
        }
    }
}
