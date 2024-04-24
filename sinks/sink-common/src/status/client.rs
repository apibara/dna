use apibara_dna_protocol::dna::common::Cursor;
use error_stack::Result;
use tokio::sync::mpsc::{self, error::TrySendError};

use crate::{SinkError, SinkErrorResultExt};

/// Message between the connector and the status service.
#[derive(Debug)]
pub enum StatusMessage {
    /// Set the starting cursor.
    SetStartingCursor(Option<Cursor>),
    /// Update the most recently indexed cursor.
    UpdateCursor(Option<Cursor>),
    /// Send a heartbeat to the status service.
    Heartbeat,
}

#[derive(Clone)]
pub struct StatusServerClient {
    tx: mpsc::Sender<StatusMessage>,
}

impl StatusServerClient {
    pub fn new(tx: mpsc::Sender<StatusMessage>) -> Self {
        StatusServerClient { tx }
    }

    /// Send heartbeat message to status server.
    pub async fn heartbeat(&self) -> Result<(), SinkError> {
        self.tx
            .send(StatusMessage::Heartbeat)
            .await
            .status("failed to send heartbeat request")?;
        Ok(())
    }

    /// Update the most recently processed cursor.
    pub async fn set_starting_cursor(&self, cursor: Option<Cursor>) -> Result<(), SinkError> {
        self.tx
            .send(StatusMessage::SetStartingCursor(cursor))
            .await
            .status("failed to send starting cursor request")?;
        Ok(())
    }

    /// Update the most recently processed cursor.
    pub async fn update_cursor(&self, cursor: Option<Cursor>) -> Result<(), SinkError> {
        match self.tx.try_send(StatusMessage::UpdateCursor(cursor)) {
            Ok(_) => Ok(()),
            // If the channel is full, we don't care.
            // This happens when the indexer is resyncing since it publishes hundreds of messages per second.
            Err(TrySendError::Full(_)) => Ok(()),
            Err(TrySendError::Closed(_)) => {
                Err(SinkError::status("failed to send update cursor request"))
            }
        }
    }
}
