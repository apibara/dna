use apibara_core::node;
use error_stack::Result;
use tokio::sync::mpsc::{self, error::TrySendError};

use crate::{StatusServerError, StatusServerResultExt};

/// Message between the connector and the status service.
#[derive(Debug)]
pub enum StatusMessage {
    /// Set the starting cursor.
    SetStartingCursor(Option<node::v1alpha2::Cursor>),
    /// Update the most recently indexed cursor.
    UpdateCursor(Option<node::v1alpha2::Cursor>),
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
    pub async fn heartbeat(&self) -> Result<(), StatusServerError> {
        self.tx
            .send(StatusMessage::Heartbeat)
            .await
            .status_server_error("failed to send heartbeat request")?;
        Ok(())
    }

    /// Update the most recently processed cursor.
    pub async fn set_starting_cursor(
        &self,
        cursor: Option<node::v1alpha2::Cursor>,
    ) -> Result<(), StatusServerError> {
        self.tx
            .send(StatusMessage::SetStartingCursor(cursor))
            .await
            .status_server_error("failed to send starting cursor request")?;
        Ok(())
    }

    /// Update the most recently processed cursor.
    pub async fn update_cursor(
        &self,
        cursor: Option<node::v1alpha2::Cursor>,
    ) -> Result<(), StatusServerError> {
        match self.tx.try_send(StatusMessage::UpdateCursor(cursor)) {
            Ok(_) => Ok(()),
            // If the channel is full, we don't care.
            // This happens when the indexer is resyncing since it publishes hundreds of messages per second.
            Err(TrySendError::Full(_)) => Ok(()),
            Err(TrySendError::Closed(_)) => Err(StatusServerError::new(
                "failed to send update cursor request",
            )),
        }
    }
}
