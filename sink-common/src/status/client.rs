use std::fmt;

use apibara_core::node;
use error_stack::{Result, ResultExt};
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct StatusServerClientError;
impl error_stack::Context for StatusServerClientError {}

impl fmt::Display for StatusServerClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("failed to communicate with status server")
    }
}

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
    pub async fn heartbeat(&self) -> Result<(), StatusServerClientError> {
        self.tx
            .send(StatusMessage::Heartbeat)
            .await
            .change_context(StatusServerClientError)
            .attach_printable("failed to send heartbeat request")?;
        Ok(())
    }

    /// Update the most recently processed cursor.
    pub async fn set_starting_cursor(
        &self,
        cursor: Option<node::v1alpha2::Cursor>,
    ) -> Result<(), StatusServerClientError> {
        self.tx
            .send(StatusMessage::SetStartingCursor(cursor))
            .await
            .change_context(StatusServerClientError)
            .attach_printable("failed to send starting cursor request")?;
        Ok(())
    }

    /// Update the most recently processed cursor.
    pub async fn update_cursor(
        &self,
        cursor: Option<node::v1alpha2::Cursor>,
    ) -> Result<(), StatusServerClientError> {
        self.tx
            .send(StatusMessage::UpdateCursor(cursor))
            .await
            .change_context(StatusServerClientError)
            .attach_printable("failed to send update cursor request")?;
        Ok(())
    }
}
