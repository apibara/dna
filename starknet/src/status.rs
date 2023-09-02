use std::sync::Arc;

use apibara_core::node::v1alpha2::StatusResponse;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::{
    core::GlobalBlockId, core::IngestionMessage, ingestion::IngestionStreamClient,
    provider::Provider,
};

#[derive(Debug, thiserror::Error)]
pub enum StatusServiceError {
    #[error("failed to send message to status service")]
    MessageSendError(#[from] mpsc::error::SendError<Message>),
    #[error("failed to receive response from status service")]
    MessageReceiveError(#[from] oneshot::error::RecvError),
}

#[derive(Debug)]
pub enum Message {
    GetStatus(oneshot::Sender<StatusResponse>),
}

pub struct StatusService<G: Provider> {
    provider: Arc<G>,
    ingestion: Arc<IngestionStreamClient>,
    rx: mpsc::Receiver<Message>,
}

pub struct StatusClient {
    tx: mpsc::Sender<Message>,
}

impl<G: Provider> StatusService<G> {
    pub fn new(provider: Arc<G>, ingestion: IngestionStreamClient) -> (Self, StatusClient) {
        let (tx, rx) = mpsc::channel(32);
        let server = Self {
            provider,
            ingestion: Arc::new(ingestion),
            rx,
        };
        let client = StatusClient { tx };
        (server, client)
    }

    pub async fn start(mut self, ct: CancellationToken) -> Result<(), StatusServiceError> {
        let mut ingestion = self.ingestion.subscribe().await;

        let mut last_ingested: Option<GlobalBlockId> = None;

        loop {
            if ct.is_cancelled() {
                break;
            }

            tokio::select! {
                _ = ct.cancelled() => break,
                client_msg = self.rx.recv() => {
                    match client_msg {
                        None => {
                            warn!("status client stream closed");
                            break;
                        },
                        Some(Message::GetStatus(tx)) => {
                            let current_head = self.get_chain_head().await;
                            let response = StatusResponse {
                                current_head: current_head.map(|c| c.to_cursor()),
                                last_ingested: last_ingested.map(|c| c.to_cursor()),
                            };
                            let _ = tx.send(response);
                        }
                    }
                }
                ingestion_msg = ingestion.next() => {
                    match ingestion_msg {
                        None => {
                            warn!("ingestion stream in status service closed");
                            break;
                        }
                        Some(Err(err)) => {
                            warn!("ingestion stream in status service error: {}", err);
                            break;
                        }
                        Some(Ok(IngestionMessage::Finalized(cursor))) => {
                            // Only update finalized cursor if it is newer than the last ingested cursor.
                            if let Some(prev) = last_ingested {
                                if prev.number() < cursor.number() {
                                    last_ingested = Some(cursor);
                                }
                            } else {
                                last_ingested = Some(cursor);
                            }
                        }
                        Some(Ok(IngestionMessage::Accepted(cursor))) => {
                            last_ingested = Some(cursor);
                        }
                        Some(Ok(IngestionMessage::Pending(_))) => {
                            // do nothing
                        }
                        Some(Ok(IngestionMessage::Invalidate(cursor))) => {
                            last_ingested = Some(cursor);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn get_chain_head(&self) -> Option<GlobalBlockId> {
        self.provider.get_head().await.ok()
    }
}

impl StatusClient {
    /// Request the status of the node to the status service.
    pub async fn get_status(&self) -> Result<StatusResponse, StatusServiceError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Message::GetStatus(tx)).await?;
        let response = rx.await?;
        Ok(response)
    }
}
