use std::time::Duration;

use apibara_core::node;
use color_eyre::eyre::{eyre, Context, Result};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tonic_health::pb::health_server::{Health, HealthServer};
use tracing::info;

use crate::status::server::StatusServer;

use super::client::{StatusMessage, StatusServerClient};

const MESSAGE_TIMEOUT: Duration = Duration::from_secs(60);

/// Message between the grpc service and the status service.
#[derive(Debug)]
enum RequestMessage {
    /// Request indexer cursors (starting, current)
    GetCursor(oneshot::Sender<Cursors>),
}

#[derive(Debug)]
pub struct Cursors {
    pub starting: Option<node::v1alpha2::Cursor>,
    pub current: Option<node::v1alpha2::Cursor>,
}

pub struct StatusService {
    health_reporter: tonic_health::server::HealthReporter,
    request_rx: mpsc::Receiver<RequestMessage>,
    status_rx: mpsc::Receiver<StatusMessage>,
}

/// Request data from the status service.
#[derive(Clone)]
pub struct StatusServiceClient {
    tx: mpsc::Sender<RequestMessage>,
}

impl StatusService {
    pub fn new() -> (
        Self,
        StatusServerClient,
        StatusServiceClient,
        HealthServer<impl Health>,
    ) {
        let (health_reporter, health_service) = tonic_health::server::health_reporter();

        let (status_tx, status_rx) = mpsc::channel(128);
        let status_client = StatusServerClient::new(status_tx);

        let (request_tx, request_rx) = mpsc::channel(128);
        let status_service_client = StatusServiceClient { tx: request_tx };

        let service = StatusService {
            health_reporter,
            status_rx,
            request_rx,
        };

        (
            service,
            status_client,
            status_service_client,
            health_service,
        )
    }

    pub async fn start(mut self, ct: CancellationToken) -> Result<()> {
        // This loop:
        //
        //  - Tracks the most recently processed cursor.
        //  - Responds to requests by the status grpc service.
        //  - Sets the health status to not serving if no messages are received for a while.
        //  - Sets the health status to serving if a message is received.
        let mut starting_cursor = None;
        let mut cursor = None;
        loop {
            tokio::select! {
                msg = self.status_rx.recv() => {
                    let msg = msg.ok_or_else(|| eyre!("status server stopped: status channel closed"))?;
                    match msg {
                        StatusMessage::Heartbeat => {},
                        StatusMessage::UpdateCursor(new_cursor) => {
                            self.health_reporter.set_serving::<StatusServer>().await;
                            cursor = new_cursor;
                        }
                        StatusMessage::SetStartingCursor(new_starting_cursor) => {
                            self.health_reporter.set_serving::<StatusServer>().await;
                            starting_cursor = new_starting_cursor;
                        }
                    }
                }
                msg = self.request_rx.recv() => {
                    let msg = msg.ok_or_else(|| eyre!("status server stopped: request channel closed"))?;
                    match msg {
                        RequestMessage::GetCursor(tx) => {
                            let cursors = Cursors {
                                starting: starting_cursor.clone(),
                                current: cursor.clone(),
                            };
                            tx.send(cursors).ok().ok_or_else(|| eyre!("failed to send cursor to status server"))?;
                        }
                    }
                }
                _ = tokio::time::sleep(MESSAGE_TIMEOUT) => {
                    self.health_reporter.set_not_serving::<StatusServer>().await;
                }
                _ = ct.cancelled() => {
                    info!("status server stopped: cancelled");
                    break;
                }
            }
        }

        Ok(())
    }
}

impl StatusServiceClient {
    /// Request the current cursor from the status service.
    pub async fn get_cursors(&self) -> Result<Cursors> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(RequestMessage::GetCursor(tx))
            .await
            .context("failed to send get cursor message to status service")?;
        let cursors = rx
            .await
            .context("failed to receive cursor from status service")?;
        Ok(cursors)
    }
}
