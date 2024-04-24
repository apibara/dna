use std::time::{Duration, Instant};

use apibara_dna_protocol::dna::{
    common::{Cursor, StatusRequest},
    stream::dna_stream_client::DnaStreamClient,
};
use apibara_observability::ObservableGauge;
use error_stack::{Result, ResultExt};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tonic_health::pb::health_server::{Health, HealthServer};
use tracing::info;

use crate::{status::server::StatusServer, SinkError, SinkErrorReportExt, SinkErrorResultExt};

use super::client::{StatusMessage, StatusServerClient};

const MESSAGE_TIMEOUT: Duration = Duration::from_secs(60);
const METRICS_PUBLISH_INTERVAL: Duration = Duration::from_secs(10);

/// Message between the grpc service and the status service.
#[derive(Debug)]
enum RequestMessage {
    /// Request indexer cursors (starting, current)
    GetCursor(oneshot::Sender<Cursors>),
}

#[derive(Debug)]
pub struct Cursors {
    /// Indexer's starting cursor.
    pub starting: Option<Cursor>,
    /// Indexer's current cursor.
    pub current: Option<Cursor>,
    /// Chain's head cursor.
    pub head: Option<Cursor>,
}

pub struct StatusService {
    health_reporter: tonic_health::server::HealthReporter,
    stream_client: DnaStreamClient<Channel>,
    request_rx: mpsc::Receiver<RequestMessage>,
    status_rx: mpsc::Receiver<StatusMessage>,
}

/// Request data from the status service.
#[derive(Clone)]
pub struct StatusServiceClient {
    tx: mpsc::Sender<RequestMessage>,
}

impl StatusService {
    pub fn new(
        stream_client: DnaStreamClient<Channel>,
    ) -> (
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
            stream_client,
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

    pub async fn start(mut self, ct: CancellationToken) -> Result<(), SinkError> {
        // This loop:
        //
        //  - Tracks the most recently processed cursor.
        //  - Responds to requests by the status grpc service.
        //  - Sets the health status to not serving if no messages are received for a while.
        //  - Sets the health status to serving if a message is received.
        let mut starting_cursor = None;
        let mut cursor = None;

        let metrics = SinkMetrics::default();
        let mut last_metrics_published = Instant::now();

        loop {
            tokio::select! {
                biased;

                msg = self.request_rx.recv() => {
                    let msg = msg.ok_or(SinkError::status("client request channel closed"))?;
                    match msg {
                        RequestMessage::GetCursor(tx) => {
                            let head = self.get_dna_head().await?;

                            let cursors = Cursors {
                                starting: starting_cursor.clone(),
                                current: cursor.clone(),
                                head,
                            };

                            tx.send(cursors)
                                .map_err(|_| SinkError::status("failed to reply with cursor"))?;
                        }
                    }
                }

                msg = self.status_rx.recv() => {
                    let msg = msg.ok_or(SinkError::status("status channel closed"))?;
                    match msg {
                        StatusMessage::Heartbeat => {},
                        StatusMessage::UpdateCursor(new_cursor) => {
                            // Publish metrics every interval.
                            if last_metrics_published.elapsed() > METRICS_PUBLISH_INTERVAL {
                                let head = self.get_dna_head().await?;
                                metrics.sync_current(&new_cursor);
                                metrics.sync_head(&head);
                                last_metrics_published = Instant::now();
                            }

                            self.health_reporter.set_serving::<StatusServer>().await;
                            cursor = new_cursor;
                        }
                        StatusMessage::SetStartingCursor(new_starting_cursor) => {
                            // Always publish starting cursor.
                            metrics.sync_start(&new_starting_cursor);

                            self.health_reporter.set_serving::<StatusServer>().await;
                            starting_cursor = new_starting_cursor;
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

    async fn get_dna_head(&self) -> Result<Option<Cursor>, SinkError> {
        let dna_status = self
            .stream_client
            .clone()
            .status(StatusRequest::default())
            .await
            .change_context(SinkError::Status)
            .attach_printable("failed to get dna status")?
            .into_inner();

        Ok(dna_status.current_head)
    }
}

impl StatusServiceClient {
    /// Request the current cursor from the status service.
    pub async fn get_cursors(&self) -> Result<Cursors, SinkError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(RequestMessage::GetCursor(tx))
            .await
            .status("failed to send get cursor request")?;
        let cursors = rx.await.status("failed to receive cursor response")?;
        Ok(cursors)
    }
}

struct SinkMetrics {
    sync_start: ObservableGauge<u64>,
    sync_current: ObservableGauge<u64>,
    sync_head: ObservableGauge<u64>,
}

impl Default for SinkMetrics {
    fn default() -> Self {
        let meter = apibara_observability::meter("sink");
        let sync_start = meter
            .u64_observable_gauge("sync_start")
            .with_description("Starting block number")
            .init();

        let sync_current = meter
            .u64_observable_gauge("sync_current")
            .with_description("Current (most recently indexed) block number")
            .init();

        let sync_head = meter
            .u64_observable_gauge("sync_head")
            .with_description("DNA stream head block number")
            .init();

        SinkMetrics {
            sync_start,
            sync_current,
            sync_head,
        }
    }
}

impl SinkMetrics {
    pub fn sync_start(&self, cursor: &Option<Cursor>) {
        if let Some(cursor) = cursor {
            let cx = apibara_observability::Context::current();
            self.sync_start.observe(&cx, cursor.order_key, &[]);
        }
    }

    pub fn sync_current(&self, cursor: &Option<Cursor>) {
        if let Some(cursor) = cursor {
            let cx = apibara_observability::Context::current();
            self.sync_current.observe(&cx, cursor.order_key, &[]);
        }
    }

    pub fn sync_head(&self, cursor: &Option<Cursor>) {
        if let Some(cursor) = cursor {
            let cx = apibara_observability::Context::current();
            self.sync_head.observe(&cx, cursor.order_key, &[]);
        }
    }
}
