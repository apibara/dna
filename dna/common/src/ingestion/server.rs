use std::{net::SocketAddr, sync::Arc};

use apibara_dna_protocol::ingestion::{
    self, ingestion_file_descriptor_set, ingestion_server, SubscribeRequest, SubscribeResponse,
};
use error_stack::ResultExt;
use futures_util::{Stream, StreamExt, TryFutureExt};
use tokio::{
    pin,
    sync::{broadcast, mpsc, Mutex},
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server as TonicServer, Request};
use tracing::{error, info};

use crate::error::{DnaError, Result};

use super::{Snapshot, SnapshotChange};

type TonicResult<T> = std::result::Result<T, tonic::Status>;

pub struct IngestionServer<S>
where
    S: Stream<Item = SnapshotChange> + Send + Sync + 'static,
{
    ingestion_stream: S,
}

impl<S> IngestionServer<S>
where
    S: Stream<Item = SnapshotChange> + Send + Sync + 'static,
{
    pub fn new(ingestion_stream: S) -> Self {
        IngestionServer { ingestion_stream }
    }

    pub async fn start(self, addr: SocketAddr, ct: CancellationToken) -> Result<()> {
        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(ingestion_file_descriptor_set())
            .build()
            .change_context(DnaError::Fatal)
            .attach_printable("failed to create gRPC reflection service")?;

        let (tx, _) = broadcast::channel(128);
        let shared_state = Arc::new(Mutex::new(IngestionState { tx, snapshot: None }));

        let update_task = tokio::spawn(
            update_shared_state(shared_state.clone(), self.ingestion_stream, ct.clone())
                .inspect_err(|err| {
                    error!(%err, "ingestion stream error");
                }),
        );

        let ingestion_service = Service::new(shared_state.clone()).into_service();

        info!(addr = %addr, "starting ingestion server");

        let server_task = TonicServer::builder()
            .add_service(reflection_service)
            .add_service(ingestion_service)
            .serve_with_shutdown(addr, {
                let ct = ct.clone();
                async move {
                    ct.cancelled().await;
                }
            });

        tokio::select! {
            Err(err) = update_task => {
                error!("ingestion stream error");
                Err(err).change_context(DnaError::Fatal).attach_printable("ingestion stream error")
            }
            Err(err) = server_task => {
                Err(err).change_context(DnaError::Fatal).attach_printable("ingestion gRPC server error")
            }
            else => {
                Ok(())
            }
        }
    }
}

struct IngestionState {
    tx: broadcast::Sender<TonicResult<SubscribeResponse>>,
    snapshot: Option<Snapshot>,
}

type SharedIngestionState = Arc<Mutex<IngestionState>>;

struct Service {
    state: SharedIngestionState,
}

impl Service {
    pub fn new(state: SharedIngestionState) -> Self {
        Service { state }
    }

    pub fn into_service(self) -> ingestion_server::IngestionServer<Self> {
        ingestion_server::IngestionServer::new(self)
    }
}

#[tonic::async_trait]
impl ingestion_server::Ingestion for Service {
    type SubscribeStream = ReceiverStream<TonicResult<SubscribeResponse>>;

    async fn subscribe(
        &self,
        _request: Request<SubscribeRequest>,
    ) -> TonicResult<tonic::Response<Self::SubscribeStream>> {
        // acquire lock on subscribers to avoid sending messages to them while adding a new one.
        let state = self.state.lock().await;
        // Send initialize message to the new subscriber.
        // Do it here to ensure that the subscriber receives the message before any other.
        let Some(snapshot) = &state.snapshot else {
            return Err(tonic::Status::failed_precondition("ingestion not started"));
        };

        let message = snapshot.to_response();
        let (tx, rx) = mpsc::channel(128);
        tokio::spawn({
            let mut event_rx = state.tx.subscribe();
            async move {
                // Send initial snapshot to the new subscriber.
                tx.send(Ok(message))
                    .await
                    .map_err(|_| tonic::Status::internal("failed to send initial snapshot"))?;

                // Consume ingestion stream and forward it to subscriber.
                loop {
                    match event_rx.recv().await {
                        Ok(message) => {
                            tx.send(message).await.map_err(|_| {
                                tonic::Status::internal("failed to send message to subscriber")
                            })?;
                        }
                        Err(_) => {
                            tx.send(Err(tonic::Status::data_loss(
                                "ingestion stream lagged behind",
                            )))
                            .await
                            .map_err(|_| {
                                tonic::Status::internal("failed to send message to subscriber")
                            })?;
                            break;
                        }
                    }
                }

                Ok::<_, tonic::Status>(())
            }
        });

        let response = ReceiverStream::new(rx);
        Ok(tonic::Response::new(response))
    }
}

async fn update_shared_state<S>(
    state: SharedIngestionState,
    ingestion_stream: S,
    ct: CancellationToken,
) -> Result<()>
where
    S: Stream<Item = SnapshotChange> + Send + Sync + 'static,
{
    let ingestion_stream = ingestion_stream.take_until(ct.cancelled()).fuse();
    pin!(ingestion_stream);

    while let Some(event) = ingestion_stream.next().await {
        match event {
            SnapshotChange::Started(snapshot) => {
                let mut state = state.lock().await;
                if state.snapshot.is_some() {
                    return Err(DnaError::Fatal)
                        .attach_printable("received started event but ingestion already started");
                }
                state.snapshot = Some(snapshot);
            }
            SnapshotChange::GroupSealed(group) => {
                let mut state = state.lock().await;
                let Some(snapshot) = &mut state.snapshot else {
                    return Err(DnaError::Fatal)
                        .attach_printable("received group sealed event but ingestion not started");
                };
                snapshot.revision = group.revision;
                // snapshot.group_count += 1;
                todo!();

                let group_name = snapshot
                    .segment_options
                    .format_segment_group_name(group.first_block_number);

                let message = {
                    use ingestion::subscribe_response::Message;
                    let message = ingestion::SealGroup {
                        group_name,
                        first_block_number: group.first_block_number,
                    };
                    ingestion::SubscribeResponse {
                        message: Some(Message::SealGroup(message)),
                    }
                };

                if state.tx.receiver_count() > 0 {
                    state
                        .tx
                        .send(Ok(message))
                        .map_err(|_| DnaError::Fatal)
                        .attach_printable("failed to send group sealed event to subscriber")?;
                }
            }
            SnapshotChange::SegmentAdded(segment) => {
                let mut state = state.lock().await;
                let Some(snapshot) = &mut state.snapshot else {
                    return Err(DnaError::Fatal).attach_printable(
                        "received segment added event but ingestion not started",
                    );
                };

                let segment_name = snapshot
                    .segment_options
                    .format_segment_name(segment.first_block_number);

                let message = {
                    use ingestion::subscribe_response::Message;
                    let message = ingestion::AddSegmentToGroup {
                        segment_name,
                        first_block_number: segment.first_block_number,
                    };
                    ingestion::SubscribeResponse {
                        message: Some(Message::AddSegmentToGroup(message)),
                    }
                };

                if state.tx.receiver_count() > 0 {
                    state
                        .tx
                        .send(Ok(message))
                        .map_err(|_| DnaError::Fatal)
                        .attach_printable("failed to segment event to subscriber")?;
                }
            }
        }
    }

    Ok(())
}
