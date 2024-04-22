use std::{net::SocketAddr, sync::Arc};

use apibara_dna_protocol::dna::ingestion::{
    ingestion_file_descriptor_set, ingestion_server, subscribe_response, BlockIngested,
    StateChanged, SubscribeRequest, SubscribeResponse,
};
use error_stack::ResultExt;
use futures_util::{Stream, StreamExt, TryFutureExt};
use tokio::{
    io::AsyncReadExt,
    pin,
    sync::{broadcast, mpsc, Mutex, MutexGuard},
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server as TonicServer, Request};
use tracing::{debug, error, info};

use crate::{
    core::Cursor,
    error::{DnaError, Result},
    storage::{LocalStorageBackend, StorageBackend},
};

use super::{Snapshot, SnapshotChange};

type TonicResult<T> = std::result::Result<T, tonic::Status>;

pub struct IngestionServer<S>
where
    S: Stream<Item = SnapshotChange> + Send + Sync + 'static,
{
    ingestion_stream: S,
    storage: LocalStorageBackend,
}

impl<S> IngestionServer<S>
where
    S: Stream<Item = SnapshotChange> + Send + Sync + 'static,
{
    pub fn new(storage: LocalStorageBackend, ingestion_stream: S) -> Self {
        IngestionServer {
            storage,
            ingestion_stream,
        }
    }

    pub async fn start(self, addr: SocketAddr, ct: CancellationToken) -> Result<()> {
        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(ingestion_file_descriptor_set())
            .build()
            .change_context(DnaError::Fatal)
            .attach_printable("failed to create gRPC reflection service")?;

        let shared_state = SharedIngestionServerState::new(self.storage);

        let update_task = tokio::spawn(
            shared_state
                .clone()
                .update_from_stream(self.ingestion_stream, ct.clone())
                .inspect_err(|err| {
                    error!(%err, "ingestion stream error");
                }),
        );

        let ingestion_service = Service::new(shared_state).into_service();

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

struct IngestionServerState {
    tx: broadcast::Sender<subscribe_response::Message>,
    storage: LocalStorageBackend,
    snapshot: Option<Snapshot>,
    cursors: Vec<Cursor>,
}

#[derive(Clone)]
struct SharedIngestionServerState(Arc<Mutex<IngestionServerState>>);

struct Service {
    state: SharedIngestionServerState,
}

impl Service {
    pub fn new(state: SharedIngestionServerState) -> Self {
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

        let mut blocks = Vec::new();
        for cursor in state.cursors.iter() {
            let prefix = format!("blocks/{}-{}", cursor.number, cursor.hash_as_hex());
            let mut reader = state
                .storage
                .clone()
                .get(prefix, "block")
                .await
                .map_err(|_| tonic::Status::internal("failed to read block from storage"))?;
            let mut data = Vec::new();
            reader
                .read_to_end(&mut data)
                .await
                .map_err(|_| tonic::Status::internal("failed to read block from storage"))?;
            debug!(cursor = ?cursor, data = data.len(), "sending block to subscriber");
            let ingested = BlockIngested {
                cursor: Some(cursor.clone().into()),
                data,
            };
            blocks.push(subscribe_response::Message::BlockIngested(ingested));
        }

        let (tx, rx) = mpsc::channel(128);

        tokio::spawn({
            let mut event_rx = state.tx.subscribe();
            async move {
                // Send initial snapshot to the new subscriber.
                tx.send(Ok(message))
                    .await
                    .map_err(|_| tonic::Status::internal("failed to send initial snapshot"))?;

                for block in blocks {
                    let message = SubscribeResponse {
                        message: Some(block),
                    };

                    tx.send(Ok(message))
                        .await
                        .map_err(|_| tonic::Status::internal("failed to send initial blocks"))?;
                }

                // Consume ingestion stream and forward it to subscriber.
                loop {
                    match event_rx.recv().await {
                        Ok(message) => {
                            let response = SubscribeResponse {
                                message: Some(message),
                            };

                            tx.send(Ok(response)).await.map_err(|_| {
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

impl SharedIngestionServerState {
    pub fn new(storage: LocalStorageBackend) -> Self {
        let (tx, _) = broadcast::channel(128);
        let state = IngestionServerState {
            storage,
            tx,
            snapshot: None,
            cursors: Vec::new(),
        };
        SharedIngestionServerState(Arc::new(Mutex::new(state)))
    }

    async fn lock(&self) -> MutexGuard<'_, IngestionServerState> {
        self.0.lock().await
    }

    async fn update_from_stream<S>(self, ingestion_stream: S, ct: CancellationToken) -> Result<()>
    where
        S: Stream<Item = SnapshotChange> + Send + Sync + 'static,
    {
        let ingestion_stream = ingestion_stream.take_until(ct.cancelled()).fuse();
        pin!(ingestion_stream);

        let Some(event) = ingestion_stream.next().await else {
            return Ok(());
        };

        match event {
            SnapshotChange::Started(snapshot) => {
                let mut state = self.lock().await;
                state.snapshot = Some(snapshot);
            }
            _ => {
                return Err(DnaError::Fatal)
                    .attach_printable("expected first event to be SnapshotChange::Started");
            }
        }

        while let Some(event) = ingestion_stream.next().await {
            match event {
                SnapshotChange::StateChanged {
                    new_state,
                    finalized,
                } => {
                    let mut state = self.lock().await;

                    let (new_state_proto, first_non_segmented_block_number) = {
                        let snapshot = state
                            .snapshot
                            .as_mut()
                            .ok_or(DnaError::Fatal)
                            .attach_printable("received state update message with no snapshot")?;

                        let new_state_proto = new_state.to_proto();
                        snapshot.ingestion = new_state;
                        (new_state_proto, snapshot.starting_block_number())
                    };

                    let cursors = std::mem::take(&mut state.cursors);

                    let (removed_cursors, cursors) = cursors
                        .into_iter()
                        .partition(|cursor| cursor.number < first_non_segmented_block_number);

                    state.cursors = cursors;
                    let removed_cursors = removed_cursors.into_iter().map(Into::into).collect();

                    let state_changed = StateChanged {
                        new_state: Some(new_state_proto),
                        removed_cursors,
                        finalized: Some(finalized.into()),
                    };
                    state.send(subscribe_response::Message::StateChanged(state_changed));
                }
                SnapshotChange::BlockIngested(block) => {
                    let cursor = block.cursor;
                    let mut state = self.lock().await;
                    state.cursors.push(cursor.clone());

                    if state.tx.receiver_count() == 0 {
                        continue;
                    }

                    let prefix = format!("blocks/{}-{}", cursor.number, cursor.hash_as_hex());
                    let mut reader = state.storage.clone().get(&prefix, "block").await?;
                    let mut data = Vec::new();
                    reader
                        .read_to_end(&mut data)
                        .await
                        .change_context(DnaError::Fatal)
                        .attach_printable("failed to read block from storage")?;
                    debug!(prefix, data = data.len(), "sending block to subscriber");
                    let ingested = BlockIngested {
                        cursor: Some(cursor.clone().into()),
                        data,
                    };

                    state.send(subscribe_response::Message::BlockIngested(ingested));
                }
                _ => {
                    return Err(DnaError::Fatal)
                        .attach_printable("unexpected event in ingestion stream")
                        .attach_printable_lazy(|| format!("event: {:?}", event));
                }
            }
        }

        Ok(())
    }
}

impl IngestionServerState {
    fn send(&self, value: subscribe_response::Message) {
        let _ = self.tx.send(value);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use apibara_dna_protocol::dna::ingestion::subscribe_response::Message;
    use tokio::{sync::mpsc, time::timeout};
    use tokio_util::sync::CancellationToken;

    use crate::{
        core::Cursor,
        ingestion::{IngestedBlock, IngestionState, Snapshot, SnapshotChange},
        segment::SegmentOptions,
        storage::LocalStorageBackend,
    };

    use super::SharedIngestionServerState;

    const DEFAULT_TIMEOUT: Duration = Duration::from_secs(1);

    #[tokio::test]
    pub async fn test_update_shared_ingestion_state() {
        let ct = CancellationToken::new();
        let storage = LocalStorageBackend::new("/tmp/dna-test-update-shared-ingestion-state");
        let (ingestion_tx, ingestion_rx) = mpsc::channel(128);
        let ingestion_stream = tokio_stream::wrappers::ReceiverStream::new(ingestion_rx);
        let shared_state = SharedIngestionServerState::new(storage);

        let task_handle = tokio::spawn(
            shared_state
                .clone()
                .update_from_stream(ingestion_stream, ct.clone()),
        );

        // Snapshot starts empty.
        {
            let state = shared_state.lock().await;
            assert!(state.snapshot.is_none());
        }

        ingestion_tx
            .send(SnapshotChange::Started(Snapshot {
                revision: 10,
                segment_options: SegmentOptions {
                    group_size: 10,
                    segment_size: 100,
                },
                ingestion: IngestionState {
                    first_block_number: 1_000_000,
                    group_count: 10,
                    extra_segment_count: 4,
                },
            }))
            .await
            .unwrap();

        // Wait for process to receive message and update its snapshot.
        {
            for i in 0.. {
                assert!(i < 10, "timeout waiting for snapshot to be set");
                let state = shared_state.lock().await;
                if state.snapshot.is_some() {
                    break;
                }
                tokio::time::sleep(DEFAULT_TIMEOUT).await;
            }
        }

        let mut update_rx = shared_state.lock().await.tx.subscribe();

        // Check that state changes are sent to subscribers.
        {
            ingestion_tx
                .send(SnapshotChange::StateChanged {
                    new_state: IngestionState {
                        first_block_number: 1_000_000,
                        group_count: 10,
                        extra_segment_count: 8,
                    },
                    finalized: Cursor::new(1_010_800, vec![]),
                })
                .await
                .unwrap();

            let message = timeout(DEFAULT_TIMEOUT, update_rx.recv())
                .await
                .expect("timeout")
                .unwrap();
            let state_changed = if let Message::StateChanged(state_changed) = message {
                state_changed
            } else {
                panic!("expected StateChanged message, got: {:?}", message);
            };

            let new_state = state_changed.new_state.unwrap();
            assert_eq!(new_state.first_block_number, 1_000_000);
            assert_eq!(new_state.group_count, 10);
            assert_eq!(new_state.extra_segment_count, 8);
            assert!(state_changed.removed_cursors.is_empty());
        }

        // Check snapshot state.
        {
            let state = shared_state.lock().await;
            let snapshot = state.snapshot.clone().unwrap();
            assert_eq!(snapshot.starting_block_number(), 1_010_800);
        }

        // Send some ingested blocks between 1_010_850 and 1_010_950
        {
            for i in 1_010_800..1_010_950 {
                let cursor = Cursor::new(i, i.to_be_bytes().to_vec());
                ingestion_tx
                    .send(SnapshotChange::BlockIngested(IngestedBlock { cursor }))
                    .await
                    .unwrap();
            }

            for i in 1_010_800..1_010_950 {
                let message = timeout(DEFAULT_TIMEOUT, update_rx.recv())
                    .await
                    .expect("timeout")
                    .unwrap();

                let ingested = if let Message::BlockIngested(ingested) = message {
                    ingested
                } else {
                    panic!("expected BlockIngested message, got: {:?}", message);
                };

                assert_eq!(ingested.cursor.unwrap().order_key, i);
            }
        }

        // After that, send a state changed to signal a segment has been ingested.
        // This usually happens one the blocks have been ingested, but since they
        // are not finalized they cannot be segmented.
        {
            ingestion_tx
                .send(SnapshotChange::StateChanged {
                    new_state: IngestionState {
                        first_block_number: 1_000_000,
                        group_count: 10,
                        extra_segment_count: 9,
                    },
                    finalized: Cursor::new(1_010_800, vec![]),
                })
                .await
                .unwrap();

            let message = timeout(DEFAULT_TIMEOUT, update_rx.recv())
                .await
                .expect("timeout")
                .unwrap();
            let state_changed = if let Message::StateChanged(state_changed) = message {
                state_changed
            } else {
                panic!("expected StateChanged message, got: {:?}", message);
            };

            let new_state = state_changed.new_state.unwrap();
            assert_eq!(new_state.first_block_number, 1_000_000);
            assert_eq!(new_state.group_count, 10);
            assert_eq!(new_state.extra_segment_count, 9);

            // Same as segment length.
            assert_eq!(state_changed.removed_cursors.len(), 100);
            for (cursor, expected_number) in state_changed.removed_cursors.iter().zip(1_010_800..) {
                assert_eq!(cursor.order_key, expected_number);
            }
        }

        ct.cancel();

        task_handle.await.unwrap().unwrap();
    }
}
