use bytes::Bytes;
use error_stack::{Result, ResultExt};
use futures_util::Stream;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Endpoint;

use crate::{ingestion::SnapshotChange, storage::LocalStorageBackend};

/// A service that keeps the ingestion state in sync with the ingestion server.
pub struct IngestionStateSyncServer {
    ingestion_endpoint: Endpoint,
    storage: LocalStorageBackend,
}

#[derive(Debug)]
pub struct IngestionStateSyncError;

impl IngestionStateSyncServer {
    pub fn new(
        ingestion_server: impl Into<Bytes>,
        storage: LocalStorageBackend,
    ) -> Result<Self, IngestionStateSyncError> {
        let ingestion_endpoint = Endpoint::from_shared(ingestion_server)
            .change_context(IngestionStateSyncError)
            .attach_printable("failed to create ingestion server endpoint")
            .attach_printable("hint: is the ingestion server address correct?")?;

        Ok(Self {
            ingestion_endpoint,
            storage,
        })
    }

    pub fn start(
        self,
        ct: CancellationToken,
    ) -> (
        impl Stream<Item = SnapshotChange>,
        JoinHandle<Result<(), IngestionStateSyncError>>,
    ) {
        let (tx, rx) = mpsc::channel(1024);

        let worker = worker::Worker {
            tx,
            endpoint: self.ingestion_endpoint,
            storage: self.storage,
        };

        let handle = tokio::spawn(worker.sync_loop(ct));

        (ReceiverStream::new(rx), handle)
    }
}

impl error_stack::Context for IngestionStateSyncError {}

impl std::fmt::Display for IngestionStateSyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ingestion state sync error")
    }
}

mod worker {
    use std::time::Duration;

    use apibara_dna_protocol::dna::ingestion::{
        ingestion_client::IngestionClient, SubscribeRequest,
    };
    use error_stack::{Result, ResultExt};
    use futures_util::{StreamExt, TryStreamExt};
    use tokio::{io::AsyncWriteExt, sync::mpsc};
    use tokio_util::sync::CancellationToken;
    use tonic::transport::{Channel, Endpoint};
    use tracing::{debug, info, warn};

    use crate::{
        core::Cursor,
        ingestion::{IngestionState, Snapshot, SnapshotChange},
        storage::{block_prefix, LocalStorageBackend, StorageBackend},
    };

    use super::IngestionStateSyncError;

    pub struct Worker {
        pub tx: mpsc::Sender<SnapshotChange>,
        pub endpoint: Endpoint,
        pub storage: LocalStorageBackend,
    }

    /// Worker error type.
    ///
    /// The worker needs to automatically reconnect to the ingestion server
    /// on disconnects or other temporary errors.
    #[derive(Debug, Clone)]
    pub enum WorkerError {
        Temporary,
        Fatal,
    }

    impl Worker {
        pub async fn sync_loop(
            mut self,
            ct: CancellationToken,
        ) -> Result<(), IngestionStateSyncError> {
            loop {
                match self.do_sync_loop(ct.clone()).await {
                    Ok(()) => break,
                    Err(err) => match err.current_context() {
                        WorkerError::Temporary => {
                            warn!(error = ?err, "snapshot sync worker temporary error, retrying in 5 seconds");
                            tokio::time::sleep(Duration::from_secs(5)).await;
                        }
                        WorkerError::Fatal => {
                            return Err(err).change_context(IngestionStateSyncError);
                        }
                    },
                }
            }

            Ok(())
        }

        async fn do_sync_loop(&mut self, ct: CancellationToken) -> Result<(), WorkerError> {
            let mut client = self.connect().await?;

            let response = client
                .subscribe(SubscribeRequest::default())
                .await
                .change_context(WorkerError::Temporary)
                .attach_printable("failed to start snapshot sync stream")?
                .into_inner()
                .take_until(ct.cancelled());

            let mut response = Box::pin(response);

            let Some(snapshot) = response
                .try_next()
                .await
                .change_context(WorkerError::Temporary)
                .attach_printable("failed to read ingestion stream first message")?
                .and_then(|m| m.as_snapshot().cloned())
                .and_then(|s| Snapshot::from_proto(&s))
            else {
                return Err(WorkerError::Fatal)
                    .attach_printable("first stream message is not a valid snapshot");
            };

            info!(?snapshot, "snapshot sync worker received snapshot");

            let Ok(_) = self.tx.send(SnapshotChange::Started { snapshot }).await else {
                todo!();
            };

            // Don't delete blocks right after they become invalid since they may still
            // be in use by clients. Remove them after a couple more messages.
            let mut prev_removed_cursors: Vec<Cursor> = Vec::new();

            while let Some(message) = response
                .try_next()
                .await
                .change_context(WorkerError::Temporary)?
            {
                use apibara_dna_protocol::dna::ingestion::subscribe_response::Message;

                match message.message {
                    Some(Message::Snapshot(_snapshot)) => {
                        return Err(WorkerError::Temporary).attach_printable(
                            "Received a new snapshot message. This should never happen.",
                        )
                    }
                    Some(Message::StateChanged(state_changed)) => {
                        let finalized: Cursor = state_changed.finalized.unwrap_or_default().into();
                        let new_state: IngestionState =
                            state_changed.new_state.unwrap_or_default().into();
                        let removed_cursors: Vec<Cursor> = state_changed
                            .removed_cursors
                            .into_iter()
                            .map(Into::into)
                            .collect();
                        debug!(?finalized, ?new_state, removed_cursors = %removed_cursors.len(), "state changed");

                        // Remove old cursors from storage.
                        for cursor in prev_removed_cursors.drain(..) {
                            let prefix = block_prefix(&cursor);
                            if self
                                .storage
                                .prefix_exists(&prefix)
                                .await
                                .change_context(WorkerError::Temporary)?
                            {
                                self.storage
                                    .remove_prefix(&prefix)
                                    .await
                                    .change_context(WorkerError::Temporary)
                                    .attach_printable("failed to remove block from storage")?;
                            }
                        }

                        prev_removed_cursors = removed_cursors;

                        let Ok(_) = self
                            .tx
                            .send(SnapshotChange::StateChanged {
                                new_state,
                                finalized,
                            })
                            .await
                        else {
                            todo!();
                        };
                    }
                    Some(Message::BlockIngested(block_ingested)) => {
                        let cursor: Cursor = block_ingested.cursor.unwrap_or_default().into();
                        let prefix = block_prefix(&cursor);
                        let mut writer = self
                            .storage
                            .put(&prefix, "block")
                            .await
                            .change_context(WorkerError::Temporary)
                            .attach_printable("failed to write block to storage")?;
                        writer
                            .write_all(&block_ingested.data)
                            .await
                            .change_context(WorkerError::Temporary)
                            .attach_printable("failed to write block to local storage")?;
                        writer
                            .shutdown()
                            .await
                            .change_context(WorkerError::Temporary)?;

                        debug!(
                            ?cursor,
                            prefix, "block ingested and written to local storage"
                        );

                        let Ok(_) = self.tx.send(SnapshotChange::BlockIngested { cursor }).await
                        else {
                            todo!();
                        };
                    }
                    Some(Message::Invalidate(_invalidate)) => {
                        debug!("invalidate");
                        // TODO: delete now useless blocks in storage.
                        // Forward message to stream.
                    }
                    None => return Err(WorkerError::Temporary).attach_printable(
                        "Unknown message type in ingestion stream. Server/client version mismatch?",
                    ),
                };
            }

            Ok(())
        }

        async fn connect(&self) -> Result<IngestionClient<Channel>, WorkerError> {
            let client = IngestionClient::connect(self.endpoint.clone())
                .await
                .change_context(WorkerError::Temporary)
                .attach_printable("failed to connect to ingestion server")?;
            Ok(client)
        }
    }

    impl error_stack::Context for WorkerError {}

    impl std::fmt::Display for WorkerError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                WorkerError::Temporary => write!(f, "temporary worker error"),
                WorkerError::Fatal => write!(f, "fatal worker error"),
            }
        }
    }
}
