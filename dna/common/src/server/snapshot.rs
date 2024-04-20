use std::sync::Arc;

use apibara_dna_protocol::ingestion;
use bytes::Bytes;
use error_stack::ResultExt;
use tokio::sync::{broadcast, Mutex};
use tokio_util::sync::CancellationToken;
use tonic::transport::Endpoint;

use crate::error::{DnaError, Result};

/// Snapshot state with all important blocks for indexing.
///
/// - `first_block_number`: first available block. Usually this
/// is 0, but it can be different if it's only a partial snapshot.
/// - `sealed_block_number`: last block number (non-inclusive) that is
/// part of a sealed segment group. Can use indexes to quickly scan
/// through blocks.
/// - `segmented_block_number`: last block number (non-inclusive) that is
/// part of a segment. Since it's not part of a sealed group, data
/// must be scanned block by block.
/// - `finalized_block_number`: chain's finalized block. Blocks before
/// this one cannot be part of a reorg.
/// - `accepted_block_number`: chain's head. Blocks between finalized and
/// this one can be reorged.
///
/// ```ascii
/// |----------+---------+----------+----------|
/// ^          ^         ^          ^          ^
/// first      sealed    segmented  finalized  accepted
/// ```
#[derive(Debug, Clone)]
pub struct SnapshotState {
    pub first_block_number: u64,
    pub sealed_block_number: u64,
    pub segmented_block_number: u64,
    // pub finalized_block_number: u32,
    // pub accepted_block_number: u32,
}

/// A client to subscribe to snapshot changes.
pub struct SnapshotSyncClient {
    inner: Arc<Mutex<Option<SnapshotSyncClientInner>>>,
}

struct SnapshotSyncClientInner {
    state: SnapshotState,
    sender: broadcast::Sender<SnapshotState>,
}

/// A service to keep the snapshot state synchronized with the
/// ingestion server.
///
/// This service:
/// - Tracks the snapshot state and makes it available to other
/// services of this server.
/// - Writes "immediate data blocks" to local storage.
pub struct SnapshotSyncService {
    ingestion_endpoint: Endpoint,
}

impl SnapshotSyncService {
    pub fn new(ingestion_server: impl Into<Bytes>) -> Result<Self> {
        let ingestion_endpoint = Endpoint::from_shared(ingestion_server)
            .change_context(DnaError::Configuration)
            .attach_printable("failed to create ingestion server endpoint")
            .attach_printable("hint: is the ingestion server address correct?")?;

        Ok(Self { ingestion_endpoint })
    }

    pub fn start(self, ct: CancellationToken) -> SnapshotSyncClient {
        let shared_state = Arc::new(Mutex::new(None));

        let worker = worker::Worker {
            endpoint: self.ingestion_endpoint,
            shared_state: shared_state.clone(),
        };

        tokio::spawn(worker.start(ct));

        SnapshotSyncClient {
            inner: shared_state,
        }
    }
}

impl SnapshotSyncClient {
    /// Subscribe to snapshot changes.
    pub async fn subscribe(&self) -> Option<(SnapshotState, broadcast::Receiver<SnapshotState>)> {
        let guard = self.inner.lock().await;

        let Some(inner) = guard.as_ref() else {
            return None;
        };

        let initial = inner.state.clone();
        let receiver = inner.sender.subscribe();
        Some((initial, receiver))
    }
}

impl SnapshotState {
    pub fn from_proto(message: ingestion::Snapshot) -> Self {
        /*
        let blocks_in_segment_group = (message.segment_size * message.group_size) as u64;

        // Last (non-inclusive) block number of the sealed segment group.
        // If group count is 0, then sealed_block_number == first_block number
        // and the indexer won't advance until data is ingested.
        let sealed_block_number =
            message.first_block_number + (blocks_in_segment_group * message.group_count as u64);

        Self {
            first_block_number: message.first_block_number,
            sealed_block_number,
            segmented_block_number: sealed_block_number,
        }
        */
        todo!();
    }
}

mod worker {
    use std::{sync::Arc, time::Duration};

    use apibara_dna_protocol::ingestion::{ingestion_client::IngestionClient, SubscribeRequest};
    use error_stack::{Result, ResultExt};
    use futures_util::{StreamExt, TryStreamExt};
    use tokio::sync::Mutex;
    use tokio_util::sync::CancellationToken;
    use tonic::transport::{Channel, Endpoint};
    use tracing::{error, warn};

    use super::{SnapshotState, SnapshotSyncClientInner};

    /// Worker error type.
    ///
    /// The worker needs to automatically reconnect to the ingestion server
    /// on disconnects or other temporary errors.
    #[derive(Debug, Clone)]
    enum WorkerError {
        Temporary,
        Fatal,
    }

    pub struct Worker {
        pub endpoint: Endpoint,
        pub shared_state: Arc<Mutex<Option<SnapshotSyncClientInner>>>,
    }

    impl Worker {
        pub async fn start(self, ct: CancellationToken) {
            loop {
                match self.inner_loop(ct.clone()).await {
                    Ok(()) => break,
                    Err(err) => match err.current_context() {
                        WorkerError::Temporary => {
                            warn!(error = ?err, "snapshot sync worker temporary error, retrying in 5 seconds");
                            tokio::time::sleep(Duration::from_secs(5)).await;
                        }
                        WorkerError::Fatal => {
                            error!(error = ?err, "snapshot sync worker error");
                            break;
                        }
                    },
                }
            }
        }

        async fn inner_loop(&self, ct: CancellationToken) -> Result<(), WorkerError> {
            let mut client = self.connect().await?;
            let response = client
                .subscribe(SubscribeRequest::default())
                .await
                .change_context(WorkerError::Temporary)
                .attach_printable("failed to start streaming ingestion changes")?
                .into_inner()
                .take_until(ct.cancelled());

            let mut response = Box::pin(response);

            let Some(snapshot) = response
                .try_next()
                .await
                .change_context(WorkerError::Temporary)
                .attach_printable("failed to read ingestion stream first message")?
                .and_then(|m| m.as_snapshot().cloned())
            else {
                return Err(WorkerError::Fatal)
                    .attach_printable("first stream message is not a snapshot");
            };

            let mut state = SnapshotState::from_proto(snapshot.clone());
            {
                let (sender, _) = tokio::sync::broadcast::channel(521);
                let new_inner_state = SnapshotSyncClientInner {
                    state: state.clone(),
                    sender,
                };
                self.shared_state.lock().await.replace(new_inner_state);
            }

            while let Some(message) = response
                .try_next()
                .await
                .change_context(WorkerError::Temporary)?
            {
                use apibara_dna_protocol::ingestion::subscribe_response::Message;

                // Use message to update state
                /*
                match message.message {
                    Some(Message::Snapshot(_snapshot)) => {
                        return Err(WorkerError::Temporary).attach_printable(
                            "Received a new snapshot message. This should never happen.",
                        )
                    }
                    Some(Message::AddSegmentToGroup(add_segment)) => {
                        let new_segmented_block_number =
                            add_segment.first_block_number + snapshot.segment_size as u64;
                        assert!(new_segmented_block_number > state.segmented_block_number);
                        state.segmented_block_number = new_segmented_block_number;
                    }
                    Some(Message::SealGroup(seal_group)) => {
                        state.sealed_block_number = seal_group.first_block_number
                            + (snapshot.group_size * snapshot.segment_size) as u64;
                        state.segmented_block_number =
                            u64::max(state.segmented_block_number, state.sealed_block_number);
                    }
                    Some(Message::AddImmediateData(_add_immediate)) => {
                        todo!();
                    }
                    Some(Message::Invalidate(_invalidate)) => {
                        todo!();
                    }
                    None => return Err(WorkerError::Temporary).attach_printable(
                        "Unknown message type in ingestion stream. Server/client version mismatch?",
                    ),
                };

                // Forward new snapshot
                if let Some(shared_state) = self.shared_state.lock().await.as_mut() {
                    shared_state.state = state.clone();
                    // Ignore error since there may be no receivers.
                    let _ = shared_state.sender.send(state.clone());
                } else {
                    return Err(WorkerError::Fatal).attach_printable(
                        "snapshot sync client state is missing. This should never happen.",
                    );
                };
                */
                todo!();
            }

            if ct.is_cancelled() {
                Ok(())
            } else {
                Err(WorkerError::Temporary)
                    .attach_printable("ingestion stream ended. This should never happen?")
            }
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
