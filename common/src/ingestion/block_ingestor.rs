use std::{collections::VecDeque, marker::PhantomData};

use error_stack::{Result, ResultExt};
use futures_util::{stream::FuturesOrdered, Stream, StreamExt};
use tokio::{io::AsyncWriteExt, sync::mpsc, task::JoinHandle};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::{
    core::Cursor,
    ingestion::ChainChangeV2_ChangeMe_Before_Release,
    storage::{block_prefix, LocalStorageBackend, StorageBackend, BLOCK_NAME},
};

#[async_trait::async_trait]
pub trait SingleBlockIngestion<T: rkyv::Archive> {
    async fn ingest_block(&self, cursor: Cursor) -> Result<T, BlockIngestorError>;
}

#[derive(Debug)]
pub struct BlockIngestorOptions {
    pub channel_size: usize,
    pub concurrency: usize,
}

#[derive(Debug, Clone)]
pub enum BlockIngestionEvent {
    /// First message in the stream, with the starting state.
    Initialize { head: Cursor, finalized: Cursor },
    /// A new head has been detected.
    NewHead(Cursor),
    /// A new finalized block has been detected.
    NewFinalized(Cursor),
    /// Ingest the provided block.
    Ingested {
        cursor: Cursor,
        prefix: String,
        filename: String,
    },
    /// The chain reorganized.
    Invalidate {
        new_head: Cursor,
        removed: Vec<Cursor>,
    },
}

pub struct BlockIngestor<T, BI, CS>
where
    T: rkyv::Archive,
    BI: SingleBlockIngestion<T>,
    CS: Stream<Item = ChainChangeV2_ChangeMe_Before_Release> + Unpin + Send + Sync + 'static,
{
    storage: LocalStorageBackend,
    ingestion: BI,
    chain_changes: CS,
    options: BlockIngestorOptions,
    message_queue: VecDeque<BlockIngestionEvent>,
    _block_phantom: PhantomData<T>,
}

#[derive(Debug)]
pub enum BlockIngestorError {
    InvalidChainChange,
    BlockIngestion,
    StreamClosed,
    Storage,
}

impl<T, BI, CS> BlockIngestor<T, BI, CS>
where
    T: rkyv::Archive + Send + 'static + rkyv::Serialize<rkyv::ser::serializers::AllocSerializer<0>>,
    BI: SingleBlockIngestion<T> + Send + Sync + 'static,
    CS: Stream<Item = ChainChangeV2_ChangeMe_Before_Release> + Unpin + Send + Sync + 'static,
{
    pub fn new(
        storage: LocalStorageBackend,
        ingestion: BI,
        chain_changes: CS,
        options: BlockIngestorOptions,
    ) -> Self {
        Self {
            storage,
            ingestion,
            chain_changes,
            options,
            message_queue: VecDeque::new(),
            _block_phantom: PhantomData,
        }
    }

    pub fn start(
        self,
        ct: CancellationToken,
    ) -> (
        ReceiverStream<BlockIngestionEvent>,
        JoinHandle<Result<(), BlockIngestorError>>,
    ) {
        let (tx, rx) = mpsc::channel(self.options.channel_size);
        let handle = tokio::spawn(self.do_loop(tx, ct));
        (ReceiverStream::new(rx), handle)
    }

    async fn do_loop(
        mut self,
        tx: mpsc::Sender<BlockIngestionEvent>,
        ct: CancellationToken,
    ) -> Result<(), BlockIngestorError> {
        info!("starting block ingestor");

        let mut chain_changes = self.chain_changes;

        let (head, finalized) = {
            let Some(change) = chain_changes.next().await else {
                return Err(BlockIngestorError::InvalidChainChange)
                    .attach_printable("chain changes stream ended unexpectedly");
            };

            match change {
                ChainChangeV2_ChangeMe_Before_Release::Initialize { head, finalized } => {
                    (head, finalized)
                }
                _ => {
                    return Err(BlockIngestorError::InvalidChainChange)
                        .attach_printable("expected initialize message")
                        .attach_printable_lazy(|| format!("received: {:?}", change));
                }
            }
        };

        debug!(%finalized, %head, "block ingestor initilaized");

        let permit = tx
            .reserve()
            .await
            .change_context(BlockIngestorError::StreamClosed)?;
        permit.send(BlockIngestionEvent::Initialize {
            head: head.clone(),
            finalized: finalized.clone(),
        });

        let mut block_ingestion = FuturesOrdered::new();

        loop {
            tokio::select! {
                biased;
                _ = ct.cancelled() => break,

                Some(message) = chain_changes.next(), if block_ingestion.len() < self.options.concurrency && tx.capacity() > 0 => {
                    use ChainChangeV2_ChangeMe_Before_Release::*;

                    match message {
                        Ingest(cursor) => {
                            debug!(%cursor, "queueing block ingestion job");
                            let task = async {
                                let response = self.ingestion.ingest_block(cursor.clone()).await;
                                response.map(|block| (cursor, block))
                            };
                            block_ingestion.push_back(task);
                        }
                        NewHead(new_head) => {
                            self.message_queue.push_back(BlockIngestionEvent::NewHead(new_head));
                        }
                        NewFinalized(new_finalized) => {
                            self.message_queue.push_back(BlockIngestionEvent::NewFinalized(new_finalized));
                        }
                        Invalidate { .. } => {
                            // TODO: notice that because we are ingesting blocks async,
                            // we lose ordering of the messages. This is an issue because the stream
                            // will contain ingested blocks that are not in the chain anymore.
                            // We should create a checkpoint when we receive an invalidate message
                            // and send the invalidate downstream only after all messages have been
                            // delivered.
                            todo!();
                        }
                        _ => return Err(BlockIngestorError::InvalidChainChange)
                            .attach_printable_lazy(|| format!("unexpected message: {:?}", message)),
                    }
                }

                Some(block) = block_ingestion.next(), if !block_ingestion.is_empty() => {
                    let (cursor, block) = block?;
                    let prefix = block_prefix(&cursor);
                    let filename = BLOCK_NAME.to_string();
                    write_single_block(&mut self.storage, &prefix, &filename, block).await?;
                    self.message_queue.push_back(BlockIngestionEvent::Ingested {
                        cursor,
                        prefix,
                        filename
                    });
                }

                permit = tx.reserve(), if !self.message_queue.is_empty() => {
                    let permit = permit.change_context(BlockIngestorError::StreamClosed)?;
                    if let Some(message) = self.message_queue.pop_front() {
                        permit.send(message);
                    }
                }

                else => {
                    return Err(BlockIngestorError::BlockIngestion)
                        .attach_printable("block ingestion stream ended unexpectedly");
                }
            }
        }

        Ok(())
    }
}

impl error_stack::Context for BlockIngestorError {}

impl std::fmt::Display for BlockIngestorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlockIngestorError::InvalidChainChange => {
                write!(f, "Invalid chain change")
            }
            BlockIngestorError::BlockIngestion => {
                write!(f, "Block ingestion error")
            }
            BlockIngestorError::StreamClosed => {
                write!(f, "Output stream closed")
            }
            BlockIngestorError::Storage => {
                write!(f, "Storage error")
            }
        }
    }
}

impl Default for BlockIngestorOptions {
    fn default() -> Self {
        Self {
            channel_size: 1024,
            concurrency: 10,
        }
    }
}

async fn write_single_block<T>(
    storage: &mut LocalStorageBackend,
    prefix: impl AsRef<str>,
    filename: impl AsRef<str>,
    block: T,
) -> Result<(), BlockIngestorError>
where
    T: rkyv::Archive + rkyv::Serialize<rkyv::ser::serializers::AllocSerializer<0>>,
{
    let bytes = rkyv::to_bytes::<T, 0>(&block)
        .change_context(BlockIngestorError::Storage)
        .attach_printable("failed to serialize block")
        .attach_printable_lazy(|| format!("prefix: {}", prefix.as_ref()))?;

    let mut writer = storage
        .put(prefix.as_ref(), filename.as_ref())
        .await
        .change_context(BlockIngestorError::Storage)?;
    writer
        .write_all(&bytes)
        .await
        .change_context(BlockIngestorError::Storage)?;
    writer
        .shutdown()
        .await
        .change_context(BlockIngestorError::Storage)?;

    debug!(
        prefix = prefix.as_ref(),
        size = bytes.len(),
        "block ingested and stored"
    );
    Ok(())
}
