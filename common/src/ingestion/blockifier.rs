use error_stack::{Result, ResultExt};
use futures_util::{stream::FuturesOrdered, Stream, StreamExt, TryFutureExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::{
    core::Cursor,
    ingestion::{BlockEvent, ChainChange, Snapshot},
};

#[async_trait::async_trait]
pub trait SingleBlockIngestion {
    async fn ingest_block(&self, cursor: Cursor) -> Result<Cursor, BlockifierError>;
}

pub struct Blockifier<BI, CS>
where
    BI: SingleBlockIngestion,
    CS: Stream<Item = ChainChange> + Unpin + Send + Sync + 'static,
{
    ingestion: BI,
    chain_changes: CS,
}

#[derive(Debug)]
pub enum BlockifierError {
    Configuration,
    InvalidChainChange,
    BlockIngestion,
}

impl<BI, CS> Blockifier<BI, CS>
where
    BI: SingleBlockIngestion + Send + Sync + 'static,
    CS: Stream<Item = ChainChange> + Unpin + Send + Sync + 'static,
{
    pub fn new(ingestion: BI, chain_changes: CS) -> Self {
        Self {
            ingestion,
            chain_changes,
        }
    }

    pub fn start(
        self,
        starting_snapshot: Snapshot,
        ct: CancellationToken,
    ) -> impl Stream<Item = BlockEvent> {
        let (tx, rx) = mpsc::channel(1024);

        tokio::spawn(
            self.process_blocks(starting_snapshot, tx, ct)
                .inspect_err(|err| {
                    warn!(err = ?err, "blockifier process returned with error");
                }),
        );

        ReceiverStream::new(rx)
    }

    async fn process_blocks(
        self,
        starting_snapshot: Snapshot,
        tx: mpsc::Sender<BlockEvent>,
        ct: CancellationToken,
    ) -> Result<(), BlockifierError> {
        let mut chain_changes = self.chain_changes;

        let (mut head, finalized) = {
            let Some(change) = chain_changes.next().await else {
                return Err(BlockifierError::InvalidChainChange)
                    .attach_printable("chain changes stream ended unexpectedly");
            };

            match change {
                ChainChange::Initialize { head, finalized } => (head, finalized),
                _ => {
                    return Err(BlockifierError::InvalidChainChange)
                        .attach_printable("expected initialize message")
                        .attach_printable_lazy(|| format!("received: {:?}", change));
                }
            }
        };

        let starting_block_number = starting_snapshot.starting_block_number();

        if finalized.number < starting_block_number {
            return Err(BlockifierError::Configuration)
                .attach_printable("finalized block is behind the starting block")
                .attach_printable_lazy(|| {
                    format!(
                        "finalized: {}, starting: {}",
                        finalized, starting_block_number
                    )
                });
        }

        info!(starting_block_number, head = %head, finalized = %finalized, "starting blockifier");

        let Ok(_) = tx
            .send(BlockEvent::Started {
                finalized: finalized.clone(),
            })
            .await
        else {
            return Ok(());
        };

        let mut block_ingestion = FuturesOrdered::new();

        // TODO: this should come from configuration
        let max_concurrent_jobs = 10;

        let mut current_block_number = starting_block_number;

        while block_ingestion.len() < max_concurrent_jobs {
            debug!(current_block_number, "push block ingestion job");
            block_ingestion.push_back(
                self.ingestion
                    .ingest_block(Cursor::new_finalized(current_block_number)),
            );
            current_block_number += 1;
        }

        loop {
            tokio::select! {
                _ = ct.cancelled() => {
                    break;

                }
                ingested_cursor = block_ingestion.next(), if !block_ingestion.is_empty() => {
                    let Some(ingested_cursor) = ingested_cursor else {
                        return Err(BlockifierError::InvalidChainChange)
                            .attach_printable("block jobs ingestion stream ended unexpectedly");
                    };

                    // TODO: should have a retry policy of sort.
                    // Probably inside the SingleBlockIngestion trait is easier?
                    let ingested_cursor = ingested_cursor?;

                    let Ok(_) = tx.send(BlockEvent::Ingested(ingested_cursor)).await else {
                        todo!();
                    };

                    while block_ingestion.len() < max_concurrent_jobs {
                        if current_block_number > head.number {
                            break;
                        }
                        current_block_number += 1;
                        block_ingestion.push_back(
                            self.ingestion
                                .ingest_block(Cursor::new_finalized(current_block_number)),
                        );
                    }
                }
                maybe_change = chain_changes.next() => {
                    let Some(change) = maybe_change else {
                        return Err(BlockifierError::InvalidChainChange)
                            .attach_printable("chain changes stream ended unexpectedly");
                    };

                    match change {
                        ChainChange::NewHead(new_head) => {
                            head = new_head;
                            while block_ingestion.len() < max_concurrent_jobs {
                                if current_block_number > head.number {
                                    break;
                                }
                                current_block_number += 1;
                                block_ingestion.push_back(
                                    self.ingestion
                                        .ingest_block(Cursor::new_finalized(current_block_number)),
                                );
                            }
                        }
                        ChainChange::NewFinalized(new_finalized) => {
                            let Ok(_) = tx.send(BlockEvent::Finalized(new_finalized)).await else {
                                todo!();
                            };
                        }
                        ChainChange::Invalidate => {
                            let Ok(_) = tx.send(BlockEvent::Invalidate).await else {
                                todo!();
                            };
                        }
                        _ => {}
                    }
                }
            }
        }

        Ok(())
    }
}

impl error_stack::Context for BlockifierError {}

impl std::fmt::Display for BlockifierError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlockifierError::Configuration => {
                write!(f, "Configuration error")
            }
            BlockifierError::InvalidChainChange => {
                write!(f, "Invalid chain change")
            }
            BlockifierError::BlockIngestion => {
                write!(f, "Block ingestion error")
            }
        }
    }
}
