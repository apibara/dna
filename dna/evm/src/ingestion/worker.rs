use std::mem;

use apibara_dna_common::{
    error::{DnaError, Result},
    ingestion::{IngestionEvent, SealGroup, Segment},
    segment::{SnapshotBuilder, SnapshotState},
    storage::StorageBackend,
};
use error_stack::ResultExt;
use futures_util::TryFutureExt;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::{
    ingestion::models,
    segment::{SegmentBuilder, SegmentGroupBuilder},
};

use super::{IngestorOptions, RpcProvider};

#[derive(Clone)]
pub struct IngestionWorker {
    tx: mpsc::Sender<IngestionCommand>,
}

pub struct IngestionWorkerBuilder<S>
where
    S: StorageBackend + Send + Sync + 'static,
{
    provider: RpcProvider,
    storage: S,
    options: IngestorOptions,
}

enum IngestionCommand {
    IngestBlockByNumber {
        block_number: u64,
        reply: oneshot::Sender<Option<IngestionEvent>>,
    },
}

struct Worker<'a, S>
where
    S: StorageBackend + Send + Sync + 'static,
{
    provider: RpcProvider,
    storage: S,
    options: IngestorOptions,
    snapshot_builder: SnapshotBuilder<'a>,
    segment_builder: SegmentBuilder<'a>,
    segment_group_builder: SegmentGroupBuilder<'a>,
}

impl IngestionWorker {
    pub async fn ingest_block_by_number(
        &self,
        block_number: u64,
    ) -> Result<Option<IngestionEvent>> {
        let (reply_tx, reply_rx) = oneshot::channel();

        self.tx
            .send(IngestionCommand::IngestBlockByNumber {
                block_number,
                reply: reply_tx,
            })
            .await
            .map_err(|_| DnaError::Fatal)
            .attach_printable("failed to send ingestion command")?;

        let response = reply_rx
            .await
            .map_err(|_| DnaError::Fatal)
            .attach_printable("failed to receive reply")?;

        Ok(response)
    }
}

impl<S> IngestionWorkerBuilder<S>
where
    S: StorageBackend + Send + Sync + 'static,
    <S as StorageBackend>::Reader: Unpin + Send,
    <S as StorageBackend>::Writer: Unpin + Send,
{
    pub fn new(provider: RpcProvider, storage: S, options: IngestorOptions) -> Self {
        Self {
            provider,
            storage,
            options,
        }
    }

    pub async fn start(self, ct: CancellationToken) -> Result<(IngestionWorker, SnapshotState)> {
        let (tx, rx) = mpsc::channel(128);

        let client = IngestionWorker { tx };
        let inner = Worker::initialize(self.provider, self.storage, self.options).await?;
        let starting_state = inner.snapshot_state().clone();

        tokio::spawn(inner.ingestion_loop(rx, ct).inspect_err(|err| {
            error!(err = ?err, "ingestion loop returned with error");
        }));

        Ok((client, starting_state))
    }
}

impl<'a, S> Worker<'a, S>
where
    S: StorageBackend + Send + Sync + 'static,
    <S as StorageBackend>::Reader: Unpin + Send,
    <S as StorageBackend>::Writer: Unpin + Send,
{
    pub async fn initialize(
        provider: RpcProvider,
        mut storage: S,
        options: IngestorOptions,
    ) -> Result<Worker<'a, S>> {
        let snapshot_builder = SnapshotBuilder::from_storage(&mut storage)
            .await?
            .unwrap_or_else(|| {
                SnapshotBuilder::new(options.starting_block, options.segment.clone())
            });

        let segment_builder = SegmentBuilder::default();
        let segment_group_builder = SegmentGroupBuilder::default();

        Ok(Self {
            provider,
            storage,
            options,
            snapshot_builder,
            segment_builder,
            segment_group_builder,
        })
    }

    pub fn snapshot_state(&self) -> &SnapshotState {
        self.snapshot_builder.state()
    }

    pub async fn ingestion_loop(
        mut self,
        mut rx: mpsc::Receiver<IngestionCommand>,
        ct: CancellationToken,
    ) -> Result<()> {
        loop {
            tokio::select! {
                Some(command) = rx.recv() => {
                    self.handle_command(command).await?;
                }
                _ = ct.cancelled() => {
                    break;
                }
                else => {
                    break;
                }
            }
        }

        Ok(())
    }

    async fn handle_command(&mut self, command: IngestionCommand) -> Result<()> {
        match command {
            IngestionCommand::IngestBlockByNumber {
                block_number,
                reply,
            } => {
                let event = self.do_ingest_block_by_number(block_number).await?;

                reply
                    .send(event)
                    .map_err(|_| DnaError::Fatal)
                    .attach_printable("failed to send reply")?;
            }
        }

        Ok(())
    }

    async fn do_ingest_block_by_number(
        &mut self,
        block_number: u64,
    ) -> Result<Option<IngestionEvent>> {
        use models::BlockTransactions;
        debug!(block_number, "ingesting block by number");

        let mut block = if self.options.get_block_by_number_with_transactions {
            self.provider
                .get_block_by_number_with_transactions(block_number)
                .await?
        } else {
            self.provider.get_block_by_number(block_number).await?
        };

        let empty_transactions = BlockTransactions::Hashes(Vec::new());
        let transactions = match mem::replace(&mut block.transactions, empty_transactions) {
            BlockTransactions::Full(transactions) => transactions,
            BlockTransactions::Hashes(hashes) => {
                self.provider
                    .get_transactions_by_hash(hashes.iter())
                    .await?
            }
            _ => panic!("ingesting uncle block"),
        };

        let receipts = if self.options.get_block_receipts_by_number {
            self.provider
                .get_block_receipts_by_number(block_number)
                .await?
        } else {
            self.provider
                .get_receipts_by_hash(block.transactions.hashes())
                .await?
        };

        self.segment_builder.add_block_header(block_number, &block);
        self.segment_builder
            .add_transactions(block_number, &transactions);
        self.segment_builder.add_receipts(block_number, &receipts);
        self.segment_builder.add_logs(block_number, &receipts);

        self.finish_block_ingestion(block_number).await
    }

    async fn finish_block_ingestion(
        &mut self,
        block_number: u64,
    ) -> Result<Option<IngestionEvent>> {
        let segment_options = &self.options.segment;

        // If the segment is not full, we're done.
        if self.segment_builder.header_count() < segment_options.segment_size {
            return Ok(None);
        }

        let segment_name = segment_options.format_segment_name(block_number);
        self.segment_builder
            .write(&format!("segment/{segment_name}"), &mut self.storage)
            .await?;
        let index = self.segment_builder.take_index();
        self.segment_group_builder
            .add_segment(segment_options.segment_start(block_number));
        self.segment_group_builder.add_index(&index);
        self.segment_builder.reset();

        info!(segment_name, "segment written");

        if self.segment_group_builder.segment_count() < segment_options.group_size {
            let first_block_number = segment_options.segment_start(block_number);
            let event = IngestionEvent::SegmentAdded(Segment { first_block_number });
            return Ok(Some(event));
        }

        let group_name = segment_options.format_segment_group_name(block_number);
        self.segment_group_builder
            .write(&group_name, &mut self.storage)
            .await?;
        self.segment_group_builder.reset();
        info!(group_name, "segment group written");

        let new_revision = self
            .snapshot_builder
            .write_revision(&mut self.storage)
            .await?;
        self.snapshot_builder.reset();
        info!(revision = new_revision, "snapshot written");

        let first_block_number = segment_options.segment_group_start(block_number);
        let event = IngestionEvent::GroupSealed(SealGroup {
            first_block_number,
            revision: new_revision,
        });
        Ok(Some(event))
    }
}
