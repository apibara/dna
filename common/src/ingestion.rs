use std::{future::Future, sync::Arc};

use error_stack::{Report, Result, ResultExt};
use futures::{stream::FuturesOrdered, StreamExt};
use rkyv::ser::serializers::AllocSerializer;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace};

use crate::{
    block_store::BlockStore,
    chain::{BlockInfo, CanonicalChainBuilder},
    chain_store::ChainStore,
    object_store::ObjectStore,
    Cursor, Hash,
};

#[derive(Debug, Clone)]
pub enum IngestionError {
    BlockNotFound,
    RpcRequest,
    CanonicalChainStoreRequest,
    BlockStoreRequest,
    BadHash,
    Model,
}

pub trait BlockIngestion {
    type Block;

    fn get_head_cursor(&self) -> impl Future<Output = Result<Cursor, IngestionError>> + Send;
    fn get_finalized_cursor(&self) -> impl Future<Output = Result<Cursor, IngestionError>> + Send;

    fn ingest_block_by_number(
        &self,
        block_number: u64,
    ) -> impl Future<Output = Result<(BlockInfo, Self::Block), IngestionError>> + Send;

    fn ingest_block_by_hash(
        &self,
        block_hash: impl Into<Hash> + Send,
    ) -> impl Future<Output = Result<(BlockInfo, Self::Block), IngestionError>> + Send;
}

type IngestionTaskHandle = JoinHandle<Result<BlockInfo, IngestionError>>;

#[derive(Debug)]
pub struct IngestionServiceOptions {
    /// Maximum number of concurrent ingestion tasks.
    pub max_concurrent_tasks: usize,
    /// How many blocks in a single chain segment.
    pub chain_segment_size: usize,
    /// How many finalized blocks to wait before uploading a chain segment.
    pub chain_segment_upload_offset_size: usize,
}

pub struct IngestionService<B, I>
where
    I: BlockIngestion<Block = B>,
    B: Send + Sync + 'static,
{
    options: IngestionServiceOptions,
    ingestion: Arc<I>,
    chain_store: ChainStore,
    block_store: BlockStore<B>,
    chain_builder: CanonicalChainBuilder,
    task_queue: FuturesOrdered<IngestionTaskHandle>,
}

enum IngestionState {
    IngestFinalized(IngestFinalizedState),
}

struct IngestFinalizedState {
    cursor: Cursor,
    finalized: Cursor,
    head: Cursor,
}

impl<B, I> IngestionService<B, I>
where
    I: BlockIngestion<Block = B> + Send + Sync + 'static,
    B: Send + Sync + 'static + rkyv::Serialize<AllocSerializer<0>>,
{
    pub fn new(ingestion: I, object_store: ObjectStore, options: IngestionServiceOptions) -> Self {
        let chain_store = ChainStore::new(object_store.clone());
        let block_store = BlockStore::new(object_store);
        Self {
            options,
            ingestion: ingestion.into(),
            chain_store,
            block_store,
            chain_builder: CanonicalChainBuilder::new(),
            task_queue: FuturesOrdered::new(),
        }
    }

    pub async fn start(mut self, ct: CancellationToken) -> Result<(), IngestionError> {
        let head = self.ingestion.get_head_cursor().await?;
        let finalized = self.ingestion.get_finalized_cursor().await?;

        // TODO: fetch starting cursor from etcd.
        // Also download the most recent canonical chain segment and restore from it.
        let starting_cursor = Cursor::new_finalized(0);
        let existing_chain_segment = self
            .chain_store
            .get_recent()
            .await
            .change_context(IngestionError::CanonicalChainStoreRequest)?;

        if let Some(_existing_chain_segment) = existing_chain_segment {
            println!("existing chain segment");
        } else {
            println!("no existing chain segment");
        }

        let mut state = if starting_cursor.number <= finalized.number {
            IngestionState::IngestFinalized(IngestFinalizedState {
                cursor: starting_cursor,
                finalized,
                head,
            })
        } else {
            todo!();
        };

        loop {
            if ct.is_cancelled() {
                break;
            }

            match state {
                IngestionState::IngestFinalized(finalized_state) => {
                    state = self.tick_finalized(finalized_state, ct.clone()).await?;
                }
            }
        }

        Ok(())
    }

    async fn tick_finalized(
        &mut self,
        state: IngestFinalizedState,
        ct: CancellationToken,
    ) -> Result<IngestionState, IngestionError> {
        let mut block_number = state.cursor.number;

        println!("ingesting finalized");
        loop {
            tokio::select! {
                _ = ct.cancelled() => break,
                join_result = self.task_queue.next() => {
                    if let Some(join_result) = join_result {
                        let block_info = join_result
                            .change_context(IngestionError::RpcRequest)?
                            .attach_printable("failed to join ingestion task")
                            .change_context(IngestionError::RpcRequest)
                            .attach_printable("failed to ingest block")?;

                        info!(block = %block_info.cursor(), "ingested block");
                        self.chain_builder.grow(block_info).change_context(IngestionError::Model)?;

                        if self.chain_builder.segment_size() == self.options.chain_segment_size + self.options.chain_segment_upload_offset_size
                        {
                            let segment = self.chain_builder.take_segment(self.options.chain_segment_size).change_context(IngestionError::Model)?;
                            info!(first_block = %segment.info.first_block, "uploading chain segment");
                            println!("uploading chain segment {:?}", segment.info);
                            self.chain_store.put(&segment).await.change_context(IngestionError::CanonicalChainStoreRequest)?;

                            let current_segment = self.chain_builder.current_segment().change_context(IngestionError::Model)?;
                            info!(first_block = %current_segment.info.first_block, "uploading recent chain segment");
                            self.chain_store.put_recent(&current_segment).await.change_context(IngestionError::CanonicalChainStoreRequest)?;
                        }
                    }

                    while self.can_push_task() {
                        trace!(block_number, "pushing finalized ingestion task");
                        self.push_ingest_block_by_number(block_number);
                        block_number += 1;
                    }
                }
            }
        }

        todo!();
    }

    fn can_push_task(&self) -> bool {
        self.task_queue.len() < self.options.max_concurrent_tasks
    }

    fn push_ingest_block_by_number(&mut self, block_number: u64) {
        self.task_queue.push_back(tokio::spawn({
            let ingestion = self.ingestion.clone();
            let store = self.block_store.clone();
            async move {
                let (block_info, block) = ingestion
                    .ingest_block_by_number(block_number)
                    .await
                    .change_context(IngestionError::RpcRequest)
                    .attach_printable("failed to ingest block")
                    .attach_printable_lazy(|| format!("block number: {}", block_number))?;

                let block_cursor = block_info.cursor();
                debug!(cursor = %block_cursor, "uploading block");

                store
                    .put(&block_cursor, &block)
                    .await
                    .change_context(IngestionError::BlockStoreRequest)?;

                Ok(block_info)
            }
        }));
    }
}

pub trait IngestionErrorExt {
    fn is_block_not_found(&self) -> bool;
}

impl IngestionErrorExt for Report<IngestionError> {
    fn is_block_not_found(&self) -> bool {
        matches!(self.current_context(), IngestionError::BlockNotFound)
    }
}

impl error_stack::Context for IngestionError {}

impl std::fmt::Display for IngestionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IngestionError::BlockNotFound => write!(f, "ingestion error: block not found"),
            IngestionError::RpcRequest => write!(f, "ingestion error: rpc request error"),
            IngestionError::BlockStoreRequest => {
                write!(f, "ingestion error: block store request error")
            }
            IngestionError::CanonicalChainStoreRequest => {
                write!(f, "ingestion error: canonical chain store request error")
            }
            IngestionError::BadHash => write!(f, "ingestion error: bad hash"),
            IngestionError::Model => write!(f, "ingestion error: conversion error"),
        }
    }
}

impl Default for IngestionServiceOptions {
    fn default() -> Self {
        Self {
            max_concurrent_tasks: 100,
            chain_segment_size: 10_000,
            chain_segment_upload_offset_size: 100,
        }
    }
}
