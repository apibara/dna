use std::{mem, time::Duration};

use alloy_primitives::B256;
use apibara_dna_common::{
    error::Result,
    storage::{LocalStorageBackend, StorageBackend},
};
use futures_util::Stream;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::{core::Cursor, ingestion::models};

use super::{ChainChange, RpcIngestionOptions, RpcProvider};

#[derive(Debug, Clone)]
pub enum BlockNumberOrHash {
    Number(u64),
    Hash(B256),
}

#[derive(Debug, Clone)]
pub enum BlockEvent {
    Finalized(Cursor),
    Ingested(Cursor),
    Invalidate,
}

pub struct BlockDownloaderService<C>
where
    C: Stream<Item = ChainChange> + Unpin + Send + Sync + 'static,
{
    provider: RpcProvider,
    storage: LocalStorageBackend,
    chain_changes: C,
    options: RpcIngestionOptions,
}

impl<C> BlockDownloaderService<C>
where
    C: Stream<Item = ChainChange> + Unpin + Send + Sync + 'static,
{
    pub fn new(
        provider: RpcProvider,
        storage: LocalStorageBackend,
        chain_changes: C,
        options: RpcIngestionOptions,
    ) -> Self {
        Self {
            provider,
            storage,
            chain_changes,
            options,
        }
    }

    pub fn start(self, ct: CancellationToken) -> impl Stream<Item = BlockEvent> {
        let (tx, rx) = mpsc::channel(128);

        tokio::spawn({
            let mut chain_changes = self.chain_changes;
            async move {
                while let Some(xxx) = chain_changes.next().await {
                    match xxx {
                        ChainChange::Initialize { head, finalized } => {
                            debug!(head = ?head, finalized = ?finalized, "initializing chain tracker");
                        }
                        ChainChange::NewHead(head) => {
                            debug!(head = ?head, "new head detected");
                            tx.send(BlockEvent::Ingested(head)).await.unwrap();
                        }
                        ChainChange::NewFinalized(finalized) => {
                            debug!(finalized = ?finalized, "new finalized block detected");
                            tx.send(BlockEvent::Finalized(finalized)).await.unwrap();
                        }
                        ChainChange::Invalidate => {
                            debug!("chain reorganized");
                            tx.send(BlockEvent::Invalidate).await.unwrap();
                        }
                    }
                }
            }
        });

        ReceiverStream::new(rx)
    }

    /*
    pub async fn download_block(&self, block: BlockNumberOrHash) -> Result<()> {
        use models::BlockTransactions;

        debug!(block = ?block, "ingesting block by number");

        let block_number = match block {
            BlockNumberOrHash::Number(number) => number,
            BlockNumberOrHash::Hash(_) => {
                todo!();
            }
        };

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

        Ok(())
    }
    */
}

impl Into<BlockNumberOrHash> for u64 {
    fn into(self) -> BlockNumberOrHash {
        BlockNumberOrHash::Number(self)
    }
}
