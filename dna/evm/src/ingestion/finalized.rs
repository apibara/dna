use std::mem;

use apibara_dna_common::error::{DnaError, Result};

use crate::segment::SegmentBuilder;

use super::{models, RpcProvider};

#[derive(Clone, Debug)]
pub struct IngestorOptions {
    /// Fetch transactions for each block in a single call.
    pub get_block_by_number_with_transactions: bool,
    /// Use `eth_getBlockReceipts` instead of `eth_getTransactionReceipt`.
    pub get_block_receipts_by_number: bool,
}

pub enum IngestionEvent {
    Segment {
        count: usize,
        first_block_number: u64,
        last_block_number: u64,
    },
    Completed {
        last_ingested_block: u64,
    },
}

/// Ingest finalized blocks.
///
/// Finalized blocks can be ingested differently than regular blocks
/// since the data is immutable and final.
pub struct FinalizedBlockIngestor {
    provider: RpcProvider,
    current_block_number: u64,
    target_block_number: Option<u64>,
    options: IngestorOptions,
}

impl FinalizedBlockIngestor {
    pub fn new(
        provider: RpcProvider,
        starting_block_number: u64,
        options: IngestorOptions,
    ) -> Self {
        Self {
            provider,
            current_block_number: starting_block_number,
            target_block_number: None,
            options,
        }
    }

    pub async fn ingest_next_segment<'a>(
        &mut self,
        builder: &mut SegmentBuilder<'a>,
        max_blocks: usize,
    ) -> Result<IngestionEvent> {
        let finalized = self.refresh_finalized_block().await?;
        if self.current_block_number > finalized {
            return Ok(IngestionEvent::Completed {
                last_ingested_block: self.current_block_number - 1,
            });
        }

        let max_blocks =
            std::cmp::min(max_blocks, (finalized - self.current_block_number) as usize);

        let first_block_number = self.current_block_number;

        for _ in 0..max_blocks {
            self.do_ingest_next_segment(builder).await?;
        }

        let last_block_number = self.current_block_number - 1;

        Ok(IngestionEvent::Segment {
            count: max_blocks,
            first_block_number,
            last_block_number,
        })
    }

    pub async fn do_ingest_next_segment<'a>(
        &mut self,
        builder: &mut SegmentBuilder<'a>,
    ) -> Result<()> {
        let block_number = self.current_block_number;

        let (block, transactions) = if self.options.get_block_by_number_with_transactions {
            let mut block_with_txs = self
                .provider
                .get_block_by_number_with_transactions(block_number)
                .await?;
            let transactions = mem::take(&mut block_with_txs.transactions);
            let block: models::Block<models::H256> = block_with_txs.into();
            (block, transactions)
        } else {
            let block = self.provider.get_block_by_number(block_number).await?;
            let transactions = self
                .provider
                .get_transactions_by_hash(&block.transactions)
                .await?;
            (block, transactions)
        };

        let receipts = if self.options.get_block_receipts_by_number {
            self.provider
                .get_block_receipts_by_number(block_number)
                .await?
        } else {
            self.provider
                .get_receipts_by_hash(&block.transactions)
                .await?
        };

        builder.add_block_header(block_number, &block);
        builder.add_transactions(block_number, &transactions);
        builder.add_receipts(block_number, &receipts);
        builder.add_logs(block_number, &receipts);

        self.current_block_number += 1;

        Ok(())
    }

    async fn refresh_finalized_block(&mut self) -> Result<u64> {
        if let Some(target_block_number) = self.target_block_number {
            if self.current_block_number < target_block_number {
                return Ok(target_block_number);
            }
        }

        let finalized_block = self.provider.get_finalized_block().await?;
        let finalized = finalized_block.number.ok_or(DnaError::Fatal)?.as_u64();
        self.target_block_number = Some(finalized);
        Ok(finalized)
    }
}

impl Default for IngestorOptions {
    fn default() -> Self {
        Self {
            get_block_by_number_with_transactions: true,
            get_block_receipts_by_number: false,
        }
    }
}
