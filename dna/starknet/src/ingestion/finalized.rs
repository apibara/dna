//! Ingest finalized blocks from Starknet.

use apibara_dna_common::error::{DnaError, Result};
use error_stack::ResultExt;
use tracing::instrument;

use crate::{
    provider::{models, RpcProvider},
    segment::BlockSegmentBuilder,
};

pub struct FinalizedBlockIngestion {
    provider: RpcProvider,
}

impl FinalizedBlockIngestion {
    pub fn new(provider: RpcProvider) -> Self {
        Self { provider }
    }

    #[instrument(skip(self, builder), err(Debug))]
    pub async fn ingest_block_by_number<'a>(
        &self,
        builder: &mut BlockSegmentBuilder<'a>,
        number: u64,
    ) -> Result<()> {
        let block_id = models::BlockId::Number(number);
        let models::MaybePendingBlockWithTxHashes::Block(block) =
            self.provider.get_block_with_tx_hashes(&block_id).await?
        else {
            return Err(DnaError::Fatal)
                .attach_printable("expected block to be finalized, it's pending");
        };

        assert!(block.status == models::BlockStatus::AcceptedOnL1);

        let transactions = self
            .provider
            .get_transactions_by_hash(&block.transactions)
            .await?;

        let receipts = self
            .provider
            .get_transactions_receipts(&block.transactions)
            .await?;

        // Check that the order was preserved.
        assert!(transactions.len() == receipts.len());
        for (hash, tx) in block.transactions.iter().zip(transactions.iter()) {
            assert!(hash == tx.transaction_hash());
        }

        for (tx, rx) in transactions.iter().zip(receipts.iter()) {
            assert!(tx.transaction_hash() == rx.transaction_hash());
        }

        builder.add_block_header(&block)?;
        builder.add_transactions(&transactions)?;
        builder.add_receipts(&receipts)?;
        builder.add_events(block.block_number, &receipts)?;

        Ok(())
    }
}
