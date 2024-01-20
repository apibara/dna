use apibara_dna_common::error::{DnaError, Result};
use error_stack::ResultExt;
use tracing::{debug, instrument};

use crate::provider::{models, RpcProvider};

pub struct FullBlock {
    pub block: models::BlockWithTxHashes,
    pub transactions: Vec<models::Transaction>,
    pub receipts: Vec<models::TransactionReceipt>,
}

pub struct RpcBlockDownloader {
    provider: RpcProvider,
}

impl RpcBlockDownloader {
    pub fn new(provider: RpcProvider) -> Self {
        Self { provider }
    }

    #[instrument(skip(self), err(Debug))]
    pub async fn download_block_by_number(&self, number: u64) -> Result<FullBlock> {
        debug!(number, "downloading block");
        let block_id = models::BlockId::Number(number);
        let models::MaybePendingBlockWithTxHashes::Block(block) =
            self.provider.get_block_with_tx_hashes(&block_id).await?
        else {
            return Err(DnaError::Fatal)
                .attach_printable("expected block to be finalized, it's pending");
        };

        assert!(block.status == models::BlockStatus::AcceptedOnL1);

        /*
        let transactions = self
            .provider
            .get_transactions_by_hash(&block.transactions)
            .await?;

        // Check that the order was preserved.
        assert!(transactions.len() == receipts.len());
        for (hash, tx) in block.transactions.iter().zip(transactions.iter()) {
            assert!(hash == tx.transaction_hash());
        }
        */

        let receipts = self
            .provider
            .get_transactions_receipts(&block.transactions)
            .await?;

        for (hash, rx) in block.transactions.iter().zip(receipts.iter()) {
            assert!(hash == rx.transaction_hash());
        }

        Ok(FullBlock {
            block,
            transactions: vec![],
            receipts,
        })
    }
}
