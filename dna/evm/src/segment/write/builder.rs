use apibara_dna_common::{
    error::{DnaError, Result},
    storage::StorageBackend,
};
use error_stack::ResultExt;
use tokio::io::AsyncWriteExt;

use crate::ingestion::models;

use super::{
    header::BlockHeaderSegmentBuilder, index::SegmentIndex, log::LogSegmentBuilder,
    receipt::ReceiptSegmentBuilder, transaction::TransactionSegmentBuilder,
};

pub struct SegmentBuilder<'a> {
    header: BlockHeaderSegmentBuilder<'a>,
    transaction: TransactionSegmentBuilder<'a>,
    receipt: ReceiptSegmentBuilder<'a>,
    log: LogSegmentBuilder<'a>,
    index: SegmentIndex,
}

impl<'a> Default for SegmentBuilder<'a> {
    fn default() -> Self {
        Self {
            header: BlockHeaderSegmentBuilder::new(),
            transaction: TransactionSegmentBuilder::new(),
            receipt: ReceiptSegmentBuilder::new(),
            log: LogSegmentBuilder::new(),
            index: SegmentIndex::default(),
        }
    }
}

impl<'a> SegmentBuilder<'a> {
    pub fn add_block_header(&mut self, block_number: u64, header: &models::Block) {
        self.header.add_block_header(block_number, header);
    }

    pub fn add_transactions(&mut self, block_number: u64, transactions: &[models::Transaction]) {
        self.transaction
            .add_transactions(block_number, transactions);
    }

    pub fn add_receipts(&mut self, block_number: u64, receipts: &[models::TransactionReceipt]) {
        self.receipt.add_receipts(block_number, receipts);
    }

    pub fn add_logs(&mut self, block_number: u64, receipts: &[models::TransactionReceipt]) {
        self.log.add_logs(block_number, receipts);
        self.index.add_logs(block_number, receipts);
    }

    pub fn take_index(&mut self) -> SegmentIndex {
        std::mem::take(&mut self.index)
    }

    pub async fn write<S: StorageBackend>(
        &mut self,
        segment_name: &str,
        storage: &mut S,
    ) -> Result<()> {
        {
            let mut writer = storage.put(segment_name, "header").await?;
            self.header.write_segment(&mut writer).await?;
            writer.shutdown().await.change_context(DnaError::Io)?;
        }

        {
            let mut writer = storage.put(segment_name, "transaction").await?;
            self.transaction.write_segment(&mut writer).await?;
            writer.shutdown().await.change_context(DnaError::Io)?;
        }

        {
            let mut writer = storage.put(segment_name, "receipt").await?;
            self.receipt.write_segment(&mut writer).await?;
            writer.shutdown().await.change_context(DnaError::Io)?;
        }

        {
            let mut writer = storage.put(segment_name, "log").await?;
            self.log.write_segment(&mut writer).await?;
            writer.shutdown().await.change_context(DnaError::Io)?;
        }

        Ok(())
    }

    pub fn reset(&mut self) {
        self.header.reset();
        self.transaction.reset();
        self.receipt.reset();
        self.log.reset();
        self.index.clear();
    }
}