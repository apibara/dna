use error_stack::ResultExt;
use flatbuffers::{FlatBufferBuilder, WIPOffset};

use apibara_dna_common::error::{DnaError, Result};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{ingestion::models, segment::store};

pub struct ReceiptSegmentBuilder<'a> {
    builder: FlatBufferBuilder<'a>,
    first_block_number: Option<u64>,
    blocks: Vec<WIPOffset<store::BlockReceipts<'a>>>,
}

impl<'a> ReceiptSegmentBuilder<'a> {
    pub fn new() -> Self {
        Self {
            builder: FlatBufferBuilder::new(),
            first_block_number: None,
            blocks: Vec::new(),
        }
    }

    pub fn add_receipts(&mut self, block_number: u64, receipts: &[models::TransactionReceipt]) {
        if self.first_block_number.is_none() {
            self.first_block_number = Some(block_number);
        }

        let receipts = receipts
            .iter()
            .map(|tx| self.add_single_receipt(tx))
            .collect::<Vec<_>>();

        let receipts = self.builder.create_vector(&receipts);

        let mut block = store::BlockReceiptsBuilder::new(&mut self.builder);
        block.add_block_number(block_number);
        block.add_receipts(receipts);

        self.blocks.push(block.finish());
    }

    pub async fn write_segment<W: AsyncWrite + Unpin>(&mut self, writer: &mut W) -> Result<()> {
        let first_block_number = self
            .first_block_number
            .ok_or(DnaError::Fatal)
            .attach_printable("writing receipt segment but no block ingested")?;

        let blocks = self.builder.create_vector(&self.blocks);
        let mut segment = store::ReceiptSegmentBuilder::new(&mut self.builder);
        segment.add_first_block_number(first_block_number);
        segment.add_blocks(blocks);

        let segment = segment.finish();
        self.builder.finish(segment, None);

        writer
            .write_all(self.builder.finished_data())
            .await
            .change_context(DnaError::Io)
            .attach_printable("failed to write receipt segment")?;

        Ok(())
    }

    pub fn reset(&mut self) {
        self.first_block_number = None;
        self.blocks.clear();
        self.builder.reset();
    }

    fn add_single_receipt(
        &mut self,
        receipt: &models::TransactionReceipt,
    ) -> WIPOffset<store::TransactionReceipt<'a>> {
        let mut out = store::TransactionReceiptBuilder::new(&mut self.builder);

        out.add_transaction_hash(&receipt.transaction_hash.into());
        out.add_transaction_index(receipt.transaction_index.as_u64());
        out.add_cumulative_gas_used(&receipt.cumulative_gas_used.into());
        if let Some(gas_used) = receipt.gas_used {
            out.add_gas_used(&gas_used.into());
        }
        if let Some(effective_gas_price) = receipt.effective_gas_price {
            out.add_effective_gas_price(&effective_gas_price.as_u128().into());
        }
        out.add_from(&receipt.from.into());
        if let Some(to) = receipt.to {
            out.add_to(&to.into());
        }
        if let Some(contract_address) = receipt.contract_address {
            out.add_contract_address(&contract_address.into());
        }
        out.add_logs_bloom(&receipt.logs_bloom.into());
        if let Some(status) = receipt.status {
            out.add_status_code(status.as_u64());
        }
        if let Some(transaction_type) = receipt.transaction_type {
            out.add_transaction_type(transaction_type.as_u32());
        }

        out.finish()
    }
}
