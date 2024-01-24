use error_stack::ResultExt;
use flatbuffers::{FlatBufferBuilder, WIPOffset};

use apibara_dna_common::error::{DnaError, Result};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{ingestion::models, segment::store};

use super::conversion::U256Ext;

pub struct TransactionSegmentBuilder<'a> {
    builder: FlatBufferBuilder<'a>,
    first_block_number: Option<u64>,
    blocks: Vec<WIPOffset<store::BlockTransactions<'a>>>,
}

impl<'a> TransactionSegmentBuilder<'a> {
    pub fn new() -> Self {
        Self {
            builder: FlatBufferBuilder::new(),
            first_block_number: None,
            blocks: Vec::new(),
        }
    }

    pub fn add_transactions(&mut self, block_number: u64, transactions: &[models::Transaction]) {
        if self.first_block_number.is_none() {
            self.first_block_number = Some(block_number);
        }

        let transactions = transactions
            .iter()
            .map(|tx| self.add_single_transaction(tx))
            .collect::<Vec<_>>();

        let transactions = self.builder.create_vector(&transactions);

        let mut block = store::BlockTransactionsBuilder::new(&mut self.builder);
        block.add_block_number(block_number);
        block.add_transactions(transactions);

        self.blocks.push(block.finish());
    }

    pub async fn write_segment<W: AsyncWrite + Unpin>(&mut self, writer: &mut W) -> Result<()> {
        let first_block_number = self
            .first_block_number
            .ok_or(DnaError::Fatal)
            .attach_printable("writing transaction segment but no block ingested")?;

        let blocks = self.builder.create_vector(&self.blocks);
        let mut segment = store::TransactionSegmentBuilder::new(&mut self.builder);
        segment.add_first_block_number(first_block_number);
        segment.add_blocks(blocks);

        let segment = segment.finish();
        self.builder.finish(segment, None);

        writer
            .write_all(self.builder.finished_data())
            .await
            .change_context(DnaError::Io)
            .attach_printable("failed to write transaction segment")?;

        Ok(())
    }

    pub fn reset(&mut self) {
        self.first_block_number = None;
        self.blocks.clear();
        self.builder.reset();
    }

    fn add_single_transaction(
        &mut self,
        transaction: &models::Transaction,
    ) -> WIPOffset<store::Transaction<'a>> {
        let input = self.builder.create_vector(&transaction.input.0);
        let signature = self.create_signature(transaction);
        let access_list = if let Some(access_list) = &transaction.access_list {
            let items = access_list
                .0
                .iter()
                .map(|item| self.create_access_list_item(item))
                .collect::<Vec<_>>();
            Some(self.builder.create_vector(&items))
        } else {
            None
        };

        let mut out = store::TransactionBuilder::new(&mut self.builder);

        out.add_hash(&transaction.hash.into());
        out.add_nonce(transaction.nonce.as_u64());
        if let Some(transaction_index) = transaction.transaction_index {
            out.add_transaction_index(transaction_index.as_u64());
        }
        out.add_from(&transaction.from.into());
        if let Some(to) = transaction.to {
            out.add_to(&to.into());
        }
        out.add_value(&transaction.value.into());
        if let Some(gas_price) = transaction.gas_price {
            out.add_gas_price(&gas_price.into_u128());
        }
        if let Some(max_fee_per_gas) = transaction.max_fee_per_gas {
            out.add_max_fee_per_gas(&max_fee_per_gas.into_u128());
        }
        if let Some(max_priority_fee_per_gas) = transaction.max_priority_fee_per_gas {
            out.add_max_priority_fee_per_gas(&max_priority_fee_per_gas.into_u128());
        }
        out.add_input(input);
        out.add_signature(signature);
        if let Some(chain_id) = transaction.chain_id {
            out.add_chain_id(chain_id.as_u64());
        }
        if let Some(access_list) = access_list {
            out.add_access_list(access_list);
        }
        if let Some(transaction_type) = transaction.transaction_type {
            out.add_transaction_type(transaction_type.as_u32());
        }

        out.finish()
    }

    fn create_signature(
        &mut self,
        transaction: &models::Transaction,
    ) -> WIPOffset<store::Signature<'a>> {
        let mut out = store::SignatureBuilder::new(&mut self.builder);
        out.add_r(&transaction.r.into());
        out.add_s(&transaction.s.into());
        out.finish()
    }

    fn create_access_list_item(
        &mut self,
        item: &models::AccessListItem,
    ) -> WIPOffset<store::AccessListItem<'a>> {
        let storage_keys: Vec<store::B256> = item
            .storage_keys
            .iter()
            .map(|key| key.into())
            .collect::<Vec<_>>();
        let storage_keys = self.builder.create_vector(&storage_keys);

        let mut out = store::AccessListItemBuilder::new(&mut self.builder);
        out.add_address(&item.address.into());
        out.add_storage_keys(storage_keys);
        out.finish()
    }
}
