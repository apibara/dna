use apibara_dna_common::store::{Block, StoreError};
use error_stack::{Result, ResultExt};

use crate::store::fragment::{BlockEvents, BlockMessages, BlockReceipts, BlockTransactions};

use super::fragment::{BlockHeader, Event, MessageToL1, Transaction, TransactionReceipt};

pub struct BlockBuilder {
    pub header: BlockHeader,
    pub transactions: Vec<Transaction>,
    pub receipts: Vec<TransactionReceipt>,
    pub events: Vec<Event>,
    pub messages: Vec<MessageToL1>,
}

impl BlockBuilder {
    pub fn build(self) -> Result<Block, StoreError> {
        // TODO: index

        let mut block = Block::default();
        block
            .add_fragment(self.header)
            .change_context(StoreError::Fragment)?;
        block
            .add_fragment(BlockTransactions(self.transactions))
            .change_context(StoreError::Fragment)?;
        block
            .add_fragment(BlockReceipts(self.receipts))
            .change_context(StoreError::Fragment)?;
        block
            .add_fragment(BlockEvents(self.events))
            .change_context(StoreError::Fragment)?;
        block
            .add_fragment(BlockMessages(self.messages))
            .change_context(StoreError::Fragment)?;

        Ok(block)
    }
}
