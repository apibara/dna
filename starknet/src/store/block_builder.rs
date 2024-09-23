use apibara_dna_common::store::{
    bitmap::{BitmapMapBuilder, RoaringBitmapExt},
    index::IndexGroup,
    Block, StoreError,
};
use error_stack::{Result, ResultExt};
use roaring::RoaringBitmap;

use crate::store::fragment::{BlockEvents, BlockMessages, BlockReceipts, BlockTransactions};

use super::{
    fragment::{BlockHeader, Event, MessageToL1, Transaction, TransactionReceipt, TransactionType},
    index::{
        IndexEventByAddress, IndexEventByKey0, IndexEventByKey1, IndexEventByKey2,
        IndexEventByKey3, IndexEventByTransactionIndex, IndexMessageByFromAddress,
        IndexMessageByToAddress, IndexMessageByTransactionIndex, IndexTransactionByEventIndex,
        IndexTransactionByMessageIndex, IndexTransactionByType,
    },
};

pub struct BlockBuilder {
    pub header: BlockHeader,
    pub transactions: Vec<Transaction>,
    pub receipts: Vec<TransactionReceipt>,
    pub events: Vec<Event>,
    pub messages: Vec<MessageToL1>,
}

impl BlockBuilder {
    pub fn build(self) -> Result<Block, StoreError> {
        let mut index = IndexGroup::default();

        let transaction_range = self.compute_transaction_range();
        let event_range = self.compute_event_range();
        let message_range = self.compute_message_range();

        self.index_events(&mut index, event_range, transaction_range.clone())?;
        self.index_messages(&mut index, message_range, transaction_range.clone())?;
        self.index_transactions(&mut index, transaction_range)?;

        assert_eq!(index.indices.len(), 12);

        let mut block = Block::default();
        block
            .add_fragment(index)
            .change_context(StoreError::Fragment)?;
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

        assert_eq!(block.fragments.len(), 6);

        Ok(block)
    }

    fn compute_transaction_range(&self) -> RoaringBitmap {
        let mut transaction_range = RoaringBitmap::new();

        for tx in self.transactions.iter() {
            transaction_range.insert(tx.transaction_index());
        }

        transaction_range
    }

    fn compute_event_range(&self) -> RoaringBitmap {
        let mut event_range = RoaringBitmap::new();

        for event in self.events.iter() {
            event_range.insert(event.event_index);
        }

        event_range
    }

    fn compute_message_range(&self) -> RoaringBitmap {
        let mut message_range = RoaringBitmap::new();

        for message in self.messages.iter() {
            message_range.insert(message.message_index);
        }

        message_range
    }

    fn index_events(
        &self,
        index: &mut IndexGroup,
        event_range: RoaringBitmap,
        transaction_range: RoaringBitmap,
    ) -> Result<(), StoreError> {
        let mut event_by_address = BitmapMapBuilder::default();
        let mut event_by_key_0 = BitmapMapBuilder::default();
        let mut event_by_key_1 = BitmapMapBuilder::default();
        let mut event_by_key_2 = BitmapMapBuilder::default();
        let mut event_by_key_3 = BitmapMapBuilder::default();
        let mut event_by_transaction_index = BitmapMapBuilder::default();
        let mut transaction_by_event_index = BitmapMapBuilder::default();

        for event in self.events.iter() {
            let event_index = event.event_index;

            event_by_address.entry(event.address).insert(event_index);

            let mut keys = event.keys.iter();

            if let Some(key) = keys.next() {
                event_by_key_0.entry(*key).insert(event_index);
            }
            if let Some(key) = keys.next() {
                event_by_key_1.entry(*key).insert(event_index);
            }
            if let Some(key) = keys.next() {
                event_by_key_2.entry(*key).insert(event_index);
            }
            if let Some(key) = keys.next() {
                event_by_key_3.entry(*key).insert(event_index);
            }

            event_by_transaction_index
                .entry(event.transaction_index)
                .insert(event_index);

            transaction_by_event_index
                .entry(event_index)
                .insert(event.transaction_index);
        }

        let event_by_address = event_by_address
            .into_bitmap_map()
            .change_context(StoreError::Indexing)?;
        let event_by_key_0 = event_by_key_0
            .into_bitmap_map()
            .change_context(StoreError::Indexing)?;
        let event_by_key_1 = event_by_key_1
            .into_bitmap_map()
            .change_context(StoreError::Indexing)?;
        let event_by_key_2 = event_by_key_2
            .into_bitmap_map()
            .change_context(StoreError::Indexing)?;
        let event_by_key_3 = event_by_key_3
            .into_bitmap_map()
            .change_context(StoreError::Indexing)?;
        let event_by_transaction_index = event_by_transaction_index
            .into_bitmap_map()
            .change_context(StoreError::Indexing)?;
        let transaction_by_event_index = transaction_by_event_index
            .into_bitmap_map()
            .change_context(StoreError::Indexing)?;

        let event_range = event_range
            .into_bitmap()
            .change_context(StoreError::Indexing)?;
        let transaction_range = transaction_range
            .into_bitmap()
            .change_context(StoreError::Indexing)?;

        index
            .add_index::<IndexEventByAddress>(event_range.clone(), event_by_address)
            .change_context(StoreError::Indexing)?;
        index
            .add_index::<IndexEventByKey0>(event_range.clone(), event_by_key_0)
            .change_context(StoreError::Indexing)?;
        index
            .add_index::<IndexEventByKey1>(event_range.clone(), event_by_key_1)
            .change_context(StoreError::Indexing)?;
        index
            .add_index::<IndexEventByKey2>(event_range.clone(), event_by_key_2)
            .change_context(StoreError::Indexing)?;
        index
            .add_index::<IndexEventByKey3>(event_range.clone(), event_by_key_3)
            .change_context(StoreError::Indexing)?;
        index
            .add_index::<IndexEventByTransactionIndex>(event_range, event_by_transaction_index)
            .change_context(StoreError::Indexing)?;
        index
            .add_index::<IndexTransactionByEventIndex>(
                transaction_range,
                transaction_by_event_index,
            )
            .change_context(StoreError::Indexing)?;

        Ok(())
    }

    fn index_messages(
        &self,
        index: &mut IndexGroup,
        message_range: RoaringBitmap,
        transaction_range: RoaringBitmap,
    ) -> Result<(), StoreError> {
        let mut transaction_by_message_index = BitmapMapBuilder::default();
        let mut message_by_transaction_index = BitmapMapBuilder::default();
        let mut message_by_from_address = BitmapMapBuilder::default();
        let mut message_by_to_address = BitmapMapBuilder::default();

        for message in self.messages.iter() {
            let message_index = message.message_index;

            transaction_by_message_index
                .entry(message_index)
                .insert(message.transaction_index);

            message_by_transaction_index
                .entry(message.transaction_index)
                .insert(message_index);

            message_by_from_address
                .entry(message.from_address)
                .insert(message_index);

            message_by_to_address
                .entry(message.to_address)
                .insert(message_index);
        }

        let transaction_by_message_index = transaction_by_message_index
            .into_bitmap_map()
            .change_context(StoreError::Indexing)?;
        let message_by_transaction_index = message_by_transaction_index
            .into_bitmap_map()
            .change_context(StoreError::Indexing)?;
        let message_by_from_address = message_by_from_address
            .into_bitmap_map()
            .change_context(StoreError::Indexing)?;
        let message_by_to_address = message_by_to_address
            .into_bitmap_map()
            .change_context(StoreError::Indexing)?;

        let message_range = message_range
            .into_bitmap()
            .change_context(StoreError::Indexing)?;
        let transaction_range = transaction_range
            .into_bitmap()
            .change_context(StoreError::Indexing)?;

        index
            .add_index::<IndexTransactionByMessageIndex>(
                transaction_range,
                transaction_by_message_index,
            )
            .change_context(StoreError::Indexing)?;
        index
            .add_index::<IndexMessageByTransactionIndex>(
                message_range.clone(),
                message_by_transaction_index,
            )
            .change_context(StoreError::Indexing)?;
        index
            .add_index::<IndexMessageByFromAddress>(message_range.clone(), message_by_from_address)
            .change_context(StoreError::Indexing)?;
        index
            .add_index::<IndexMessageByToAddress>(message_range.clone(), message_by_to_address)
            .change_context(StoreError::Indexing)?;

        Ok(())
    }

    fn index_transactions(
        &self,
        index: &mut IndexGroup,
        transaction_range: RoaringBitmap,
    ) -> Result<(), StoreError> {
        let mut transaction_by_type = BitmapMapBuilder::default();

        for tx in self.transactions.iter() {
            let transaction_index = tx.transaction_index();

            let tx_type = match tx {
                Transaction::InvokeTransactionV0(_) => TransactionType::InvokeTransactionV0,
                Transaction::InvokeTransactionV1(_) => TransactionType::InvokeTransactionV1,
                Transaction::InvokeTransactionV3(_) => TransactionType::InvokeTransactionV3,

                Transaction::L1HandlerTransaction(_) => TransactionType::L1HandlerTransaction,
                Transaction::DeployTransaction(_) => TransactionType::DeployTransaction,

                Transaction::DeclareTransactionV0(_) => TransactionType::DeclareTransactionV0,
                Transaction::DeclareTransactionV1(_) => TransactionType::DeclareTransactionV1,
                Transaction::DeclareTransactionV2(_) => TransactionType::DeclareTransactionV2,
                Transaction::DeclareTransactionV3(_) => TransactionType::DeclareTransactionV3,

                Transaction::DeployAccountTransactionV1(_) => {
                    TransactionType::DeployAccountTransactionV1
                }
                Transaction::DeployAccountTransactionV3(_) => {
                    TransactionType::DeployAccountTransactionV3
                }
            };

            transaction_by_type.entry(tx_type).insert(transaction_index);
        }

        let transaction_range = transaction_range
            .into_bitmap()
            .change_context(StoreError::Indexing)?;

        let transaction_by_type = transaction_by_type
            .into_bitmap_map()
            .change_context(StoreError::Indexing)?;

        index
            .add_index::<IndexTransactionByType>(transaction_range, transaction_by_type)
            .change_context(StoreError::Indexing)?;

        Ok(())
    }
}
