//! Aggregate data for one block.

use std::sync::Arc;

use crate::{
    core::{pb::starknet::v1alpha2, GlobalBlockId},
    db::StorageReader,
};

pub trait BlockDataAggregator {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Returns a `Block` with data for the given block.
    ///
    /// If there is no data for the given block, it returns `None`.
    fn aggregate_for_block(
        &self,
        block_id: &GlobalBlockId,
    ) -> Result<Option<v1alpha2::Block>, Self::Error>;
}

pub struct DatabaseBlockDataAggregator<R: StorageReader> {
    storage: Arc<R>,
    filter: v1alpha2::Filter,
}

impl<R> DatabaseBlockDataAggregator<R>
where
    R: StorageReader,
{
    pub fn new(storage: Arc<R>, filter: v1alpha2::Filter) -> Self {
        DatabaseBlockDataAggregator { storage, filter }
    }

    fn status(&self, block_id: &GlobalBlockId) -> Result<v1alpha2::BlockStatus, R::Error> {
        let status = self
            .storage
            .read_status(block_id)?
            .unwrap_or(v1alpha2::BlockStatus::Unspecified);
        Ok(status)
    }

    fn header(&self, block_id: &GlobalBlockId) -> Result<Option<v1alpha2::BlockHeader>, R::Error> {
        if self.filter.header.is_some() {
            self.storage.read_header(&block_id)
        } else {
            Ok(None)
        }
    }

    fn transactions(
        &self,
        block_id: &GlobalBlockId,
    ) -> Result<Vec<v1alpha2::TransactionWithReceipt>, R::Error> {
        if self.filter.transactions.len() == 0 {
            return Ok(Vec::default());
        }

        /*
        let transactions = self
            .storage
            .read_body(block_id)?
            .into_iter()
            .filter(|tx| self.filter_transaction(tx))
            .collect();

        Ok(transactions)
        */
        todo!()
    }

    fn receipts(
        &self,
        block_id: &GlobalBlockId,
    ) -> Result<Vec<v1alpha2::TransactionReceipt>, R::Error> {
        /*
        if self.filter.receipts.len() == 0 {
            return Ok(Vec::default());
        }
        */
        Ok(Vec::default())
    }

    fn events(
        &self,
        block_id: &GlobalBlockId,
    ) -> Result<Vec<v1alpha2::EventWithTransaction>, R::Error> {
        if self.filter.events.len() == 0 {
            return Ok(Vec::default());
        }

        /*
        let receipts = self.storage.read_receipts(block_id)?;
        let mut events = Vec::default();
        for receipt in receipts {
            for event in receipt.events {
                for filter in &self.filter.events {
                    if filter.matches(&event) {
                        events.push(event);
                        break;
                    }
                }
            }
        }
        Ok(events)
        */
        todo!()
    }

    fn l2_to_l1_messages(
        &self,
        block_id: &GlobalBlockId,
    ) -> Result<Vec<v1alpha2::L2ToL1MessageWithTransaction>, R::Error> {
        Ok(Vec::default())
    }

    fn state_update(
        &self,
        block_id: &GlobalBlockId,
    ) -> Result<Option<v1alpha2::StateUpdate>, R::Error> {
        // TODO: change state update flag to be a filter
        /*
        if self.filter.include_state_update {
            self.storage.read_state_update(&block_id)
        } else {
            Ok(None)
        }
        */
        Ok(None)
    }

    fn filter_transaction(&self, tx: &v1alpha2::Transaction) -> bool {
        for filter in &self.filter.transactions {
            if filter.matches(tx) {
                return true;
            }
        }
        false
    }
}

impl<R> BlockDataAggregator for DatabaseBlockDataAggregator<R>
where
    R: StorageReader,
{
    type Error = R::Error;

    #[tracing::instrument(level = "trace", skip(self))]
    fn aggregate_for_block(
        &self,
        block_id: &GlobalBlockId,
    ) -> Result<Option<v1alpha2::Block>, Self::Error> {
        let mut has_data = false;

        let status = self.status(block_id)?;

        let header = self.header(block_id)?;
        has_data |= header.is_some();

        let transactions = self.transactions(block_id)?;
        has_data |= !transactions.is_empty();

        let receipts = self.receipts(block_id)?;
        has_data |= !receipts.is_empty();

        let events = self.events(block_id)?;
        has_data |= !events.is_empty();

        let l2_to_l1_messages = self.l2_to_l1_messages(block_id)?;
        has_data |= !l2_to_l1_messages.is_empty();

        let state_update = self.state_update(block_id)?;
        has_data |= state_update.is_some();

        let data = v1alpha2::Block {
            status: status as i32,
            header,
            state_update,
            transactions,
            receipts,
            events,
            l2_to_l1_messages,
        };

        if has_data {
            Ok(Some(data))
        } else {
            Ok(None)
        }
    }
}
