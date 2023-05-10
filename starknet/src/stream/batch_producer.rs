use std::sync::Arc;

use apibara_core::starknet::v1alpha2;
use apibara_node::{
    async_trait,
    server::RequestMeter,
    stream::{BatchCursor, BatchProducer, StreamConfiguration, StreamError},
};
use tracing::trace;

use crate::{core::GlobalBlockId, db::StorageReader};

/// A [BatchProducer] that reads data from the database.
pub struct DbBatchProducer<R>
where
    R: StorageReader + Send + Sync + 'static,
{
    storage: Arc<R>,
    inner: Option<InnerProducer<R>>,
}

struct InnerProducer<R>
where
    R: StorageReader + Send + Sync + 'static,
{
    storage: Arc<R>,
    filter: v1alpha2::Filter,
}

impl<R> DbBatchProducer<R>
where
    R: StorageReader + Send + Sync + 'static,
{
    pub fn new(storage: Arc<R>) -> Self {
        DbBatchProducer {
            inner: None,
            storage,
        }
    }

    fn block_data(&self, block_id: &GlobalBlockId) -> Result<Option<v1alpha2::Block>, R::Error> {
        match self.inner {
            None => Ok(None),
            Some(ref inner) => inner.block_data(block_id),
        }
    }
}

impl<R> InnerProducer<R>
where
    R: StorageReader + Send + Sync + 'static,
{
    fn block_data(&self, block_id: &GlobalBlockId) -> Result<Option<v1alpha2::Block>, R::Error> {
        let mut has_data = false;

        let mut data_counter = DataCounter::default();
        let status = self.status(block_id)?;

        let header = self.header(block_id, &mut data_counter)?;
        if !self.has_weak_header() {
            has_data |= header.is_some();
        }

        let transactions = self.transactions(block_id, &mut data_counter)?;
        has_data |= !transactions.is_empty();

        let events = self.events(block_id, &mut data_counter)?;
        has_data |= !events.is_empty();

        let l2_to_l1_messages = self.l2_to_l1_messages(block_id, &mut data_counter)?;
        has_data |= !l2_to_l1_messages.is_empty();

        let state_update = self.state_update(block_id, &mut data_counter)?;
        has_data |= state_update.is_some();

        let data = v1alpha2::Block {
            status: status as i32,
            header,
            state_update,
            transactions,
            events,
            l2_to_l1_messages,
        };

        if has_data {
            // emit here so that weak headers are not counted
            // data_counter.update_meter(meter);

            Ok(Some(data))
        } else {
            Ok(None)
        }
    }

    fn status(&self, block_id: &GlobalBlockId) -> Result<v1alpha2::BlockStatus, R::Error> {
        let status = self
            .storage
            .read_status(block_id)?
            .unwrap_or(v1alpha2::BlockStatus::Unspecified);
        Ok(status)
    }

    fn has_weak_header(&self) -> bool {
        // No header is the same as a weak header.
        self.filter.header.as_ref().map(|h| h.weak).unwrap_or(true)
    }

    fn header(
        &self,
        block_id: &GlobalBlockId,
        meter: &mut DataCounter,
    ) -> Result<Option<v1alpha2::BlockHeader>, R::Error> {
        if self.filter.header.is_some() {
            meter.header = 1;
            self.storage.read_header(block_id)
        } else {
            Ok(None)
        }
    }

    fn transactions(
        &self,
        block_id: &GlobalBlockId,
        meter: &mut DataCounter,
    ) -> Result<Vec<v1alpha2::TransactionWithReceipt>, R::Error> {
        if self.filter.transactions.is_empty() {
            return Ok(Vec::default());
        }

        let transactions = self.storage.read_body(block_id)?;
        let (mut receipts, _) = self.storage.read_receipts(block_id)?;

        assert!(transactions.len() == receipts.len());
        receipts.sort_by(|a, b| a.transaction_index.cmp(&b.transaction_index));

        let transactions_with_receipts: Vec<_> = transactions
            .into_iter()
            .zip(receipts.into_iter())
            .flat_map(|(tx, rx)| {
                if self.filter_transaction(&tx) {
                    Some(v1alpha2::TransactionWithReceipt {
                        transaction: Some(tx),
                        receipt: Some(rx),
                    })
                } else {
                    None
                }
            })
            .collect();

        meter.transaction = transactions_with_receipts.len();

        Ok(transactions_with_receipts)
    }

    fn events(
        &self,
        block_id: &GlobalBlockId,
        meter: &mut DataCounter,
    ) -> Result<Vec<v1alpha2::EventWithTransaction>, R::Error> {
        if self.filter.events.is_empty() {
            return Ok(Vec::default());
        }

        let transactions = self.storage.read_body(block_id)?;
        let (mut receipts, bloom) = self.storage.read_receipts(block_id)?;

        // quickly check if any event would match using bloom filter
        if let Some(bloom) = bloom {
            let mut has_match = false;
            for filter in &self.filter.events {
                match &filter.from_address {
                    None => {
                        // an empty filter matches any address
                        has_match = true;
                        break;
                    }
                    Some(address) => {
                        if bloom.check(address) {
                            has_match = true;
                            break;
                        }
                    }
                }
                for key in &filter.keys {
                    if bloom.check(key) {
                        has_match = true;
                        break;
                    }
                }
            }

            // bail out early
            if !has_match {
                trace!("bloom did not match any event.");
                return Ok(Vec::default());
            }
        }

        assert!(transactions.len() == receipts.len());
        receipts.sort_by(|a, b| a.transaction_index.cmp(&b.transaction_index));

        let mut events = Vec::default();
        for receipt in &receipts {
            let transaction = &transactions[receipt.transaction_index as usize];
            for event in &receipt.events {
                if self.filter_event(event) {
                    let transaction = transaction.clone();
                    let receipt = receipt.clone();
                    let event = event.clone();

                    events.push(v1alpha2::EventWithTransaction {
                        transaction: Some(transaction),
                        receipt: Some(receipt),
                        event: Some(event),
                    });
                }
            }
        }

        meter.event = events.len();

        Ok(events)
    }

    fn l2_to_l1_messages(
        &self,
        block_id: &GlobalBlockId,
        meter: &mut DataCounter,
    ) -> Result<Vec<v1alpha2::L2ToL1MessageWithTransaction>, R::Error> {
        if self.filter.messages.is_empty() {
            return Ok(Vec::default());
        }

        let transactions = self.storage.read_body(block_id)?;
        let (mut receipts, _) = self.storage.read_receipts(block_id)?;

        assert!(transactions.len() == receipts.len());
        receipts.sort_by(|a, b| a.transaction_index.cmp(&b.transaction_index));

        let mut messages = Vec::default();
        for receipt in &receipts {
            let transaction = &transactions[receipt.transaction_index as usize];
            for message in &receipt.l2_to_l1_messages {
                if self.filter_l2_to_l1_message(message) {
                    let transaction = transaction.clone();
                    let receipt = receipt.clone();
                    let message = message.clone();

                    messages.push(v1alpha2::L2ToL1MessageWithTransaction {
                        transaction: Some(transaction),
                        receipt: Some(receipt),
                        message: Some(message),
                    });
                }
            }
        }

        meter.message = messages.len();

        Ok(messages)
    }

    fn state_update(
        &self,
        block_id: &GlobalBlockId,
        meter: &mut DataCounter,
    ) -> Result<Option<v1alpha2::StateUpdate>, R::Error> {
        let filter = if let Some(filter) = self.filter.state_update.as_ref() {
            filter
        } else {
            return Ok(None);
        };

        let original_state_update =
            if let Some(update) = self.storage.read_state_update(block_id)? {
                update
            } else {
                return Ok(None);
            };

        let state_diff = if let Some(diff) = original_state_update.state_diff {
            diff
        } else {
            return Ok(None);
        };

        let mut has_value = false;

        let storage_diffs: Vec<_> = state_diff
            .storage_diffs
            .into_iter()
            .filter(|diff| self.filter_storage_diff(diff, filter))
            .collect();
        has_value |= !storage_diffs.is_empty();
        meter.storage_diff = storage_diffs.len();

        let declared_contracts: Vec<_> = state_diff
            .declared_contracts
            .into_iter()
            .filter(|d| self.filter_declared_contracts(d, filter))
            .collect();
        has_value |= !declared_contracts.is_empty();
        meter.declared_contract = declared_contracts.len();

        let deployed_contracts: Vec<_> = state_diff
            .deployed_contracts
            .into_iter()
            .filter(|d| self.filter_deployed_contracts(d, filter))
            .collect();
        has_value |= !deployed_contracts.is_empty();
        meter.deployed_contract = deployed_contracts.len();

        let nonces: Vec<_> = state_diff
            .nonces
            .into_iter()
            .filter(|n| self.filter_nonces(n, filter))
            .collect();
        has_value |= !nonces.is_empty();
        meter.nonce_update = nonces.len();

        if has_value {
            let diff = v1alpha2::StateDiff {
                storage_diffs,
                declared_contracts,
                deployed_contracts,
                nonces,
            };
            let state_update = v1alpha2::StateUpdate {
                new_root: original_state_update.new_root,
                old_root: original_state_update.old_root,
                state_diff: Some(diff),
            };
            Ok(Some(state_update))
        } else {
            Ok(None)
        }
    }

    fn filter_transaction(&self, tx: &v1alpha2::Transaction) -> bool {
        self.filter.transactions.iter().any(|f| f.matches(tx))
    }

    fn filter_event(&self, event: &v1alpha2::Event) -> bool {
        self.filter.events.iter().any(|f| f.matches(event))
    }

    fn filter_l2_to_l1_message(&self, message: &v1alpha2::L2ToL1Message) -> bool {
        self.filter.messages.iter().any(|f| f.matches(message))
    }

    fn filter_storage_diff(
        &self,
        diff: &v1alpha2::StorageDiff,
        filter: &v1alpha2::StateUpdateFilter,
    ) -> bool {
        filter.storage_diffs.iter().any(|f| f.matches(diff))
    }

    fn filter_declared_contracts(
        &self,
        declared_contract: &v1alpha2::DeclaredContract,
        filter: &v1alpha2::StateUpdateFilter,
    ) -> bool {
        filter
            .declared_contracts
            .iter()
            .any(|f| f.matches(declared_contract))
    }

    fn filter_deployed_contracts(
        &self,
        deployed_contract: &v1alpha2::DeployedContract,
        filter: &v1alpha2::StateUpdateFilter,
    ) -> bool {
        filter
            .deployed_contracts
            .iter()
            .any(|f| f.matches(deployed_contract))
    }

    fn filter_nonces(
        &self,
        nonce: &v1alpha2::NonceUpdate,
        filter: &v1alpha2::StateUpdateFilter,
    ) -> bool {
        filter.nonces.iter().any(|f| f.matches(nonce))
    }
}

#[derive(Debug, Default)]
struct DataCounter {
    pub header: usize,
    pub transaction: usize,
    pub event: usize,
    pub message: usize,
    pub storage_diff: usize,
    pub declared_contract: usize,
    pub deployed_contract: usize,
    pub nonce_update: usize,
}

impl DataCounter {
    pub fn update_meter<M: RequestMeter>(&self, meter: &Arc<M>) {
        meter.increment_counter("header", self.header as u64);
        meter.increment_counter("transaction", self.transaction as u64);
        meter.increment_counter("event", self.event as u64);
        meter.increment_counter("message", self.message as u64);
        meter.increment_counter("storage_diff", self.storage_diff as u64);
        meter.increment_counter("declared_contract", self.declared_contract as u64);
        meter.increment_counter("deployed_contract", self.deployed_contract as u64);
        meter.increment_counter("nonce_update", self.nonce_update as u64);
    }
}

#[async_trait]
impl<R> BatchProducer for DbBatchProducer<R>
where
    R: StorageReader + Send + Sync + 'static,
{
    type Cursor = GlobalBlockId;
    type Filter = v1alpha2::Filter;
    type Block = v1alpha2::Block;

    fn reconfigure(
        &mut self,
        configuration: &StreamConfiguration<Self::Cursor, Self::Filter>,
    ) -> Result<(), StreamError> {
        let new_inner = InnerProducer {
            storage: self.storage.clone(),
            filter: configuration.filter.clone(),
        };
        self.inner = Some(new_inner);
        Ok(())
    }

    async fn next_batch(
        &mut self,
        cursors: impl Iterator<Item = Self::Cursor> + Send + Sync,
    ) -> Result<Vec<Self::Block>, StreamError> {
        let batch: Vec<_> = cursors
            .flat_map(|cursor| self.block_data(&cursor).transpose())
            .collect::<Result<Vec<_>, _>>()
            .map_err(StreamError::internal)?;
        Ok(batch)
    }
}
