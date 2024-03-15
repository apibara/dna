use std::sync::Arc;

use apibara_core::starknet::v1alpha2;
use apibara_node::{
    async_trait,
    server::RequestMeter,
    stream::{BatchProducer, StreamConfiguration, StreamError},
};
use tracing::debug_span;

use crate::{core::GlobalBlockId, db::StorageReader};

/// A [BatchProducer] that reads data from the database.
pub struct DbBatchProducer<R>
where
    R: StorageReader + Send + Sync + 'static,
{
    storage: Arc<R>,
    inner: Vec<InnerProducer<R>>,
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
            inner: Vec::default(),
            storage,
        }
    }

    fn block_data<M: RequestMeter>(
        &self,
        block_id: &GlobalBlockId,
        meter: &M,
    ) -> Result<Vec<v1alpha2::Block>, R::Error> {
        let mut blocks = Vec::default();
        let is_multi_filter = self.inner.len() > 1;
        let mut all_empty = true;
        for inner in &self.inner {
            if let Some(block) = inner.block_data(block_id, meter)? {
                all_empty = false;
                blocks.push(block);
            } else if is_multi_filter {
                // push an empty block to keep the order of the blocks.
                let empty = v1alpha2::Block {
                    empty: true,
                    ..Default::default()
                };
                blocks.push(empty);
            }
        }

        // If all blocks are empty, return an empty vector to skip this batch.
        if all_empty {
            Ok(Vec::default())
        } else {
            Ok(blocks)
        }
    }
}

impl<R> InnerProducer<R>
where
    R: StorageReader + Send + Sync + 'static,
{
    #[tracing::instrument(skip(self, meter), level = "debug")]
    fn block_data<M: RequestMeter>(
        &self,
        block_id: &GlobalBlockId,
        meter: &M,
    ) -> Result<Option<v1alpha2::Block>, R::Error> {
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
            empty: false,
        };

        if has_data {
            // emit here so that weak headers are not counted
            data_counter.update_meter(meter);

            Ok(Some(data))
        } else {
            Ok(None)
        }
    }

    #[tracing::instrument(skip(self), level = "debug")]
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

    #[tracing::instrument(skip(self, meter), level = "debug")]
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

    #[tracing::instrument(skip(self, meter), level = "debug")]
    fn transactions(
        &self,
        block_id: &GlobalBlockId,
        meter: &mut DataCounter,
    ) -> Result<Vec<v1alpha2::TransactionWithReceipt>, R::Error> {
        if self.filter.transactions.is_empty() {
            return Ok(Vec::default());
        }

        let transactions = self.storage.read_body(block_id)?;
        let mut receipts = self.storage.read_receipts(block_id)?;

        assert!(transactions.len() == receipts.len());
        receipts.sort_by(|a, b| a.transaction_index.cmp(&b.transaction_index));

        let transactions_with_receipts: Vec<_> = transactions
            .into_iter()
            .zip(receipts.into_iter())
            .flat_map(|(tx, rx)| {
                if self.filter_transaction(&tx, rx.execution_status) {
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

    #[tracing::instrument(skip(self, meter), level = "debug")]
    fn events(
        &self,
        block_id: &GlobalBlockId,
        meter: &mut DataCounter,
    ) -> Result<Vec<v1alpha2::EventWithTransaction>, R::Error> {
        if self.filter.events.is_empty() {
            return Ok(Vec::default());
        }

        let transactions = self.storage.read_body(block_id)?;
        let mut receipts = self.storage.read_receipts(block_id)?;

        assert!(transactions.len() == receipts.len());
        receipts.sort_by(|a, b| a.transaction_index.cmp(&b.transaction_index));

        let filter_events_span = debug_span!(
            "filter_events",
            block_id = %block_id,
            event_filters = %self.filter.events.len()
        );
        let events = filter_events_span.in_scope(|| {
            let mut events = Vec::default();
            for receipt in &receipts {
                let transaction = &transactions[receipt.transaction_index as usize];
                for event in &receipt.events {
                    if let Some(filter) = self.filter_event(event, receipt.execution_status) {
                        let transaction = if filter.include_transaction.unwrap_or(true) {
                            Some(transaction.clone())
                        } else {
                            None
                        };

                        let receipt = if filter.include_receipt.unwrap_or(true) {
                            Some(receipt.clone())
                        } else {
                            None
                        };

                        let event = event.clone();

                        events.push(v1alpha2::EventWithTransaction {
                            event: Some(event),
                            transaction,
                            receipt,
                        });
                    }
                }
            }
            events
        });

        meter.event = events.len();

        Ok(events)
    }

    #[tracing::instrument(skip(self, meter), level = "debug")]
    fn l2_to_l1_messages(
        &self,
        block_id: &GlobalBlockId,
        meter: &mut DataCounter,
    ) -> Result<Vec<v1alpha2::L2ToL1MessageWithTransaction>, R::Error> {
        if self.filter.messages.is_empty() {
            return Ok(Vec::default());
        }

        let transactions = self.storage.read_body(block_id)?;
        let mut receipts = self.storage.read_receipts(block_id)?;

        assert!(transactions.len() == receipts.len());
        receipts.sort_by(|a, b| a.transaction_index.cmp(&b.transaction_index));

        let mut messages = Vec::default();
        for receipt in &receipts {
            let transaction = &transactions[receipt.transaction_index as usize];
            for message in &receipt.l2_to_l1_messages {
                if self.filter_l2_to_l1_message(message, receipt.execution_status) {
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

    #[tracing::instrument(skip(self, meter), level = "debug")]
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

        let storage_diffs = self.storage_diffs(block_id, filter)?;
        has_value |= !storage_diffs.is_empty();
        meter.storage_diff = storage_diffs.len();

        let declared_contracts: Vec<_> = state_diff
            .declared_contracts
            .into_iter()
            .filter(|d| self.filter_declared_contracts(d, filter))
            .collect();
        has_value |= !declared_contracts.is_empty();
        meter.declared_contract = declared_contracts.len();

        let declared_classes: Vec<_> = state_diff
            .declared_classes
            .into_iter()
            .filter(|d| self.filter_declared_classes(d, filter))
            .collect();
        has_value |= !declared_classes.is_empty();
        meter.declared_class = declared_classes.len();

        let deployed_contracts: Vec<_> = state_diff
            .deployed_contracts
            .into_iter()
            .filter(|d| self.filter_deployed_contracts(d, filter))
            .collect();
        has_value |= !deployed_contracts.is_empty();
        meter.deployed_contract = deployed_contracts.len();

        let replaced_classes: Vec<_> = state_diff
            .replaced_classes
            .into_iter()
            .filter(|d| self.filter_replaced_classes(d, filter))
            .collect();
        has_value |= !replaced_classes.is_empty();
        meter.replaced_class = replaced_classes.len();

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
                declared_classes,
                deployed_contracts,
                replaced_classes,
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

    fn filter_transaction(&self, tx: &v1alpha2::Transaction, tx_status: i32) -> bool {
        self.filter.transactions.iter().any(|f| {
            let include_if_success_or_reverted =
                tx_status != v1alpha2::ExecutionStatus::Reverted as i32 || f.include_reverted;
            include_if_success_or_reverted && f.matches(tx)
        })
    }

    fn filter_event(
        &self,
        event: &v1alpha2::Event,
        tx_status: i32,
    ) -> Option<&v1alpha2::EventFilter> {
        self.filter.events.iter().find(|f| {
            let include_if_success_or_reverted =
                tx_status != v1alpha2::ExecutionStatus::Reverted as i32 || f.include_reverted();
            include_if_success_or_reverted && f.matches(event)
        })
    }

    fn filter_l2_to_l1_message(&self, message: &v1alpha2::L2ToL1Message, tx_status: i32) -> bool {
        self.filter.messages.iter().any(|f| {
            let include_if_success_or_reverted =
                tx_status != v1alpha2::ExecutionStatus::Reverted as i32 || f.include_reverted;
            include_if_success_or_reverted && f.matches(message)
        })
    }

    fn storage_diffs(
        &self,
        block_id: &GlobalBlockId,
        filter: &v1alpha2::StateUpdateFilter,
    ) -> Result<Vec<v1alpha2::StorageDiff>, R::Error> {
        if filter.storage_diffs.is_empty() {
            return Ok(Vec::default());
        }

        let all_storage_diffs_requested = filter
            .storage_diffs
            .iter()
            .any(|f| f.contract_address.is_none());

        if all_storage_diffs_requested {
            let diffs = self.storage.read_all_storage_diff(block_id)?;
            Ok(diffs)
        } else {
            let mut diffs = Vec::default();
            for storage_filter in &filter.storage_diffs {
                if let Some(contract_address) = &storage_filter.contract_address {
                    if let Some(diff) =
                        self.storage.read_storage_diff(block_id, contract_address)?
                    {
                        diffs.push(diff);
                    }
                }
            }
            Ok(diffs)
        }
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

    fn filter_declared_classes(
        &self,
        declared_class: &v1alpha2::DeclaredClass,
        filter: &v1alpha2::StateUpdateFilter,
    ) -> bool {
        filter
            .declared_classes
            .iter()
            .any(|f| f.matches(declared_class))
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

    fn filter_replaced_classes(
        &self,
        replaced_class: &v1alpha2::ReplacedClass,
        filter: &v1alpha2::StateUpdateFilter,
    ) -> bool {
        filter
            .replaced_classes
            .iter()
            .any(|f| f.matches(replaced_class))
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
    pub declared_class: usize,
    pub deployed_contract: usize,
    pub replaced_class: usize,
    pub nonce_update: usize,
}

impl DataCounter {
    pub fn update_meter<M: RequestMeter>(&self, meter: &M) {
        meter.increment_counter("header", self.header as u64);
        meter.increment_counter("transaction", self.transaction as u64);
        meter.increment_counter("event", self.event as u64);
        meter.increment_counter("message", self.message as u64);
        meter.increment_counter("storage_diff", self.storage_diff as u64);
        meter.increment_counter("declared_contract", self.declared_contract as u64);
        meter.increment_counter("declared_class", self.declared_class as u64);
        meter.increment_counter("deployed_contract", self.deployed_contract as u64);
        meter.increment_counter("replaced_class", self.replaced_class as u64);
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
        let mut new_inner = Vec::default();
        for filter in &configuration.filter {
            let inner = InnerProducer {
                storage: self.storage.clone(),
                filter: filter.clone(),
            };
            new_inner.push(inner);
        }
        self.inner = new_inner;
        Ok(())
    }

    async fn next_batch<M: RequestMeter>(
        &mut self,
        cursors: impl Iterator<Item = Self::Cursor> + Send + Sync,
        meter: &M,
    ) -> Result<Vec<Self::Block>, StreamError> {
        let mut batch = Vec::default();
        for cursor in cursors {
            let blocks = self
                .block_data(&cursor, meter)
                .map_err(StreamError::internal)?;
            batch.extend(blocks);
        }
        Ok(batch)
    }
}
