//! Filter data for one block.

use std::sync::Arc;

use tracing::{event, Level};

use crate::{
    core::{pb::starknet::v1alpha2, GlobalBlockId},
    db::StorageReader,
};

pub trait BlockDataFilter {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Returns a `Block` with data for the given block.
    ///
    /// If there is no data for the given block, it returns `None`.
    fn data_for_block(
        &self,
        block_id: &GlobalBlockId,
    ) -> Result<Option<v1alpha2::Block>, Self::Error>;
}

pub struct DatabaseBlockDataFilter<R: StorageReader> {
    storage: Arc<R>,
    filter: v1alpha2::Filter,
}

#[derive(Debug, Default)]
struct DataMeter {
    pub header: usize,
    pub transaction: usize,
    pub event: usize,
    pub message: usize,
    pub storage_diff: usize,
    pub declared_contract: usize,
    pub deployed_contract: usize,
    pub nonce_update: usize,
}

impl DataMeter {
    pub fn emit_event(&self) {
        event!(
            Level::INFO,
            data.is_metrics = true,
            data.header = self.header,
            data.transaction = self.transaction,
            data.event = self.event,
            data.message = self.message,
            data.storage_diff = self.storage_diff,
            data.declared_contract = self.declared_contract,
            data.deployed_contract = self.deployed_contract,
            data.nonce_update = self.nonce_update,
        );
    }
}

impl<R> DatabaseBlockDataFilter<R>
where
    R: StorageReader,
{
    pub fn new(storage: Arc<R>, filter: v1alpha2::Filter) -> Self {
        DatabaseBlockDataFilter { storage, filter }
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
        meter: &mut DataMeter,
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
        meter: &mut DataMeter,
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
        meter: &mut DataMeter,
    ) -> Result<Vec<v1alpha2::EventWithTransaction>, R::Error> {
        if self.filter.events.is_empty() {
            return Ok(Vec::default());
        }

        let transactions = self.storage.read_body(block_id)?;
        let receipts = self.storage.read_receipts(block_id)?;

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
        meter: &mut DataMeter,
    ) -> Result<Vec<v1alpha2::L2ToL1MessageWithTransaction>, R::Error> {
        if self.filter.messages.is_empty() {
            return Ok(Vec::default());
        }

        let transactions = self.storage.read_body(block_id)?;
        let receipts = self.storage.read_receipts(block_id)?;

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
        meter: &mut DataMeter,
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

impl<R> BlockDataFilter for DatabaseBlockDataFilter<R>
where
    R: StorageReader,
{
    type Error = R::Error;

    #[tracing::instrument(level = "trace", skip(self))]
    fn data_for_block(
        &self,
        block_id: &GlobalBlockId,
    ) -> Result<Option<v1alpha2::Block>, Self::Error> {
        let mut has_data = false;

        let mut meter = DataMeter::default();
        let status = self.status(block_id)?;

        let header = self.header(block_id, &mut meter)?;
        if !self.has_weak_header() {
            has_data |= header.is_some();
        }

        let transactions = self.transactions(block_id, &mut meter)?;
        has_data |= !transactions.is_empty();

        let events = self.events(block_id, &mut meter)?;
        has_data |= !events.is_empty();

        let l2_to_l1_messages = self.l2_to_l1_messages(block_id, &mut meter)?;
        has_data |= !l2_to_l1_messages.is_empty();

        let state_update = self.state_update(block_id, &mut meter)?;
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
            meter.emit_event();

            Ok(Some(data))
        } else {
            Ok(None)
        }
    }
}
