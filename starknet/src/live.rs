use std::collections::HashMap;

use apibara_dna_common::fragment::{Block, HeaderFragment, IndexGroupFragment, JoinGroupFragment};
use apibara_dna_common::ingestion::IngestionError;
use apibara_dna_protocol::starknet;
use error_stack::Result;
use prost::Message;

use crate::{
    ingestion::{
        collect_block_body_and_index, collect_receipts_body_and_index,
        collect_state_update_body_and_index,
    },
    provider::{models, NewTransactionMessage, NewTransactionReceiptMessage, StarknetLiveMessage},
};

/// Result of inserting a live transaction or receipt into the assembler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveAssemblerInsert {
    Inserted,
    Updated,
    Duplicate,
}

/// A pending block fragment assembled from live Starknet websocket data.
#[derive(Debug)]
pub struct LivePendingBlock {
    pub block_number: u64,
    pub transaction_hashes: Vec<models::FieldElement>,
    pub block: Block,
}

/// Correlates live Starknet transaction and receipt websocket notifications.
#[derive(Debug, Default)]
pub struct StarknetLiveAssembler {
    entries: HashMap<models::FieldElement, LiveEntry>,
    next_arrival_order: u64,
}

#[derive(Debug, Clone)]
struct LiveEntry {
    transaction: Option<models::TransactionWithL2Status>,
    receipt: Option<models::TransactionReceiptWithBlockInfo>,
    block_number: Option<u64>,
    transaction_index: Option<u64>,
    arrival_order: u64,
}

impl StarknetLiveAssembler {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_message(&mut self, message: StarknetLiveMessage) -> LiveAssemblerInsert {
        match message {
            StarknetLiveMessage::Transaction(message) => self.push_transaction(message),
            StarknetLiveMessage::Receipt(message) => self.push_receipt(message),
        }
    }

    pub fn push_transaction(&mut self, message: NewTransactionMessage) -> LiveAssemblerInsert {
        self.push_transaction_with_meta(
            message.transaction,
            message.block_number,
            message.transaction_index,
        )
    }

    pub fn push_transaction_with_meta(
        &mut self,
        transaction: models::TransactionWithL2Status,
        block_number: Option<u64>,
        transaction_index: Option<u64>,
    ) -> LiveAssemblerInsert {
        let transaction_hash = *transaction.txn.transaction_hash();
        let was_new = !self.entries.contains_key(&transaction_hash);
        let entry = self.entry_mut(transaction_hash);

        let mut changed = entry.merge_position(block_number, transaction_index);
        if entry.transaction.is_none() {
            entry.transaction = Some(transaction);
            changed = true;
        }

        insertion_result(was_new, changed)
    }

    pub fn push_receipt(&mut self, message: NewTransactionReceiptMessage) -> LiveAssemblerInsert {
        self.push_receipt_with_meta(message.receipt, message.transaction_index)
    }

    pub fn push_receipt_with_meta(
        &mut self,
        receipt: models::TransactionReceiptWithBlockInfo,
        transaction_index: Option<u64>,
    ) -> LiveAssemblerInsert {
        let transaction_hash = *receipt.receipt.transaction_hash();
        let block_number = Some(receipt.block.block_number());
        let was_new = !self.entries.contains_key(&transaction_hash);
        let entry = self.entry_mut(transaction_hash);

        let mut changed = entry.merge_position(block_number, transaction_index);
        if entry.receipt.is_none() {
            entry.receipt = Some(receipt);
            changed = true;
        }

        insertion_result(was_new, changed)
    }

    pub fn pending_block_numbers(&self) -> Vec<u64> {
        let mut block_numbers = self
            .entries
            .values()
            .filter_map(|entry| {
                entry
                    .receipt
                    .as_ref()
                    .map(|receipt| receipt.block.block_number())
            })
            .collect::<Vec<_>>();
        block_numbers.sort_unstable();
        block_numbers.dedup();
        block_numbers
    }

    pub fn prune_through_block(&mut self, block_number: u64) {
        self.entries.retain(|_, entry| {
            entry
                .receipt
                .as_ref()
                .map(|receipt| receipt.block.block_number() > block_number)
                .unwrap_or(true)
        });
    }

    pub fn build_pending_block(
        &self,
        block_number: u64,
    ) -> Result<Option<LivePendingBlock>, IngestionError> {
        let entries = self.ordered_entries(block_number);
        if entries.is_empty() {
            return Ok(None);
        }

        let transaction_hashes = entries
            .iter()
            .map(|entry| *entry.receipt.as_ref().unwrap().receipt.transaction_hash())
            .collect::<Vec<_>>();

        let body_ingestion_result = if entries.iter().all(|entry| entry.transaction.is_some()) {
            let transactions = entries
                .iter()
                .map(|entry| {
                    let transaction = entry.transaction.as_ref().unwrap().clone();
                    let receipt = entry.receipt.as_ref().unwrap().receipt.clone();
                    models::TransactionWithReceipt {
                        transaction: transaction.txn.into(),
                        receipt,
                    }
                })
                .collect::<Vec<_>>();

            collect_block_body_and_index(&transactions, &[])?
        } else {
            let receipts = entries
                .iter()
                .map(|entry| entry.receipt.as_ref().unwrap().receipt.clone())
                .collect::<Vec<_>>();

            collect_receipts_body_and_index(&receipts)?
        };

        let state_update_ingestion_result =
            collect_state_update_body_and_index(&empty_state_diff())?;

        let mut body_fragments = body_ingestion_result.body;
        let mut index_fragments = body_ingestion_result.index;
        let mut join_fragments = body_ingestion_result.join;

        body_fragments.extend(state_update_ingestion_result.body);
        index_fragments.extend(state_update_ingestion_result.index);
        join_fragments.extend(state_update_ingestion_result.join);

        let header = starknet::BlockHeader {
            block_number,
            ..Default::default()
        };

        Ok(Some(LivePendingBlock {
            block_number,
            transaction_hashes,
            block: Block {
                header: HeaderFragment {
                    data: header.encode_to_vec(),
                },
                index: IndexGroupFragment {
                    indexes: index_fragments,
                },
                body: body_fragments,
                join: JoinGroupFragment {
                    joins: join_fragments,
                },
            },
        }))
    }

    fn entry_mut(&mut self, transaction_hash: models::FieldElement) -> &mut LiveEntry {
        if !self.entries.contains_key(&transaction_hash) {
            let arrival_order = self.next_arrival_order;
            self.next_arrival_order += 1;
            self.entries.insert(
                transaction_hash,
                LiveEntry {
                    transaction: None,
                    receipt: None,
                    block_number: None,
                    transaction_index: None,
                    arrival_order,
                },
            );
        }

        self.entries.get_mut(&transaction_hash).unwrap()
    }

    fn ordered_entries(&self, block_number: u64) -> Vec<&LiveEntry> {
        let mut entries = self
            .entries
            .values()
            .filter(|entry| {
                entry
                    .receipt
                    .as_ref()
                    .is_some_and(|receipt| receipt.block.block_number() == block_number)
            })
            .collect::<Vec<_>>();

        entries.sort_by_key(|entry| entry.order_key());

        entries
    }
}

impl LiveEntry {
    fn merge_position(
        &mut self,
        block_number: Option<u64>,
        transaction_index: Option<u64>,
    ) -> bool {
        let mut changed = false;

        if self.block_number.is_none() && block_number.is_some() {
            self.block_number = block_number;
            changed = true;
        }

        if self.transaction_index.is_none() && transaction_index.is_some() {
            self.transaction_index = transaction_index;
            changed = true;
        }

        changed
    }

    fn position(&self) -> Option<(u64, u64)> {
        self.block_number.zip(self.transaction_index)
    }

    fn order_key(&self) -> (u8, u64, u64, u64) {
        match self.position() {
            Some((block_number, transaction_index)) => {
                (0, block_number, transaction_index, self.arrival_order)
            }
            None => (1, 0, 0, self.arrival_order),
        }
    }
}

fn insertion_result(was_uncorrelated: bool, changed: bool) -> LiveAssemblerInsert {
    match (was_uncorrelated, changed) {
        (_, false) => LiveAssemblerInsert::Duplicate,
        (true, true) => LiveAssemblerInsert::Inserted,
        (false, true) => LiveAssemblerInsert::Updated,
    }
}

fn empty_state_diff() -> models::StateDiff {
    models::StateDiff {
        storage_diffs: Vec::new(),
        deprecated_declared_classes: Vec::new(),
        declared_classes: Vec::new(),
        migrated_compiled_classes: None,
        deployed_contracts: Vec::new(),
        replaced_classes: Vec::new(),
        nonces: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fragment::{RECEIPT_FRAGMENT_ID, TRANSACTION_FRAGMENT_ID};
    use starknet_rust::core::types::{
        ExecutionResources, FeePayment, InvokeTransaction, InvokeTransactionReceipt,
        InvokeTransactionV1, PriceUnit, TransactionFinalityStatus,
    };

    fn felt(value: u64) -> models::FieldElement {
        models::FieldElement::from_hex(&format!("0x{value:x}")).unwrap()
    }

    fn transaction(hash: u64) -> models::TransactionWithL2Status {
        models::TransactionWithL2Status {
            txn: models::Transaction::Invoke(InvokeTransaction::V1(InvokeTransactionV1 {
                transaction_hash: felt(hash),
                sender_address: felt(1),
                calldata: Vec::new(),
                max_fee: felt(0),
                signature: Vec::new(),
                nonce: felt(0),
            })),
            finality_status: models::L2TransactionStatus::PreConfirmed,
        }
    }

    fn receipt(hash: u64, block_number: u64) -> models::TransactionReceiptWithBlockInfo {
        models::TransactionReceiptWithBlockInfo {
            receipt: models::TransactionReceipt::Invoke(InvokeTransactionReceipt {
                transaction_hash: felt(hash),
                actual_fee: FeePayment {
                    amount: felt(0),
                    unit: PriceUnit::Fri,
                },
                finality_status: TransactionFinalityStatus::PreConfirmed,
                messages_sent: Vec::new(),
                events: Vec::new(),
                execution_resources: ExecutionResources {
                    l1_gas: 0,
                    l1_data_gas: 0,
                    l2_gas: 0,
                },
                execution_result: models::ExecutionResult::Succeeded,
            }),
            block: models::ReceiptBlock::PreConfirmed { block_number },
        }
    }

    fn body_fragment(
        block: &Block,
        fragment_id: u8,
    ) -> &apibara_dna_common::fragment::BodyFragment {
        block
            .body
            .iter()
            .find(|fragment| fragment.fragment_id == fragment_id)
            .unwrap()
    }

    fn receipt_hashes(block: &Block) -> Vec<models::FieldElement> {
        body_fragment(block, RECEIPT_FRAGMENT_ID)
            .data
            .iter()
            .map(|data| {
                let receipt = starknet::TransactionReceipt::decode(data.as_slice()).unwrap();
                let hash = receipt.meta.unwrap().transaction_hash.unwrap();
                models::FieldElement::from_bytes_be_slice(hash.to_bytes().as_slice())
            })
            .collect()
    }

    #[test]
    fn correlates_transaction_and_receipt() {
        let mut assembler = StarknetLiveAssembler::new();

        assert_eq!(
            assembler.push_transaction_with_meta(transaction(1), None, None),
            LiveAssemblerInsert::Inserted
        );
        assert_eq!(
            assembler.push_receipt_with_meta(receipt(1, 10), Some(0)),
            LiveAssemblerInsert::Updated
        );

        let block = assembler.build_pending_block(10).unwrap().unwrap().block;
        assert_eq!(body_fragment(&block, TRANSACTION_FRAGMENT_ID).data.len(), 1);
        assert_eq!(body_fragment(&block, RECEIPT_FRAGMENT_ID).data.len(), 1);
    }

    #[test]
    fn emits_receipt_only_block() {
        let mut assembler = StarknetLiveAssembler::new();

        assert_eq!(
            assembler.push_receipt_with_meta(receipt(1, 10), None),
            LiveAssemblerInsert::Inserted
        );

        let pending = assembler.build_pending_block(10).unwrap().unwrap();
        assert_eq!(pending.block_number, 10);
        assert_eq!(
            body_fragment(&pending.block, TRANSACTION_FRAGMENT_ID)
                .data
                .len(),
            0
        );
        assert_eq!(
            body_fragment(&pending.block, RECEIPT_FRAGMENT_ID)
                .data
                .len(),
            1
        );
    }

    #[test]
    fn ignores_duplicate_receipts() {
        let mut assembler = StarknetLiveAssembler::new();

        assert_eq!(
            assembler.push_receipt_with_meta(receipt(1, 10), Some(0)),
            LiveAssemblerInsert::Inserted
        );
        assert_eq!(
            assembler.push_receipt_with_meta(receipt(1, 10), Some(0)),
            LiveAssemblerInsert::Duplicate
        );

        let block = assembler.build_pending_block(10).unwrap().unwrap().block;
        assert_eq!(body_fragment(&block, RECEIPT_FRAGMENT_ID).data.len(), 1);
    }

    #[test]
    fn orders_by_block_transaction_index_when_present() {
        let mut assembler = StarknetLiveAssembler::new();

        assembler.push_receipt_with_meta(receipt(12, 10), Some(2));
        assembler.push_receipt_with_meta(receipt(10, 10), Some(0));
        assembler.push_receipt_with_meta(receipt(11, 10), Some(1));

        let block = assembler.build_pending_block(10).unwrap().unwrap().block;
        assert_eq!(receipt_hashes(&block), vec![felt(10), felt(11), felt(12)]);
    }

    #[test]
    fn falls_back_to_arrival_order_without_transaction_index() {
        let mut assembler = StarknetLiveAssembler::new();

        assembler.push_receipt_with_meta(receipt(12, 10), None);
        assembler.push_receipt_with_meta(receipt(10, 10), None);
        assembler.push_receipt_with_meta(receipt(11, 10), None);

        let block = assembler.build_pending_block(10).unwrap().unwrap().block;
        assert_eq!(receipt_hashes(&block), vec![felt(12), felt(10), felt(11)]);
    }
}
