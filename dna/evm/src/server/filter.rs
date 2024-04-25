use std::collections::{BTreeMap, HashSet};

use apibara_dna_protocol::evm;
use flatbuffers::ForwardsUOffset;

use crate::segment::{
    store::{self, SingleBlock},
    BlockSegment, BlockSegmentReaderOptions,
};

pub struct SegmentFilter {
    inner: evm::Filter,
}

impl SegmentFilter {
    pub fn new(inner: evm::Filter) -> Self {
        Self { inner }
    }

    pub fn has_required_header(&self) -> bool {
        self.inner
            .header
            .as_ref()
            .and_then(|h| h.always)
            .unwrap_or(false)
    }

    pub fn has_withdrawals(&self) -> bool {
        !self.inner.withdrawals.is_empty()
    }

    pub fn has_transactions(&self) -> bool {
        !self.inner.transactions.is_empty()
    }

    pub fn has_logs(&self) -> bool {
        !self.inner.logs.is_empty()
    }

    pub fn transactions(&self) -> impl Iterator<Item = &evm::TransactionFilter> {
        self.inner.transactions.iter()
    }

    pub fn logs(&self) -> impl Iterator<Item = &evm::LogFilter> {
        self.inner.logs.iter()
    }

    pub fn withdrawals(&self) -> impl Iterator<Item = &evm::WithdrawalFilter> {
        self.inner.withdrawals.iter()
    }

    pub fn block_segment_reader_options(&self) -> BlockSegmentReaderOptions {
        // Logs are needed if:
        // - There's at least one log filter.
        // - There's at least one transaction filter that requires logs.
        let needs_logs = self.has_logs() || self.transactions().any(|tx| tx.include_logs());

        // Receipts are needed if:
        // - There's at least one transaction filter that requires receipts.
        // - Theres' at least one log filter that requires receipts.
        let needs_receipts = self.logs().any(|log| log.include_receipt())
            || self.transactions().any(|tx| tx.include_receipt());

        // Transactions are needed if:
        // - There's at least one transaction filter.
        // - There's at least one log filter that requires transactions.
        let needs_transactions =
            self.has_transactions() || self.logs().any(|log| log.include_transaction());

        BlockSegmentReaderOptions {
            needs_logs,
            needs_receipts,
            needs_transactions,
        }
    }

    pub fn filter_segment_block_data<'a>(
        &self,
        relative_index: usize,
        block_segment: &'a BlockSegment<'a>,
    ) -> Option<evm::Block> {
        let header = block_segment
            .header
            .headers()
            .unwrap_or_default()
            .get(relative_index);

        let block_transactions = block_segment
            .transactions
            .as_ref()
            .map(|s| s.blocks().unwrap_or_default().get(relative_index))
            .and_then(|b| b.transactions());
        let block_logs = block_segment
            .logs
            .as_ref()
            .map(|s| s.blocks().unwrap_or_default().get(relative_index))
            .and_then(|b| b.logs());
        let block_receipts = block_segment
            .receipts
            .as_ref()
            .map(|s| s.blocks().unwrap_or_default().get(relative_index))
            .and_then(|b| b.receipts());

        let work_item = WorkItem {
            header,
            transactions: block_transactions,
            logs: block_logs,
            receipts: block_receipts,
        };

        work_item.filter_block(&self)
    }

    pub fn filter_single_block_data<'a>(&self, block: &SingleBlock<'a>) -> Option<evm::Block> {
        let header = block.header().expect("missing header");
        let work_item = WorkItem {
            header,
            transactions: block.transactions(),
            logs: block.logs(),
            receipts: block.receipts(),
        };
        work_item.filter_block(&self)
    }
}

struct WorkItem<'a> {
    header: store::BlockHeader<'a>,
    transactions: Option<flatbuffers::Vector<'a, ForwardsUOffset<store::Transaction<'a>>>>,
    logs: Option<flatbuffers::Vector<'a, ForwardsUOffset<store::Log<'a>>>>,
    receipts: Option<flatbuffers::Vector<'a, ForwardsUOffset<store::TransactionReceipt<'a>>>>,
}

impl<'a> WorkItem<'a> {
    pub fn filter_block(&self, filter: &SegmentFilter) -> Option<evm::Block> {
        let mut withdrawals = Vec::new();
        if filter.has_withdrawals() {
            'withdrawal: for (withdrawal_index, withdrawal) in self
                .header
                .withdrawals()
                .unwrap_or_default()
                .iter()
                .enumerate()
            {
                let withdrawal_address: evm::Address = withdrawal
                    .address()
                    .expect("withdrawal must have address")
                    .into();
                let withdrawal_validator = withdrawal.validator_index();

                for withdrawal_filter in filter.withdrawals() {
                    let should_include_by_address = withdrawal_filter
                        .address
                        .as_ref()
                        .map(|addr| addr == &withdrawal_address);

                    let should_include_by_validator = withdrawal_filter
                        .validator_index
                        .as_ref()
                        .map(|validator| *validator == withdrawal_validator);

                    let should_include =
                        match (should_include_by_address, should_include_by_validator) {
                            (None, None) => true,
                            (Some(a), Some(b)) => a && b,
                            (Some(a), None) => a,
                            (None, Some(b)) => b,
                        };

                    if should_include {
                        let mut withdrawal: evm::Withdrawal = withdrawal.into();
                        withdrawal.withdrawal_index = withdrawal_index as u64;
                        withdrawals.push(withdrawal);
                        continue 'withdrawal;
                    }
                }
            }
        }

        // Since transactions, logs, and receipts can reference each other, we need to
        // keep track of the indices of data we need to include in the response.
        let mut required_transactions: HashSet<u64> = HashSet::new();
        let mut required_logs: HashSet<u64> = HashSet::new();
        let mut required_receipts: HashSet<u64> = HashSet::new();

        let mut transactions = BTreeMap::new();

        if filter.has_transactions() {
            let block_transactions = self.transactions.expect("missing transaction data");

            for (transaction_index, transaction) in block_transactions.iter().enumerate() {
                let transaction_index = transaction_index as u64;
                let from = transaction
                    .from()
                    .expect("transaction must have from")
                    .into();
                let to = transaction.to().map(Into::into);

                for transaction_filter in filter.transactions() {
                    if transactions.contains_key(&transaction_index) {
                        if transaction_filter.include_logs.unwrap_or(false) {
                            required_logs.insert(transaction_index);
                        }
                        if transaction_filter.include_receipt.unwrap_or(false) {
                            required_receipts.insert(transaction_index);
                        }

                        continue;
                    }

                    let should_include_by_from =
                        transaction_filter.from.as_ref().map(|f| f == &from);

                    let should_include_by_to = match (&transaction_filter.to, &to) {
                        (None, _) => None,
                        (Some(a), Some(b)) => Some(a == b),
                        _ => Some(false),
                    };

                    let should_include = match (should_include_by_from, should_include_by_to) {
                        (None, None) => true,
                        (Some(a), Some(b)) => a && b,
                        (Some(a), None) => a,
                        (None, Some(b)) => b,
                    };

                    if should_include {
                        let t: evm::Transaction = transaction.into();
                        assert_eq!(t.transaction_index, transaction_index);
                        transactions.insert(transaction_index, t);

                        if transaction_filter.include_logs.unwrap_or(false) {
                            required_logs.insert(transaction_index);
                        }
                        if transaction_filter.include_receipt.unwrap_or(false) {
                            required_receipts.insert(transaction_index);
                        }

                        continue;
                    }
                }
            }
        }

        let mut logs = BTreeMap::<u64, _>::new();
        if filter.has_logs() {
            let block_logs = self.logs.expect("missing log data");

            for log in block_logs.iter() {
                let address = log.address().expect("log must have address").into();
                let topics: Vec<evm::B256> = log
                    .topics()
                    .unwrap_or_default()
                    .iter()
                    .map(Into::into)
                    .collect();

                for log_filter in filter.logs() {
                    if logs.contains_key(&log.log_index()) {
                        if log_filter.include_transaction.unwrap_or(false) {
                            required_transactions.insert(log.transaction_index());
                        }

                        if log_filter.include_receipt.unwrap_or(false) {
                            required_receipts.insert(log.transaction_index());
                        }
                        continue;
                    }

                    let should_include_by_address =
                        log_filter.address.as_ref().map(|addr| addr == &address);

                    let should_include_by_topics =
                        {
                            if log_filter.topics.is_empty() {
                                None
                            } else if log_filter.topics.len() > topics.len() {
                                Some(false)
                            } else {
                                Some(log_filter.topics.iter().zip(topics.iter()).all(|(f, t)| {
                                    f.value.as_ref().map(|fv| fv == t).unwrap_or(true)
                                }))
                            }
                        };

                    let should_include = match (should_include_by_address, should_include_by_topics)
                    {
                        (None, None) => true,
                        (Some(a), Some(b)) => a && b,
                        (Some(a), None) => a,
                        (None, Some(b)) => b,
                    };

                    if should_include {
                        let l: evm::Log = log.into();
                        logs.insert(log.log_index(), l);

                        if log_filter.include_transaction.unwrap_or(false) {
                            required_transactions.insert(log.transaction_index());
                        }

                        if log_filter.include_receipt.unwrap_or(false) {
                            required_receipts.insert(log.transaction_index());
                        }

                        continue;
                    }
                }
            }
        }

        // Fill additional data required by the filters.
        for transaction_index in required_transactions {
            let block_transactions = self.transactions.expect("missing transaction data");
            for transaction in block_transactions.iter() {
                let should_include = transaction.transaction_index() == transaction_index
                    && !transactions.contains_key(&transaction_index);
                if should_include {
                    let t: evm::Transaction = transaction.into();
                    transactions.insert(transaction_index, t);
                }
            }
        }

        for transaction_index in required_logs {
            let block_logs = self.logs.expect("missing log data");
            for log in block_logs.iter() {
                let should_include = log.transaction_index() == transaction_index
                    && !logs.contains_key(&log.log_index());
                if should_include {
                    let l: evm::Log = log.into();
                    logs.insert(log.log_index(), l);
                }
            }
        }

        let mut receipts = BTreeMap::<u64, _>::new();
        for transaction_index in required_receipts {
            let block_receipts = self.receipts.expect("missing receipt data");
            for receipt in block_receipts.iter() {
                if receipt.transaction_index() == transaction_index {
                    let r: evm::TransactionReceipt = receipt.into();
                    receipts.insert(transaction_index, r);
                }
            }
        }

        let should_send_block = filter.has_required_header()
            | ((transactions.len() > 0)
                | (logs.len() > 0)
                | (receipts.len() > 0)
                | (withdrawals.len() > 0));

        if should_send_block {
            Some(evm::Block {
                header: Some(self.header.into()),
                withdrawals,
                transactions: transactions.into_values().collect(),
                logs: logs.into_values().collect(),
                receipts: receipts.into_values().collect(),
            })
        } else {
            None
        }
    }
}
