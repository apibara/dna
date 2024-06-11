use apibara_dna_protocol::starknet;

use crate::segment::SegmentReaderOptions;

pub struct SegmentFilter {
    inner: starknet::Filter,
}

impl SegmentFilter {
    pub fn new(inner: starknet::Filter) -> Self {
        Self { inner }
    }

    pub fn has_required_header(&self) -> bool {
        self.inner
            .header
            .as_ref()
            .and_then(|h| h.always)
            .unwrap_or(false)
    }

    pub fn has_transactions(&self) -> bool {
        !self.inner.transactions.is_empty()
    }

    pub fn has_events(&self) -> bool {
        !self.inner.events.is_empty()
    }

    pub fn has_messages(&self) -> bool {
        !self.inner.messages.is_empty()
    }

    pub fn transactions(&self) -> impl Iterator<Item = &starknet::TransactionFilter> {
        self.inner.transactions.iter()
    }

    pub fn events(&self) -> impl Iterator<Item = &starknet::EventFilter> {
        self.inner.events.iter()
    }

    pub fn segment_reader_options(&self) -> SegmentReaderOptions {
        // Logs are needed if:
        // - There's at least one log filter.
        // - There's at least one transaction filter that requires logs.
        let needs_events = self.has_events() || self.transactions().any(|tx| tx.include_events());

        // Receipts are needed if:
        // - There's at least one transaction filter that requires receipts.
        // - Theres' at least one log filter that requires receipts.
        // let needs_receipts = self.logs().any(|log| log.include_receipt())
        //     || self.transactions().any(|tx| tx.include_receipt());
        let needs_receipts = false;

        // Transactions are needed if:
        // - There's at least one transaction filter.
        // - There's at least one log filter that requires transactions.
        let needs_transactions =
            self.has_transactions() || self.events().any(|log| log.include_transaction());

        SegmentReaderOptions {
            needs_transactions,
            needs_receipts,
            needs_events,
        }
    }
}
