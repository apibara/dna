use apibara_dna_protocol::evm;

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
}
