use apibara_dna_protocol::starknet;

use crate::segment::store;

pub struct TransactionFilter {
    pub include_reverted: bool,
    pub include_receipt: bool,
    pub include_events: bool,
    pub include_messages: bool,
    pub inner: Option<InnerTransactionFilter>,
}

pub enum InnerTransactionFilter {
    InvokeV0,
    InvokeV1,
    InvokeV3,
    Deploy,
    DeclareV0,
    DeclareV1,
    DeclareV2,
    DeclareV3,
    L1Handler,
    DeployAccountV1,
    DeployAccountV3,
}

impl TransactionFilter {
    pub fn matches(&self, transaction: &store::Transaction) -> bool {
        let meta = transaction.meta();
        // If reverted, then we must include reverted transactions.
        if !self.include_reverted && meta.transaction_reverted {
            return false;
        }

        // If it's an empty filter, then we match everything.
        let Some(inner) = &self.inner else {
            return true;
        };

        inner.matches(transaction)
    }
}

impl InnerTransactionFilter {
    pub fn matches(&self, transaction: &store::Transaction) -> bool {
        use store::Transaction::*;
        use InnerTransactionFilter::*;
        match (self, transaction) {
            (InvokeV0, InvokeTransactionV0(_)) => true,
            (InvokeV1, InvokeTransactionV1(_)) => true,
            (InvokeV3, InvokeTransactionV3(_)) => true,
            (Deploy, DeployTransaction(_)) => true,
            (DeclareV0, DeclareTransactionV0(_)) => true,
            (DeclareV1, DeclareTransactionV1(_)) => true,
            (DeclareV2, DeclareTransactionV2(_)) => true,
            (DeclareV3, DeclareTransactionV3(_)) => true,
            (L1Handler, L1HandlerTransaction(_)) => true,
            (DeployAccountV1, DeployAccountTransactionV1(_)) => true,
            (DeployAccountV3, DeployAccountTransactionV3(_)) => true,
            _ => false,
        }
    }
}

impl From<starknet::TransactionFilter> for TransactionFilter {
    fn from(value: starknet::TransactionFilter) -> Self {
        TransactionFilter {
            include_reverted: value.include_reverted.unwrap_or(false),
            include_receipt: value.include_receipt.unwrap_or(false),
            include_events: value.include_events.unwrap_or(false),
            include_messages: value.include_messages.unwrap_or(false),
            inner: value.inner.map(InnerTransactionFilter::from),
        }
    }
}

impl From<starknet::transaction_filter::Inner> for InnerTransactionFilter {
    fn from(value: starknet::transaction_filter::Inner) -> Self {
        use starknet::transaction_filter::Inner::*;
        match value {
            InvokeV0(inner) => inner.into(),
            InvokeV1(inner) => inner.into(),
            InvokeV3(inner) => inner.into(),
            Deploy(inner) => inner.into(),
            DeclareV0(inner) => inner.into(),
            DeclareV1(inner) => inner.into(),
            DeclareV2(inner) => inner.into(),
            DeclareV3(inner) => inner.into(),
            L1Handler(inner) => inner.into(),
            DeployAccountV1(inner) => inner.into(),
            DeployAccountV3(inner) => inner.into(),
        }
    }
}

impl From<starknet::InvokeTransactionV0Filter> for InnerTransactionFilter {
    fn from(_value: starknet::InvokeTransactionV0Filter) -> Self {
        InnerTransactionFilter::InvokeV0
    }
}

impl From<starknet::InvokeTransactionV1Filter> for InnerTransactionFilter {
    fn from(_value: starknet::InvokeTransactionV1Filter) -> Self {
        InnerTransactionFilter::InvokeV1
    }
}

impl From<starknet::InvokeTransactionV3Filter> for InnerTransactionFilter {
    fn from(_value: starknet::InvokeTransactionV3Filter) -> Self {
        InnerTransactionFilter::InvokeV3
    }
}

impl From<starknet::DeclareV0TransactionFilter> for InnerTransactionFilter {
    fn from(_value: starknet::DeclareV0TransactionFilter) -> Self {
        InnerTransactionFilter::DeclareV0
    }
}

impl From<starknet::DeclareV1TransactionFilter> for InnerTransactionFilter {
    fn from(_value: starknet::DeclareV1TransactionFilter) -> Self {
        InnerTransactionFilter::DeclareV1
    }
}

impl From<starknet::DeclareV2TransactionFilter> for InnerTransactionFilter {
    fn from(_value: starknet::DeclareV2TransactionFilter) -> Self {
        InnerTransactionFilter::DeclareV2
    }
}

impl From<starknet::DeclareV3TransactionFilter> for InnerTransactionFilter {
    fn from(_value: starknet::DeclareV3TransactionFilter) -> Self {
        InnerTransactionFilter::DeclareV3
    }
}

impl From<starknet::L1HandlerTransactionFilter> for InnerTransactionFilter {
    fn from(_value: starknet::L1HandlerTransactionFilter) -> Self {
        InnerTransactionFilter::L1Handler
    }
}

impl From<starknet::DeployTransactionFilter> for InnerTransactionFilter {
    fn from(_value: starknet::DeployTransactionFilter) -> Self {
        InnerTransactionFilter::Deploy
    }
}

impl From<starknet::DeployAccountV1TransactionFilter> for InnerTransactionFilter {
    fn from(_value: starknet::DeployAccountV1TransactionFilter) -> Self {
        InnerTransactionFilter::DeployAccountV1
    }
}

impl From<starknet::DeployAccountV3TransactionFilter> for InnerTransactionFilter {
    fn from(_value: starknet::DeployAccountV3TransactionFilter) -> Self {
        InnerTransactionFilter::DeployAccountV3
    }
}
