//! Filter block data.

use crate::core::pb::starknet::v1alpha2::*;

trait VecMatch {
    fn prefix_matches(&self, other: &Self) -> bool;
}

impl<T> VecMatch for Vec<T>
where
    T: PartialEq,
{
    fn prefix_matches(&self, other: &Self) -> bool {
        if self.is_empty() {
            return true;
        }

        if self.len() > other.len() {
            return false;
        }

        for (a, b) in self.iter().zip(other) {
            if a != b {
                return false;
            }
        }

        true
    }
}

/// [Option] extension trait to match values. `None` matches anything.
trait FilterMatch {
    fn matches(&self, other: &Self) -> bool;
}

impl FilterMatch for Option<FieldElement> {
    fn matches(&self, other: &Self) -> bool {
        if self.is_none() {
            return true;
        }
        self == other
    }
}

impl TransactionFilter {
    pub fn matches(&self, tx: &Transaction) -> bool {
        match self.filter.as_ref() {
            None => true,
            Some(transaction_filter::Filter::InvokeV0(filter)) => filter.matches(tx),
            Some(transaction_filter::Filter::InvokeV1(filter)) => filter.matches(tx),
            Some(transaction_filter::Filter::Deploy(filter)) => filter.matches(tx),
            Some(transaction_filter::Filter::Declare(filter)) => filter.matches(tx),
            Some(transaction_filter::Filter::L1Handler(filter)) => filter.matches(tx),
            Some(transaction_filter::Filter::DeployAccount(filter)) => filter.matches(tx),
        }
    }
}

impl InvokeTransactionV0Filter {
    pub fn matches(&self, tx: &Transaction) -> bool {
        match tx.transaction.as_ref() {
            Some(transaction::Transaction::InvokeV0(tx)) => {
                self.contract_address.matches(&tx.contract_address)
                    && self.entry_point_selector.matches(&tx.entry_point_selector)
                    && self.calldata.prefix_matches(&tx.calldata)
            }
            _ => false,
        }
    }
}

impl InvokeTransactionV1Filter {
    pub fn matches(&self, tx: &Transaction) -> bool {
        match tx.transaction.as_ref() {
            Some(transaction::Transaction::InvokeV1(tx)) => {
                self.sender_address.matches(&tx.sender_address)
                    && self.calldata.prefix_matches(&tx.calldata)
            }
            _ => false,
        }
    }
}

impl DeployTransactionFilter {
    pub fn matches(&self, tx: &Transaction) -> bool {
        match tx.transaction.as_ref() {
            Some(transaction::Transaction::Deploy(tx)) => {
                self.class_hash.matches(&tx.class_hash)
                    && self
                        .contract_address_salt
                        .matches(&tx.contract_address_salt)
                    && self
                        .constructor_calldata
                        .prefix_matches(&tx.constructor_calldata)
            }
            _ => false,
        }
    }
}

impl DeclareTransactionFilter {
    pub fn matches(&self, tx: &Transaction) -> bool {
        match tx.transaction.as_ref() {
            Some(transaction::Transaction::Declare(tx)) => {
                self.class_hash.matches(&tx.class_hash)
                    && self.sender_address.matches(&tx.sender_address)
            }
            _ => false,
        }
    }
}

impl L1HandlerTransactionFilter {
    pub fn matches(&self, tx: &Transaction) -> bool {
        match tx.transaction.as_ref() {
            Some(transaction::Transaction::L1Handler(tx)) => {
                self.contract_address.matches(&tx.contract_address)
                    && self.entry_point_selector.matches(&tx.entry_point_selector)
                    && self.calldata.prefix_matches(&tx.calldata)
            }
            _ => false,
        }
    }
}

impl DeployAccountTransactionFilter {
    pub fn matches(&self, tx: &Transaction) -> bool {
        match tx.transaction.as_ref() {
            Some(transaction::Transaction::DeployAccount(tx)) => {
                self.class_hash.matches(&tx.class_hash)
                    && self
                        .contract_address_salt
                        .matches(&tx.contract_address_salt)
                    && self
                        .constructor_calldata
                        .prefix_matches(&tx.constructor_calldata)
            }
            _ => false,
        }
    }
}

impl EventFilter {
    pub fn matches(&self, event: &Event) -> bool {
        self.from_address.matches(&event.from_address)
            && self.keys.prefix_matches(&event.keys)
            && self.data.prefix_matches(&event.data)
    }
}

impl L2ToL1MessageFilter {
    pub fn matches(&self, message: &L2ToL1Message) -> bool {
        self.to_address.matches(&message.to_address)
            && self.payload.prefix_matches(&message.payload)
    }
}

impl StorageDiffFilter {
    pub fn matches(&self, storage_diff: &StorageDiff) -> bool {
        self.contract_address
            .matches(&storage_diff.contract_address)
    }
}

impl DeclaredContractFilter {
    pub fn matches(&self, declared_contract: &DeclaredContract) -> bool {
        self.class_hash.matches(&declared_contract.class_hash)
    }
}

impl DeployedContractFilter {
    pub fn matches(&self, deployed_contract: &DeployedContract) -> bool {
        self.contract_address
            .matches(&deployed_contract.contract_address)
            && self.class_hash.matches(&deployed_contract.class_hash)
    }
}

impl NonceUpdateFilter {
    pub fn matches(&self, nonce: &NonceUpdate) -> bool {
        self.contract_address.matches(&nonce.contract_address) && self.nonce.matches(&nonce.nonce)
    }
}
