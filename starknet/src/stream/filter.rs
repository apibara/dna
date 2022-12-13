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
        if self.len() == 0 {
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

        return true;
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
                self.contract_address == tx.contract_address
                    && self.entry_point_selector == tx.entry_point_selector
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
                self.sender_address == tx.sender_address
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
                self.class_hash == tx.class_hash
                    && self.contract_address_salt == tx.contract_address_salt
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
                self.class_hash == tx.class_hash && self.sender_address == tx.sender_address
            }
            _ => false,
        }
    }
}

impl L1HandlerTransactionFilter {
    pub fn matches(&self, tx: &Transaction) -> bool {
        match tx.transaction.as_ref() {
            Some(transaction::Transaction::L1Handler(tx)) => {
                self.contract_address == tx.contract_address
                    && self.entry_point_selector == tx.entry_point_selector
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
                self.class_hash == tx.class_hash
                    && self.contract_address_salt == tx.contract_address_salt
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
        self.from_address == event.from_address
            && self.keys.prefix_matches(&event.keys)
            && self.data.prefix_matches(&event.data)
    }
}
