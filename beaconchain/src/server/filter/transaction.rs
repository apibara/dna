use apibara_dna_protocol::beaconchain;

use crate::segment::store;

pub struct TransactionFilter {
    pub from: Option<store::Address>,
    pub to: Option<store::Address>,
    pub include_blob: bool,
}

impl TransactionFilter {
    pub fn matches(&self, transaction: &store::ArchivedTransaction) -> bool {
        if self.from.is_none() && self.to.is_none() {
            return true;
        }

        if let Some(from_addr) = &self.from {
            if transaction.from.0 != from_addr.0 {
                return false;
            }
        }

        if let Some(to_addr) = &self.to {
            // If the filter specifies a `to` address but the transaction is a CREATE,
            // then it won't match.
            let Some(tx_to) = transaction.to.as_ref() else {
                return false;
            };
            if tx_to.0 != to_addr.0 {
                return false;
            }
        }

        true
    }
}

impl From<beaconchain::TransactionFilter> for TransactionFilter {
    fn from(x: beaconchain::TransactionFilter) -> Self {
        let from = x.from.map(store::Address::from);
        let to = x.to.map(store::Address::from);
        Self {
            from,
            to,
            include_blob: x.include_blob.unwrap_or(false),
        }
    }
}
