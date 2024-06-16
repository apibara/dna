use apibara_dna_protocol::starknet;

use crate::segment::store;

pub struct MessageToL1Filter {
    pub to_address: Option<store::FieldElement>,
    pub include_reverted: bool,
    pub include_transaction: bool,
    pub include_receipt: bool,
}

impl MessageToL1Filter {
    pub fn matches(&self, message: &store::MessageToL1) -> bool {
        // If reverted, then we must include reverted messages.
        if !self.include_reverted && message.transaction_reverted {
            return false;
        }

        let Some(to_address) = &self.to_address else {
            return true;
        };

        to_address == &message.to_address
    }
}

impl From<starknet::MessageToL1Filter> for MessageToL1Filter {
    fn from(value: starknet::MessageToL1Filter) -> Self {
        let to_address = value.to_address.map(store::FieldElement::from);

        Self {
            to_address,
            include_reverted: value.include_reverted.unwrap_or(false),
            include_transaction: value.include_transaction.unwrap_or(false),
            include_receipt: value.include_receipt.unwrap_or(false),
        }
    }
}
