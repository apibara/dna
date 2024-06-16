use apibara_dna_protocol::starknet;

use crate::segment::store;

pub struct MessageToL1Filter {
    pub from_address: Option<store::FieldElement>,
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

        let from_address_match = if let Some(from_address) = &self.from_address {
            from_address == &message.from_address
        } else {
            true
        };

        let to_address_match = if let Some(to_address) = &self.to_address {
            to_address == &message.to_address
        } else {
            true
        };

        from_address_match && to_address_match
    }
}

impl From<starknet::MessageToL1Filter> for MessageToL1Filter {
    fn from(value: starknet::MessageToL1Filter) -> Self {
        let from_address = value.from_address.map(store::FieldElement::from);
        let to_address = value.to_address.map(store::FieldElement::from);

        Self {
            from_address,
            to_address,
            include_reverted: value.include_reverted.unwrap_or(false),
            include_transaction: value.include_transaction.unwrap_or(false),
            include_receipt: value.include_receipt.unwrap_or(false),
        }
    }
}
