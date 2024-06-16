use apibara_dna_protocol::starknet;

use crate::segment::store;

impl From<&store::BlockHeader> for starknet::BlockHeader {
    fn from(value: &store::BlockHeader) -> Self {
        let block_hash = (&value.block_hash).into();
        let parent_block_hash = (&value.parent_block_hash).into();
        let sequencer_address = (&value.sequencer_address).into();
        let new_root = (&value.new_root).into();
        let timestamp = prost_types::Timestamp {
            seconds: value.timestamp as i64,
            nanos: 0,
        };

        starknet::BlockHeader {
            block_hash: Some(block_hash),
            parent_block_hash: Some(parent_block_hash),
            block_number: value.block_number,
            sequencer_address: Some(sequencer_address),
            new_root: Some(new_root),
            timestamp: Some(timestamp),
            starknet_version: value.starknet_version.clone(),
            ..Default::default()
        }
    }
}

impl From<&store::Event> for starknet::Event {
    fn from(value: &store::Event) -> Self {
        let from_address = (&value.from_address).into();
        let keys = value
            .keys
            .iter()
            .map(starknet::FieldElement::from)
            .collect();

        // TODO
        starknet::Event {
            from_address: Some(from_address),
            keys,
            event_index: value.event_index as u64,
        }
    }
}

impl From<&store::MessageToL1> for starknet::MessageToL1 {
    fn from(value: &store::MessageToL1) -> Self {
        starknet::MessageToL1::default()
    }
}

impl From<&store::Transaction> for starknet::Transaction {
    fn from(value: &store::Transaction) -> Self {
        starknet::Transaction::default()
    }
}

impl From<&store::TransactionReceipt> for starknet::TransactionReceipt {
    fn from(value: &store::TransactionReceipt) -> Self {
        starknet::TransactionReceipt::default()
    }
}

impl From<&store::FieldElement> for starknet::FieldElement {
    fn from(value: &store::FieldElement) -> Self {
        starknet::FieldElement::from_bytes(&value.0)
    }
}

impl From<starknet::FieldElement> for store::FieldElement {
    fn from(value: starknet::FieldElement) -> Self {
        store::FieldElement(value.to_bytes())
    }
}
