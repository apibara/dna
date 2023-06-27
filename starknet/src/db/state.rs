//! State update data.

use apibara_core::starknet::v1alpha2;
use apibara_node::db::{KeyDecodeError, Table, TableKey};

use crate::core::GlobalBlockId;

/// Store state updates without storage diffs.
#[derive(Debug, Clone, Copy, Default)]
pub struct StateUpdateTable {}

/// Store storage diffs for a given block.
#[derive(Debug, Clone, Copy, Default)]
pub struct StorageDiffTable {}

impl Table for StateUpdateTable {
    type Key = GlobalBlockId;
    type Value = v1alpha2::StateUpdate;

    fn db_name() -> &'static str {
        "StateUpdate"
    }
}

pub struct StorageDiffKey {
    pub block_id: GlobalBlockId,
    pub contract_address: v1alpha2::FieldElement,
}

impl Table for StorageDiffTable {
    type Key = StorageDiffKey;
    type Value = v1alpha2::StorageDiff;

    fn db_name() -> &'static str {
        "StorageDiff"
    }
}

impl TableKey for StorageDiffKey {
    type Encoded = [u8; 72];

    fn encode(&self) -> Self::Encoded {
        let mut out = [0; 72];
        out[..40].copy_from_slice(&self.block_id.encode());
        out[40..].copy_from_slice(&self.contract_address.to_bytes());
        out
    }

    fn decode(b: &[u8]) -> Result<Self, apibara_node::db::KeyDecodeError> {
        let block_id = GlobalBlockId::decode(&b[..40])?;

        let contract_address = v1alpha2::FieldElement::from_slice(&b[40..]).map_err(|_| {
            KeyDecodeError::InvalidByteSize {
                expected: 72,
                actual: b.len(),
            }
        })?;

        Ok(StorageDiffKey {
            block_id,
            contract_address,
        })
    }
}
