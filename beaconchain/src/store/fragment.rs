//! Store fragments for the beacon chain.

use apibara_dna_common::Hash;
use rkyv::{Archive, Deserialize, Serialize};

use crate::provider::models;

/// A beacon chain slot.
#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub enum Slot<T> {
    Missed { slot: u64 },
    Proposed(T),
}

impl<T: rkyv::Archive> ArchivedSlot<T> {
    pub fn as_proposed(&self) -> Option<&<T as Archive>::Archived> {
        match self {
            ArchivedSlot::Missed { .. } => None,
            ArchivedSlot::Proposed(data) => Some(data),
        }
    }
}

/// An address of 160 bits.
#[derive(Archive, Serialize, Deserialize, PartialEq, Clone, Copy, Default, PartialOrd, Eq, Ord)]
#[archive(check_bytes)]
pub struct Address(pub [u8; 20]);

/// A fixed-size byte array of 32 bytes.
#[derive(Archive, Serialize, Deserialize, Default, PartialEq, Clone, Copy)]
#[archive(check_bytes)]
pub struct B256(pub [u8; 32]);

/// An unsigned integer of 256 bits.
#[derive(Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub struct U256(pub [u8; 32]);

/// A fixed-size byte array of 48 bytes.
#[derive(Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub struct B384(pub [u8; 48]);

/// A variable-size byte array.
#[derive(Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub struct Bytes(pub Vec<u8>);

/// A block header.
#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct BlockHeader {
    pub slot: u64,
    pub proposer_index: u32,
    pub parent_root: B256,
    pub state_root: B256,
    pub randao_reveal: Bytes,
    pub deposit_count: u64,
    pub deposit_root: B256,
    pub block_hash: B256,
    pub graffiti: B256,
    pub execution_payload: Option<ExecutionPayload>,
    pub blob_kzg_commitments: Vec<B384>,
}

/// Execution payload.
#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct ExecutionPayload {
    pub parent_hash: B256,
    pub fee_recipient: Address,
    pub state_root: B256,
    pub receipts_root: B256,
    pub logs_bloom: Bytes,
    pub prev_randao: B256,
    pub block_number: u64,
    pub timestamp: u64,
}

/// A transaction.
#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct Transaction {
    pub transaction_type: u64,
    pub transaction_index: u32,
    pub transaction_hash: B256,
    pub nonce: u64,
    pub from: Address,
    pub to: Option<Address>,
    pub value: U256,
    pub gas_price: Option<u128>,
    pub gas_limit: u128,
    pub max_fee_per_gas: Option<u128>,
    pub max_priority_fee_per_gas: Option<u128>,
    pub max_fee_per_blob_gas: Option<u128>,
    pub input: Bytes,
    pub signature: Signature,
    pub chain_id: Option<u64>,
    pub access_list: Vec<AccessListItem>,
    pub blob_versioned_hashes: Vec<B256>,
}

/// A transaction signature.
#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct Signature {
    pub r: U256,
    pub s: U256,
}

/// A transaction access list item.
#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct AccessListItem {
    pub address: Address,
    pub storage_keys: Vec<B256>,
}

/// A validator.
#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct Validator {
    pub validator_index: u32,
    pub balance: u64,
    pub status: models::ValidatorStatus,
    pub pubkey: B384,
    pub withdrawal_credentials: B256,
    pub effective_balance: u64,
    pub slashed: bool,
    pub activation_eligibility_epoch: u64,
    pub activation_epoch: u64,
    pub exit_epoch: u64,
    pub withdrawable_epoch: u64,
}

/// A blob.
#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct Blob {
    pub blob_index: u32,
    pub blob: Bytes,
    pub kzg_commitment: B384,
    pub kzg_proof: B384,
    pub kzg_commitment_inclusion_proof: Vec<B256>,
    pub blob_hash: B256,
    pub transaction_index: u32,
    pub transaction_hash: B256,
}

impl std::fmt::Debug for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Address(0x{})", hex::encode(self.0))
    }
}

impl std::fmt::Debug for B256 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "B256(0x{})", hex::encode(self.0))
    }
}

impl std::fmt::Debug for U256 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "U256(0x{})", hex::encode(self.0))
    }
}

impl std::fmt::Debug for B384 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "B384(0x{})", hex::encode(self.0))
    }
}

impl std::fmt::Debug for Bytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Bytes(0x{})", hex::encode(&self.0))
    }
}

impl From<B256> for Hash {
    fn from(value: B256) -> Self {
        Hash(value.0.to_vec())
    }
}
