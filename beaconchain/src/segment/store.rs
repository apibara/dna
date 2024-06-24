use std::collections::BTreeMap;

use apibara_dna_common::segment::store::{Bitmap, BlockData, Segment};
use rkyv::{with::AsVec, Archive, Deserialize, Serialize};

use crate::ingestion::models;

#[derive(
    Archive, Serialize, Deserialize, Debug, PartialEq, Clone, Copy, Default, PartialOrd, Eq, Ord,
)]
#[archive(check_bytes)]
pub struct Address(pub [u8; 20]);

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct B256(pub [u8; 32]);

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct U256(pub [u8; 32]);

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct B384(pub [u8; 48]);

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct Bytes(pub Vec<u8>);

/// A beacon chain slot.
#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub enum Slot<T> {
    Missed,
    Proposed(T),
}

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
    pub execution_payload: ExecutionPayload,
    pub blob_kzg_commitments: Vec<B384>,
}

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

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct Signature {
    pub r: U256,
    pub s: U256,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct AccessListItem {
    pub address: Address,
    pub storage_keys: Vec<B256>,
}

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

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct Blob {
    pub index: u32,
    pub blob: Bytes,
    pub kzg_commitment: B384,
    pub kzg_proof: B384,
    pub kzg_commitment_inclusion_proof: Vec<B256>,
    pub hash: B256,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct SingleBlock {
    pub header: BlockHeader,
    pub transactions: Vec<Transaction>,
    pub validators: Vec<Validator>,
    pub blobs: Vec<Blob>,
}

pub type BlockHeaderSegment = Segment<Slot<BlockHeader>>;
pub type TransactionSegment = Segment<Slot<BlockData<Transaction>>>;
pub type ValidatorSegment = Segment<Slot<BlockData<Validator>>>;
pub type BlobSegment = Segment<Slot<BlockData<Blob>>>;

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct SegmentGroupIndex {
    #[with(AsVec)]
    pub transaction_by_from_address: BTreeMap<Address, Bitmap>,
    #[with(AsVec)]
    pub transaction_by_to_address: BTreeMap<Address, Bitmap>,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct SegmentGroup {
    pub index: SegmentGroupIndex,
}
