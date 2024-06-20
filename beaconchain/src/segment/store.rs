use std::collections::BTreeMap;

use rkyv::{with::AsVec, Archive, Deserialize, Serialize};

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
    // TODO: fill them in with the real fields.
    pub proposer_index: bool,
    pub parent_root: bool,
    pub state_root: bool,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct Transaction {
    pub transaction_index: u32,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct Validator {
    pub validator_index: u32,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct Blob {}

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct SingleBlock {
    pub header: BlockHeader,
    pub transactions: Vec<Transaction>,
    pub validators: Vec<Validator>,
    pub blobs: Vec<Blob>,
}
