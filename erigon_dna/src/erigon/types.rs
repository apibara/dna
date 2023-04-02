use ethers_core::types::{H160, H256};

pub use reth_primitives::Header;

/// Unique block id.
#[derive(Clone, Debug, Copy, Default)]
pub struct GlobalBlockId(pub(crate) u64, pub(crate) H256);

/// Unique log id.
#[derive(Clone, Debug, Copy, Default)]
pub struct LogId(pub(crate) u64, pub(crate) u32);

/// Block hash.
#[derive(Clone, Debug, Copy, Default)]
pub struct BlockHash(pub(crate) H256);

#[derive(Clone, Debug, Default)]
pub struct LogTopicIndex {
    pub log_topic: Vec<u8>,
    pub shard: [u8; 2],
}

#[derive(Clone, Debug, Default)]
pub struct LogAddressIndex {
    pub log_address: H160,
    pub block: u64,
}

#[derive(Clone, Debug, Default)]
pub struct TransactionLog {
    pub logs: Vec<Log>,
}

#[derive(Clone, Debug, Default)]
pub struct Log {
    pub address: H160,
    pub topics: Vec<H256>,
    pub data: Vec<u8>,
}

/// Fork choice.
#[derive(Clone, Debug, Copy)]
pub enum Forkchoice {
    HeadBlockHash,
    SafeBlockHash,
    FinalizedBlockHash,
}

impl GlobalBlockId {
    pub fn new(number: u64, hash: H256) -> Self {
        GlobalBlockId(number, hash)
    }
}
