use ethers_core::types::H256;

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
