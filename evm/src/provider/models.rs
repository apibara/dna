pub use alloy_primitives::{Address, Bloom, B256, U128, U256};
pub use alloy_rpc_types::{
    AccessListItem, Block, Header, Log, Signature, Transaction, TransactionReceipt, Withdrawal,
};
use apibara_dna_common::{chain::BlockInfo, Cursor, Hash};

pub type BlockWithTxHashes = Block<B256>;

pub trait BlockExt {
    fn cursor(&self) -> Cursor;
    fn block_info(&self) -> BlockInfo;
}

impl<T> BlockExt for Block<T> {
    fn cursor(&self) -> Cursor {
        let number = self.header.number;
        let hash = self.header.hash;
        Cursor::new(number, Hash(hash.to_vec()))
    }

    fn block_info(&self) -> BlockInfo {
        let number = self.header.number;
        let hash = self.header.hash;
        let parent = self.header.parent_hash;

        BlockInfo {
            number,
            hash: Hash(hash.to_vec()),
            parent: Hash(parent.to_vec()),
        }
    }
}
