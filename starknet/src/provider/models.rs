use apibara_dna_common::{Cursor, Hash};
pub use starknet::core::types::{
    Felt as FieldElement, MaybePendingBlockWithReceipts, MaybePendingBlockWithTxHashes,
    MaybePendingStateUpdate,
};

pub trait BlockExt {
    fn is_finalized(&self) -> bool;
    fn cursor(&self) -> Option<Cursor>;
}

impl BlockExt for MaybePendingBlockWithTxHashes {
    fn is_finalized(&self) -> bool {
        let MaybePendingBlockWithTxHashes::Block(block) = self else {
            return false;
        };

        block.status == starknet::core::types::BlockStatus::AcceptedOnL1
    }

    fn cursor(&self) -> Option<Cursor> {
        let MaybePendingBlockWithTxHashes::Block(block) = self else {
            return None;
        };

        let number = block.block_number;
        let hash = block.block_hash.to_bytes_be().to_vec();

        Cursor::new(number, Hash(hash)).into()
    }
}
