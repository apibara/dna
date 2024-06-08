use apibara_dna_common::core::Cursor;

use crate::ingestion::models;

pub trait BlockExt {
    fn is_finalized(&self) -> bool;
}

pub trait GetCursor {
    fn cursor(&self) -> Option<Cursor>;
}

pub trait TransactionReceiptExt {
    fn events(&self) -> impl Iterator<Item = &models::Event>;
}

impl GetCursor for models::BlockWithTxs {
    fn cursor(&self) -> Option<Cursor> {
        let number = self.block_number;
        let hash = self.block_hash.to_bytes_be().to_vec();
        Some(Cursor::new(number, hash))
    }
}

impl GetCursor for models::BlockWithTxHashes {
    fn cursor(&self) -> Option<Cursor> {
        let number = self.block_number;
        let hash = self.block_hash.to_bytes_be().to_vec();
        Some(Cursor::new(number, hash))
    }
}

impl GetCursor for models::BlockWithReceipts {
    fn cursor(&self) -> Option<Cursor> {
        let number = self.block_number;
        let hash = self.block_hash.to_bytes_be().to_vec();
        Some(Cursor::new(number, hash))
    }
}

impl GetCursor for models::PendingBlockWithTxHashes {
    fn cursor(&self) -> Option<Cursor> {
        todo!();
    }
}

impl GetCursor for models::MaybePendingBlockWithTxHashes {
    fn cursor(&self) -> Option<Cursor> {
        use models::MaybePendingBlockWithTxHashes::*;
        match self {
            PendingBlock(block) => block.cursor(),
            Block(block) => block.cursor(),
        }
    }
}

impl BlockExt for models::MaybePendingBlockWithTxHashes {
    fn is_finalized(&self) -> bool {
        use models::MaybePendingBlockWithTxHashes::*;
        match self {
            PendingBlock(_) => false,
            Block(block) => block.is_finalized(),
        }
    }
}

impl BlockExt for models::BlockWithTxHashes {
    fn is_finalized(&self) -> bool {
        self.status == models::BlockStatus::AcceptedOnL1
    }
}

impl TransactionReceiptExt for models::TransactionReceipt {
    fn events(&self) -> impl Iterator<Item = &models::Event> {
        vec![].into_iter()
    }
}
