//! # Track the chain's head
use std::{marker::PhantomData, sync::Arc};

use libmdbx::{Environment, EnvironmentKind, Error as MdbxError, Transaction, TransactionKind, RW};

use crate::db::{
    tables::{self, Block, BlockHash},
    KeyDecodeError, MdbxRWTransactionExt, MdbxTransactionExt, TableCursor,
};

/// Track the chain's state.
///
/// The chain tracker is a state machine used to keep track of the chain state,
/// including chain reorganizations.
///
/// The state is grown two ways:
///  - by adding _indexed blocks_ incrementally, starting at block with height 0.
///  - by adding to the _chain's head_. The head is used to know if the chain is
///    completely indexed and to detect reorganizations.
///
/// The difference in height between the chain's head and the most recent indexed block
/// is called _gap_. A gap equals to 0 implies the chain's state is synced.
///
///
/// ```txt
///               /------- gap --------\
///              v                      v
///
///     0  1  2  3            90 91 92 93
///     o--o--o--o             o--o--o--o
///              ^              \-x     ^
///              |                ^     |
///              |                |     |
///              |          uncle block |
///        indexed block               head
/// ```
///
/// The chain tracker must be supplied with blocks since it only handles the chain's state,
/// and doesn't know how to fetch blocks from the chain.
pub struct ChainTracker<B: Block, E: EnvironmentKind> {
    db: Arc<Environment<E>>,
    phantom: PhantomData<B>,
}

/// Change to the chain state.
#[derive(Debug)]
pub enum ChainChange<B: Block> {
    /// Chain advanced by the provided blocks.
    Advance(Vec<B>),
    /// Chain advanced through a reorganization.
    Reorg(Vec<B>),
    /// Need to provide the requested block to make a decision.
    MissingBlock(u64, B::Hash),
    /// The provided block already belongs to the chain.
    AlreadySeen,
}

#[derive(Debug, thiserror::Error)]
pub enum ChainTrackerError {
    #[error("error originating from database")]
    Database(#[from] MdbxError),
    #[error("error decoding key from database")]
    KeyDecodeError(#[from] KeyDecodeError),
    #[error("invalid block number (expected {expected}, given {given})")]
    InvalidBlockNumber { expected: u64, given: u64 },
    #[error("missing chain's head")]
    MissingHead,
    #[error("inconsistent database state")]
    InconsistentState,
}

pub type Result<T> = std::result::Result<T, ChainTrackerError>;

impl<B, E> ChainTracker<B, E>
where
    B: Block,
    E: EnvironmentKind,
{
    /// Creates a new chain tracker, persisting data to the given db.
    pub fn new(db: Arc<Environment<E>>) -> Result<Self> {
        let txn = db.begin_rw_txn()?;
        txn.ensure_table::<tables::BlockTable<B>>(None)?;
        txn.ensure_table::<tables::CanonicalBlockTable<B::Hash>>(None)?;
        txn.commit()?;
        Ok(ChainTracker {
            db,
            phantom: PhantomData::default(),
        })
    }

    /// Returns the height of the most recent head, if any.
    pub fn head_height(&self) -> Result<Option<u64>> {
        let txn = self.db.begin_ro_txn()?;
        let mut block_cursor = txn.open_table::<tables::BlockTable<B>>()?.cursor()?;
        let height = block_cursor.last()?.map(|((height, _), _)| height);
        txn.commit()?;
        Ok(height)
    }

    /// Returns the gap between the indexed block and the head, if any.
    pub fn gap(&self) -> Result<Option<u64>> {
        let indexed = match self.latest_indexed_block()? {
            Some(block) => block.number(),
            None => return Ok(None),
        };
        let height = match self.head_height()? {
            Some(height) => height,
            None => return Ok(None),
        };
        Ok(Some(height - indexed))
    }

    /// Returns a block in the canonical chain by its number or height.
    pub fn block_by_number(&self, number: u64) -> Result<Option<B>> {
        let txn = self.db.begin_ro_txn()?;
        let block =
            self.block_by_func_with_txn(&txn, |canon_cursor| canon_cursor.seek_exact(&number))?;
        txn.commit()?;
        Ok(block)
    }

    /// Returns the most recently indexed block.
    pub fn latest_indexed_block(&self) -> Result<Option<B>> {
        let txn = self.db.begin_ro_txn()?;
        let block = self.block_by_func_with_txn(&txn, |canon_cursor| canon_cursor.last())?;
        txn.commit()?;
        Ok(block)
    }

    /// Updates the state with the new head.
    pub fn update_head(&self, head: &B) -> Result<()> {
        let txn = self.db.begin_rw_txn()?;
        let mut block_cursor = txn.open_table::<tables::BlockTable<B>>()?.cursor()?;
        block_cursor.put(&(head.number(), head.hash().clone()), head)?;
        self.store_block_with_cursor(head, &mut block_cursor)?;
        txn.commit()?;
        Ok(())
    }

    /// Updates the state with a new indexed block.
    ///
    /// This function must be called after at least one head has been
    /// stored, so that the tracker can detect chain reorganizations.
    pub fn update_indexed_block(&self, block: B) -> Result<ChainChange<B>> {
        let _head_height = self.head_height()?.ok_or(ChainTrackerError::MissingHead)?;

        let txn = self.db.begin_rw_txn()?;
        let mut block_cursor = txn.open_table::<tables::BlockTable<B>>()?.cursor()?;
        let mut canon_cursor = txn
            .open_table::<tables::CanonicalBlockTable<B::Hash>>()?
            .cursor()?;

        // store block information to db
        self.store_block_with_cursor(&block, &mut block_cursor)?;

        match self.latest_indexed_block()? {
            None => {
                // first block being indexed, expect it to have height 0
                if block.number() != 0 {
                    return Err(ChainTrackerError::InvalidBlockNumber {
                        expected: 0,
                        given: block.number(),
                    });
                }
                // update canonical chain
                self.update_canonical_chain_with_cursor(&block, &mut canon_cursor)?;
                txn.commit()?;
                Ok(ChainChange::Advance(vec![block]))
            }
            Some(latest_block) => {
                // there is a gap in the chain, request missing block to caller
                if block.number() > latest_block.number() + 1 {
                    txn.commit()?;
                    return Ok(ChainChange::MissingBlock(
                        block.number() - 1,
                        block.parent_hash().clone(),
                    ));
                }

                if block.number() == latest_block.number() && block.hash() == latest_block.hash() {
                    return Ok(ChainChange::AlreadySeen);
                }

                // simple block application
                if block.number() == latest_block.number() + 1
                    && latest_block.hash() == block.parent_hash()
                {
                    let blocks = self.advance_chain_state_with_block(
                        block,
                        &mut canon_cursor,
                        &mut block_cursor,
                    )?;

                    txn.commit()?;

                    return Ok(ChainChange::Advance(blocks));
                }

                todo!()
            }
        }
    }

    fn store_block_with_cursor(
        &self,
        block: &B,
        cursor: &mut TableCursor<'_, tables::BlockTable<B>, RW>,
    ) -> Result<()> {
        cursor.put(&(block.number(), block.hash().clone()), block)?;
        Ok(())
    }

    fn update_canonical_chain_with_cursor(
        &self,
        block: &B,
        cursor: &mut TableCursor<'_, tables::CanonicalBlockTable<B::Hash>, RW>,
    ) -> Result<()> {
        let canon_block = tables::CanonicalBlock {
            hash: block.hash().as_ref().to_vec(),
        };
        cursor.put(&block.number(), &canon_block)?;
        Ok(())
    }

    fn block_by_func_with_txn<F, K>(
        &self,
        txn: &Transaction<'_, K, E>,
        func: F,
    ) -> Result<Option<B>>
    where
        F: Fn(
            &mut TableCursor<'_, tables::CanonicalBlockTable<B::Hash>, K>,
        ) -> std::result::Result<Option<(u64, tables::CanonicalBlock)>, MdbxError>,
        K: TransactionKind,
    {
        let mut canon_cursor = txn
            .open_table::<tables::CanonicalBlockTable<B::Hash>>()?
            .cursor()?;
        let (block_number, block_hash) = if let Some((num, hash)) = func(&mut canon_cursor)? {
            let hash = B::Hash::from_slice(&hash.hash)?;
            (num, hash)
        } else {
            return Ok(None);
        };
        let mut block_cursor = txn.open_table::<tables::BlockTable<B>>()?.cursor()?;
        let block = block_cursor
            .seek_exact(&(block_number, block_hash))?
            .map(|(_, block)| block);
        Ok(block)
    }

    fn advance_chain_state_with_block(
        &self,
        block: B,
        canon_cursor: &mut TableCursor<'_, tables::CanonicalBlockTable<B::Hash>, RW>,
        block_cursor: &mut TableCursor<'_, tables::BlockTable<B>, RW>,
    ) -> Result<Vec<B>> {
        let mut next_iter_block = Some(block);
        let mut applied_blocks = Vec::new();

        while let Some(block) = next_iter_block.take() {
            self.update_canonical_chain_with_cursor(&block, canon_cursor)?;
            let next_block_number = block.number() + 1;
            let block_hash = block.hash().clone();
            applied_blocks.push(block);

            let mut maybe_next_block =
                block_cursor.seek_range(&(next_block_number, B::Hash::zero()))?;

            while let Some(((block_number, _), next_block)) = maybe_next_block {
                // explored all siblings
                if block_number != next_block_number {
                    break;
                }

                if next_block.parent_hash() == &block_hash {
                    next_iter_block = Some(next_block);
                    break;
                }
                maybe_next_block = block_cursor.next()?;
            }
        }

        Ok(applied_blocks)
    }
}

impl<B: Block> ChainChange<B> {
    pub fn as_advance(&self) -> Option<&[B]> {
        match self {
            ChainChange::Advance(blocks) => Some(blocks),
            _ => None,
        }
    }

    pub fn as_reorg(&self) -> Option<&[B]> {
        match self {
            ChainChange::Reorg(blocks) => Some(blocks),
            _ => None,
        }
    }

    pub fn as_missing_block(&self) -> Option<(u64, &B::Hash)> {
        match self {
            ChainChange::MissingBlock(num, hash) => Some((*num, hash)),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{f32::consts::E, sync::Arc};

    use assert_matches::assert_matches;
    use libmdbx::{Environment, NoWriteMap};
    use prost::Message;
    use tempfile::tempdir;

    use crate::db::{
        tables::{Block, BlockHash},
        MdbxEnvironmentExt,
    };

    use super::{ChainChange, ChainTracker, ChainTrackerError};

    #[derive(PartialEq, Clone, Message)]
    pub struct TestBlockHash {
        #[prost(bytes, tag = "1")]
        pub hash: Vec<u8>,
    }

    impl TestBlockHash {
        // Create a new block hash. Fork is used to denote different chains.
        pub fn new(number: u64, fork: u64) -> TestBlockHash {
            let mut out = [0; 32];
            out[..8].copy_from_slice(&number.to_be_bytes());
            out[24..].copy_from_slice(&fork.to_be_bytes());
            TestBlockHash { hash: out.to_vec() }
        }
    }

    #[derive(Message, Clone)]
    pub struct TestBlock {
        #[prost(message, required, tag = "1")]
        pub hash: TestBlockHash,
        #[prost(message, required, tag = "2")]
        pub parent_hash: TestBlockHash,
        #[prost(uint64, required, tag = "3")]
        pub number: u64,
    }

    impl BlockHash for TestBlockHash {
        fn from_slice(b: &[u8]) -> Result<Self, crate::db::KeyDecodeError> {
            if b.len() != 32 {
                return Err(crate::db::KeyDecodeError::InvalidByteSize {
                    expected: 32,
                    actual: b.len(),
                });
            }
            Ok(TestBlockHash { hash: b.to_vec() })
        }

        fn zero() -> Self {
            let hash = vec![0; 32];
            TestBlockHash { hash }
        }
    }

    impl AsRef<[u8]> for TestBlockHash {
        fn as_ref(&self) -> &[u8] {
            &self.hash
        }
    }

    impl Block for TestBlock {
        type Hash = TestBlockHash;

        fn number(&self) -> u64 {
            self.number
        }

        fn hash(&self) -> &Self::Hash {
            &self.hash
        }

        fn parent_hash(&self) -> &Self::Hash {
            &self.parent_hash
        }
    }

    fn new_chain_tracker() -> ChainTracker<TestBlock, NoWriteMap> {
        let _ = env_logger::builder().is_test(true).try_init();
        let path = tempdir().unwrap();
        let db = Environment::<NoWriteMap>::open(path.path()).unwrap();
        ChainTracker::new(Arc::new(db)).unwrap()
    }

    #[test]
    pub fn test_advance_indexed_block() {
        let chain = new_chain_tracker();

        let genesis = TestBlock {
            number: 0,
            hash: TestBlockHash::new(0, 0),
            parent_hash: TestBlockHash::new(0, 0),
        };
        assert_matches!(
            chain.update_indexed_block(genesis.clone()),
            Err(ChainTrackerError::MissingHead)
        );

        let head = TestBlock {
            number: 10,
            hash: TestBlockHash::new(10, 0),
            parent_hash: TestBlockHash::new(9, 0),
        };

        chain.update_head(&head).unwrap();
        let change = chain.update_indexed_block(genesis).unwrap();
        assert_matches!(change, ChainChange::Advance(..));

        assert_eq!(chain.head_height().unwrap(), Some(10));
        let block = chain.latest_indexed_block().unwrap().unwrap();
        assert_eq!(block.number, 0);

        let block_1 = TestBlock {
            number: 1,
            hash: TestBlockHash::new(1, 0),
            parent_hash: TestBlockHash::new(0, 0),
        };
        let block_2 = TestBlock {
            number: 2,
            hash: TestBlockHash::new(2, 0),
            parent_hash: TestBlockHash::new(1, 0),
        };

        let (block_num, _) = chain
            .update_indexed_block(block_2.clone())
            .unwrap()
            .as_missing_block()
            .unwrap();
        assert_eq!(block_num, 1);

        chain.update_indexed_block(block_1).unwrap();
        chain.update_indexed_block(block_2).unwrap();
        let block = chain.latest_indexed_block().unwrap().unwrap();
        assert_eq!(block.number, 2);
        let block = chain.block_by_number(1).unwrap().unwrap();
        assert_eq!(block.number, 1);
    }

    #[test]
    pub fn test_index_connect_with_head() {
        let chain = new_chain_tracker();

        // simulate adding heads while backfilling the rest of the chain
        let block_2 = TestBlock {
            number: 2,
            hash: TestBlockHash::new(2, 0),
            parent_hash: TestBlockHash::new(1, 0),
        };
        chain.update_head(&block_2).unwrap();

        let block_3 = TestBlock {
            number: 3,
            hash: TestBlockHash::new(3, 0),
            parent_hash: TestBlockHash::new(2, 0),
        };
        chain.update_head(&block_3).unwrap();

        let genesis = TestBlock {
            number: 0,
            hash: TestBlockHash::new(0, 0),
            parent_hash: TestBlockHash::new(0, 0),
        };
        chain.update_indexed_block(genesis).unwrap();

        // now we backfill block_1, we expect the tracker to advance
        // to block 3

        let block_1 = TestBlock {
            number: 1,
            hash: TestBlockHash::new(1, 0),
            parent_hash: TestBlockHash::new(0, 0),
        };
        let res = chain.update_indexed_block(block_1).unwrap();

        let blocks = res.as_advance().unwrap();
        assert_eq!(blocks.len(), 3);

        let block = chain.latest_indexed_block().unwrap().unwrap();
        assert_eq!(block.number, 3);
    }

    #[test]
    pub fn test_reorg() {
        todo!()
    }

    #[test]
    pub fn test_block_gap() {
        todo!()
    }

    #[test]
    pub fn test_invalidate_blocks() {
        todo!()
    }
}
