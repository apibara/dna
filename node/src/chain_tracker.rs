//! # Track the chain's head
use std::{marker::PhantomData, sync::Arc};

use libmdbx::{Environment, EnvironmentKind, Error as MdbxError, Transaction, TransactionKind, RW};
use tracing::debug;

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
        let height = match block_cursor.last()? {
            None => None,
            Some(((height, _), _)) => Some(height),
        };
        txn.commit()?;
        Ok(height)
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
        block_cursor.put(&(head.number(), head.hash().clone()), &head)?;
        self.store_block_with_cursor(head, &mut block_cursor)?;
        txn.commit()?;
        Ok(())
    }

    /// Updates the state with a new indexed block.
    ///
    /// This function must be called after at least one head has been
    /// stored, so that the tracker can detect chain reorganizations.
    pub fn update_indexed_block(&self, block: &B) -> Result<()> {
        let head_height = self.head_height()?.ok_or(ChainTrackerError::MissingHead)?;

        match self.latest_indexed_block()? {
            None => {
                // first block being indexed, expect it to have height 0
                if block.number() != 0 {
                    return Err(ChainTrackerError::InvalidBlockNumber {
                        expected: 0,
                        given: block.number(),
                    });
                }
                let txn = self.db.begin_rw_txn()?;
                let mut block_cursor = txn.open_table::<tables::BlockTable<B>>()?.cursor()?;
                let mut canon_cursor = txn
                    .open_table::<tables::CanonicalBlockTable<B::Hash>>()?
                    .cursor()?;
                self.store_block_with_cursor(block, &mut block_cursor)?;
                self.update_canonical_chain_with_cursor(block, &mut canon_cursor)?;
                txn.commit()?;
                Ok(())
            }
            Some(prev_block) => {
                // any block must be successors of the previous indexed block
                if block.number() != prev_block.number() + 1 {
                    // TODO: if the height is > head, return a missing block error
                    // containing the height and hash of the block
                    // also store the block for later use
                    if block.number() > head_height {
                        todo!()
                    }

                    return Err(ChainTrackerError::InvalidBlockNumber {
                        expected: prev_block.number() + 1,
                        given: block.number(),
                    });
                }

                let txn = self.db.begin_rw_txn()?;
                let mut block_cursor = txn.open_table::<tables::BlockTable<B>>()?.cursor()?;
                let mut canon_cursor = txn
                    .open_table::<tables::CanonicalBlockTable<B::Hash>>()?
                    .cursor()?;
                self.store_block_with_cursor(block, &mut block_cursor)?;

                // check if the given block parent hash matches prev block.
                if prev_block.hash() == block.parent_hash() {
                    // clean apply
                    self.update_canonical_chain_with_cursor(&block, &mut canon_cursor)?;
                } else {
                    // TODO: can it reorg the chain?
                    todo!()
                }

                txn.commit()?;

                Ok(())
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
        let block = match block_cursor.seek_exact(&(block_number, block_hash))? {
            None => None,
            Some((_, block)) => Some(block),
        };
        Ok(block)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use libmdbx::{Environment, NoWriteMap};
    use prost::Message;
    use tempfile::tempdir;

    use crate::db::{
        tables::{Block, BlockHash},
        MdbxEnvironmentExt,
    };

    use super::{ChainTracker, ChainTrackerError};

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

    #[derive(Message)]
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
            chain.update_indexed_block(&genesis),
            Err(ChainTrackerError::MissingHead)
        );

        let head = TestBlock {
            number: 10,
            hash: TestBlockHash::new(10, 0),
            parent_hash: TestBlockHash::new(9, 0),
        };

        chain.update_head(&head).unwrap();
        chain.update_indexed_block(&genesis).unwrap();

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

        assert_matches!(
            chain.update_indexed_block(&block_2),
            Err(ChainTrackerError::InvalidBlockNumber {
                expected: 1,
                given: 2
            })
        );
        chain.update_indexed_block(&block_1).unwrap();
        chain.update_indexed_block(&block_2).unwrap();
        let block = chain.latest_indexed_block().unwrap().unwrap();
        assert_eq!(block.number, 2);
        let block = chain.block_by_number(1).unwrap().unwrap();
        assert_eq!(block.number, 1);
    }
}
