//! # Track the chain's head
use std::{marker::PhantomData, sync::Arc};

use libmdbx::{Environment, EnvironmentKind, Error as MdbxError, Transaction, RW};
use tracing::debug;

use crate::db::{
    tables::{
        self, BlockHash, BlockHeader as DbBlockHeader, BlockHeaderTable, CanonicalBlockHeader,
        CanonicalBlockHeaderTable,
    },
    KeyDecodeError, MdbxRWTransactionExt, MdbxTransactionExt, TableCursor,
};

/// Information about a block, used to form a chain of blocks.
///
/// The genesis block has hash = 0.
#[derive(Debug, Clone)]
pub struct BlockHeader<H: BlockHash> {
    pub hash: H,
    pub parent_hash: H,
    pub number: u64,
}

/// Track the chain's head.
///
/// The head tracker is a state machine to track the chain's current head.
/// It doesn't interact with nodes directly, instead the caller is expected to
/// iteratively supply the blocks requested by the head tracker.
/// The head tracker uses mdbx to persist the canonical chain.
pub struct HeadTracker<H: BlockHash, E: EnvironmentKind> {
    db: Arc<Environment<E>>,
    minimum_headers_buffer_size: usize,
    phantom: PhantomData<H>,
}

#[derive(Debug, thiserror::Error)]
pub enum HeadTrackerError {
    #[error("error originating from database")]
    Database(#[from] MdbxError),
    #[error("error decoding key from database")]
    KeyDecodeError(#[from] KeyDecodeError),
    #[error("inconsistent database state")]
    InconsistentState,
}

pub type Result<T> = std::result::Result<T, HeadTrackerError>;

/// Identifier used when requesting blocks.
#[derive(Debug, Clone)]
pub enum BlockId<H: BlockHash> {
    Number(u64),
    HashAndNumber(H, u64),
}

/// A response from any of the [HeadTracker]'s methods.
#[derive(Debug, Clone)]
pub enum HeadTrackerResponse<H: BlockHash> {
    /// The supplied block is the new head.
    NewHead(BlockHeader<H>),
    /// The supplied block is the new head and caused a reorg.
    Reorg(Vec<BlockHeader<H>>),
    /// The supplied block is not the one following the current head.
    BlockGap(BlockHeader<H>),
    /// The tracker is missing some block headers to take a decision.
    MissingBlockHeaders(Vec<BlockId<H>>),
    /// Block already belongs to the canonical chain.
    AlreadySeenBlock,
}

impl<H, E> HeadTracker<H, E>
where
    H: BlockHash,
    E: EnvironmentKind,
{
    /// Create a new head tracker that stores data in the given db.
    ///
    /// The head tracker requires to always have at least `minimum_headers_buffer_size` headers
    /// stored.
    pub fn new(db: Arc<Environment<E>>, minimum_headers_buffer_size: usize) -> Result<Self> {
        let txn = db.begin_rw_txn()?;
        txn.ensure_table::<tables::BlockHeaderTable<H>>(None)?;
        txn.ensure_table::<tables::CanonicalBlockHeaderTable<H>>(None)?;
        txn.commit()?;
        Ok(HeadTracker {
            db,
            minimum_headers_buffer_size,
            phantom: PhantomData::default(),
        })
    }

    pub fn apply_head(&self, head: BlockHeader<H>) -> Result<HeadTrackerResponse<H>> {
        if self.is_first_block_ever()? {
            // Have no blocks in storage because it's a new chain.
            // So just apply the genesis block.
            if head.number == 0 {
                return self.build_initial_canonical_chain(head);
            }

            // Get an initial buffer of blocks.
            let missing_end = head.number - 1;
            let missing_start = if missing_end > self.minimum_headers_buffer_size as u64 {
                1 + missing_end - self.minimum_headers_buffer_size as u64
            } else {
                0
            };

            let missing_blocks = missing_start..=missing_end;
            return Ok(HeadTrackerResponse::MissingBlockHeaders(
                missing_blocks.map(BlockId::Number).collect(),
            ));
        }

        // After receiving the initial buffer of blocks, it's time to apply the provided head
        // to build the canonical chain.
        if !self.has_canonical_chain()? {
            return self.build_initial_canonical_chain(head);
        }

        // Canonical chain exists, try to extend it.
        self.extend_canonical_chain(head)
    }

    pub fn supply_missing_blocks(&self, blocks: Vec<BlockHeader<H>>) -> Result<()> {
        let txn = self.db.begin_rw_txn()?;
        let blocks_table = txn.open_table::<tables::BlockHeaderTable<H>>()?;
        let mut blocks_cursor = blocks_table.cursor()?;
        for block in &blocks {
            let hash = block.hash.clone();
            blocks_cursor.put(&(block.number, hash), &block.into())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Returns the current head according to this tracker.
    pub fn current_head(&self) -> Result<Option<BlockHeader<H>>> {
        let txn = self.db.begin_ro_txn()?;
        let canonical_table = txn.open_table::<tables::CanonicalBlockHeaderTable<H>>()?;
        let blocks_table = txn.open_table::<tables::BlockHeaderTable<H>>()?;
        match canonical_table.cursor()?.last()? {
            None => {
                txn.commit()?;
                Ok(None)
            }
            Some((block_number, canon)) => {
                let hash = H::from_slice(&canon.hash)?;
                match blocks_table.get(&(block_number, hash.clone()))? {
                    Some(header) => {
                        txn.commit()?;
                        let header = header.try_into()?;
                        Ok(Some(header))
                    }
                    None => {
                        // Canonical block headers references a block that doesn't exist
                        // in block headers.
                        debug!(block_number = ?block_number, block_hash = ?hash, "inconsistent state");
                        txn.commit()?;
                        Err(HeadTrackerError::InconsistentState)
                    }
                }
            }
        }
    }

    fn is_first_block_ever(&self) -> Result<bool> {
        let txn = self.db.begin_ro_txn()?;
        let table = txn.open_table::<tables::BlockHeaderTable<H>>()?;
        let mut cursor = table.cursor()?;
        let value = cursor.first()?;
        txn.commit()?;
        Ok(value.is_none())
    }

    fn has_canonical_chain(&self) -> Result<bool> {
        let txn = self.db.begin_ro_txn()?;
        let table = txn.open_table::<tables::CanonicalBlockHeaderTable<H>>()?;
        let mut cursor = table.cursor()?;
        let value = cursor.first()?;
        txn.commit()?;
        Ok(value.is_some())
    }

    fn build_initial_canonical_chain(
        &self,
        head: BlockHeader<H>,
    ) -> Result<HeadTrackerResponse<H>> {
        // iterate backwards to check that we have all blocks starting from `head`.
        // if any block is missing, request it to the caller.
        // if no block is missing, update canonical chain and store head.
        let txn = self.db.begin_rw_txn()?;
        let canon_table = txn.open_table::<tables::CanonicalBlockHeaderTable<H>>()?;
        let mut canon_cursor = canon_table.cursor()?;
        let blocks_table = txn.open_table::<tables::BlockHeaderTable<H>>()?;
        let mut blocks_cursor = blocks_table.cursor()?;

        debug_assert!(head.number > 0);
        let mut block_info = (head.number - 1, head.parent_hash.clone());

        // Optimistically insert new canonical head. If the head does not form a valid
        // chain the txn will be aborted.
        canon_cursor.put(&head.number, &head.hash.clone().into())?;
        blocks_cursor.put(&(head.number, head.hash.clone()), &(&head).into())?;

        loop {
            match blocks_cursor.seek_exact(&block_info)? {
                None => {
                    // Request block with the given hash and number
                    // Abort transaction
                    drop(txn);
                    todo!()
                }
                Some(((block_number, block_hash), block)) => {
                    if head.number - block_number < self.minimum_headers_buffer_size as u64 {
                        // extend canonical chain
                        canon_cursor.put(&block_number, &block_hash.clone().into())?;
                        let parent_hash = H::from_slice(&block.parent_hash)?;
                        if block_number == 0 {
                            txn.commit()?;
                            return Ok(HeadTrackerResponse::NewHead(head));
                        }
                        block_info = (block_number - 1, parent_hash);
                    } else {
                        // inspected enough blocks and everything is in order.
                        txn.commit()?;
                        return Ok(HeadTrackerResponse::NewHead(head));
                    }
                }
            }
        }
    }

    fn extend_canonical_chain(&self, head: BlockHeader<H>) -> Result<HeadTrackerResponse<H>> {
        let txn = self.db.begin_rw_txn()?;
        let canon_table = txn.open_table::<tables::CanonicalBlockHeaderTable<H>>()?;
        let mut canon_cursor = canon_table.cursor()?;
        let blocks_table = txn.open_table::<tables::BlockHeaderTable<H>>()?;
        let mut blocks_cursor = blocks_table.cursor()?;

        // check if the provided block already belongs to the canonical chain
        if let Some((_, canon_block)) = canon_cursor.seek_exact(&head.number)? {
            if canon_block.hash == head.hash.as_ref() {
                return Ok(HeadTrackerResponse::AlreadySeenBlock);
            }
        }

        // If it reaches this function there must be at least one value in CanonicalBlockHeadersTable
        let (canon_block_number, canon_block) = canon_cursor
            .last()?
            .ok_or(HeadTrackerError::InconsistentState)?;

        let canon_hash = H::from_slice(&canon_block.hash)?;

        debug!(canon_block_number = ?canon_block_number, block_number = ?head.number, "extend chain");

        if canon_block_number + 1 == head.number {
            // possible clean apply
            let (_, current_head) = blocks_cursor
                .seek_exact(&(canon_block_number, canon_hash))?
                .ok_or(HeadTrackerError::InconsistentState)?;

            let current_head_hash = H::from_slice(&current_head.hash)?;
            if current_head_hash == head.parent_hash {
                // clean apply
                canon_cursor.put(&head.number, &head.hash.clone().into())?;
                blocks_cursor.put(&(head.number, head.hash.clone()), &(&head).into())?;
                txn.commit()?;
                return Ok(HeadTrackerResponse::NewHead(head));
            }

            // possible reorg. block has the correct number but comes from a different chain
            self.handle_chain_reorg_with_txn(head, txn, &mut canon_cursor, &mut blocks_cursor)
        } else if canon_block_number >= head.number {
            // possible reorg. block belongs to a different chain
            self.handle_chain_reorg_with_txn(head, txn, &mut canon_cursor, &mut blocks_cursor)
        } else {
            // there's a gap in the supplied blocks so it's not possible to take a decision
            debug_assert!(head.number - canon_block_number > 1);
            let (_, current_head) = blocks_cursor
                .seek_exact(&(canon_block_number, canon_hash))?
                .ok_or(HeadTrackerError::InconsistentState)?;
            let current_head = current_head.try_into()?;
            Ok(HeadTrackerResponse::BlockGap(current_head))
        }
    }

    fn handle_chain_reorg_with_txn(
        &self,
        new_head: BlockHeader<H>,
        txn: Transaction<'_, RW, E>,
        canon_cursor: &mut TableCursor<CanonicalBlockHeaderTable<H>, RW>,
        blocks_cursor: &mut TableCursor<BlockHeaderTable<H>, RW>,
    ) -> Result<HeadTrackerResponse<H>> {
        let mut new_head = new_head;
        let mut heads_buf = Vec::new();

        loop {
            // consuming the entire canonical chain buffer is an unrecoverable error
            let (current_head_number, current_head_hash) = canon_cursor
                .last()?
                .ok_or(HeadTrackerError::InconsistentState)?;

            debug!(current_head_number = ?current_head_number, "pop canon");

            // consume canonical chain until it reaches a point where the provided head
            // can join
            if current_head_number >= new_head.number {
                canon_cursor.del()?;
                continue;
            }

            debug_assert!(current_head_number == new_head.number - 1);

            // join two chains at `new_head.parent`?
            if new_head.parent_hash.as_ref() == current_head_hash.hash {
                canon_cursor.put(&new_head.number, &new_head.hash.clone().into())?;
                blocks_cursor.put(
                    &(new_head.number, new_head.hash.clone()),
                    &(&new_head).into(),
                )?;
                txn.commit()?;

                // return new blocks in order
                heads_buf.push(new_head);
                heads_buf.reverse();
                return Ok(HeadTrackerResponse::Reorg(heads_buf));
            }

            // try joining chains again at `new_head.parent`
            match blocks_cursor.seek_exact(&(new_head.number - 1, new_head.parent_hash.clone()))? {
                None => {
                    // block is missing from storage. request it to client
                    return Ok(HeadTrackerResponse::MissingBlockHeaders(vec![
                        BlockId::HashAndNumber(new_head.parent_hash, new_head.number - 1),
                    ]));
                }
                Some((_, parent_block)) => {
                    heads_buf.push(new_head);
                    new_head = parent_block.try_into()?;
                    canon_cursor.del()?;
                    continue;
                }
            }
        }
    }
}

impl<H: BlockHash> From<&BlockHeader<H>> for DbBlockHeader {
    fn from(value: &BlockHeader<H>) -> Self {
        let hash = value.hash.as_ref().to_vec();
        let parent_hash = value.parent_hash.as_ref().to_vec();
        DbBlockHeader {
            number: value.number,
            hash,
            parent_hash,
        }
    }
}

impl<H: BlockHash> BlockId<H> {
    pub fn as_number(&self) -> Option<&u64> {
        match self {
            BlockId::Number(n) => Some(n),
            _ => None,
        }
    }

    pub fn as_hash_and_number(&self) -> Option<(&H, &u64)> {
        match self {
            BlockId::HashAndNumber(h, n) => Some((h, n)),
            _ => None,
        }
    }
}

impl<H: BlockHash> TryFrom<DbBlockHeader> for BlockHeader<H> {
    type Error = KeyDecodeError;

    fn try_from(value: DbBlockHeader) -> std::result::Result<Self, Self::Error> {
        let hash = H::from_slice(&value.hash)?;
        let parent_hash = H::from_slice(&value.parent_hash)?;
        Ok(BlockHeader {
            number: value.number,
            hash,
            parent_hash,
        })
    }
}

impl<H: BlockHash> From<H> for CanonicalBlockHeader {
    fn from(hash: H) -> Self {
        let hash = hash.as_ref().to_vec();
        CanonicalBlockHeader { hash }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use libmdbx::{Environment, NoWriteMap};
    use tempfile::tempdir;

    use crate::db::{tables::BlockHash, MdbxEnvironmentExt};

    use super::{BlockHeader, BlockId, HeadTracker, HeadTrackerResponse};

    #[derive(Debug, Clone, PartialEq)]
    pub struct TestBlockHash([u8; 32]);

    impl TestBlockHash {
        /// Create a new block hash. Fork is used to denote different chains.
        pub fn new(number: u64, fork: u64) -> TestBlockHash {
            let mut out = [0; 32];
            out[..8].copy_from_slice(&number.to_be_bytes());
            out[24..].copy_from_slice(&fork.to_be_bytes());
            TestBlockHash(out)
        }
    }

    impl BlockHash for TestBlockHash {
        fn from_slice(b: &[u8]) -> Result<Self, crate::db::KeyDecodeError> {
            if b.len() != 32 {
                return Err(crate::db::KeyDecodeError::InvalidByteSize {
                    expected: 32,
                    actual: b.len(),
                });
            }
            let mut out = [0; 32];
            out.copy_from_slice(b);
            Ok(TestBlockHash(out))
        }
    }

    impl AsRef<[u8]> for TestBlockHash {
        fn as_ref(&self) -> &[u8] {
            self.0.as_ref()
        }
    }

    #[test]
    pub fn test_clean_head_application() {
        // Test the case in which the head application is "clean", that is the supplied
        // block number is the successors of the current head number, and the parent hash
        // is the previous head hash.
        let _ = env_logger::builder().is_test(true).try_init();
        let path = tempdir().unwrap();
        let db = Environment::<NoWriteMap>::open(path.path()).unwrap();
        let head_tracker = HeadTracker::<TestBlockHash, _>::new(Arc::new(db), 4).unwrap();

        assert!(head_tracker.current_head().unwrap().is_none());

        let block = BlockHeader {
            number: 3,
            hash: TestBlockHash::new(3, 0),
            parent_hash: TestBlockHash::new(2, 0),
        };

        let response = match head_tracker.apply_head(block.clone()).unwrap() {
            HeadTrackerResponse::MissingBlockHeaders(missing) => missing,
            _ => panic!("expected MissingBlockHeaders"),
        };

        let response = response
            .iter()
            .map(|id| match id {
                BlockId::Number(i) => *i,
                _ => panic!("expected BlockId::Number"),
            })
            .collect::<Vec<u64>>();

        assert!(response == vec![0, 1, 2]);
        let blocks = response
            .iter()
            .map(|idx| BlockHeader {
                number: *idx,
                hash: TestBlockHash::new(*idx, 0),
                parent_hash: TestBlockHash::new(if *idx > 0 { idx - 1 } else { 0 }, 0),
            })
            .collect();
        head_tracker.supply_missing_blocks(blocks).unwrap();

        assert!(head_tracker.current_head().unwrap().is_none());

        let response = match head_tracker.apply_head(block).unwrap() {
            HeadTrackerResponse::NewHead(head) => head,
            _ => panic!("expected NewHead"),
        };
        assert!(response.number == 3);

        let new_head = head_tracker.current_head().unwrap().unwrap();
        assert!(new_head.number == 3);
        assert!(new_head.parent_hash == TestBlockHash::new(2, 0));

        let block = BlockHeader {
            number: 4,
            hash: TestBlockHash::new(4, 0),
            parent_hash: TestBlockHash::new(3, 0),
        };

        let response = match head_tracker.apply_head(block).unwrap() {
            HeadTrackerResponse::NewHead(head) => head,
            _ => panic!("expected NewHead"),
        };
        assert!(response.number == 4);

        let new_head = head_tracker.current_head().unwrap().unwrap();
        assert!(new_head.number == 4);
        assert!(new_head.parent_hash == TestBlockHash::new(3, 0));
    }

    #[test]
    pub fn test_head_application_with_gap() {
        // Test the case in which the the user supplies a new head that is not the successor
        // of the current head. The head tracker informs the caller of this so that they can
        // fetch and provide the missing block(s).
        let _ = env_logger::builder().is_test(true).try_init();
        let path = tempdir().unwrap();
        let db = Environment::<NoWriteMap>::open(path.path()).unwrap();
        let head_tracker = HeadTracker::<TestBlockHash, _>::new(Arc::new(db), 4).unwrap();

        let blocks = (1..100)
            .map(|idx| BlockHeader {
                number: idx,
                hash: TestBlockHash::new(idx, 0),
                parent_hash: TestBlockHash::new(idx - 1, 0),
            })
            .collect();
        head_tracker.supply_missing_blocks(blocks).unwrap();

        let block = BlockHeader {
            number: 100,
            hash: TestBlockHash::new(100, 0),
            parent_hash: TestBlockHash::new(99, 0),
        };

        let response = match head_tracker.apply_head(block).unwrap() {
            HeadTrackerResponse::NewHead(head) => head,
            _ => panic!("expected NewHead"),
        };
        assert!(response.number == 100);

        let block = BlockHeader {
            number: 102,
            hash: TestBlockHash::new(102, 0),
            parent_hash: TestBlockHash::new(101, 0),
        };

        let response = match head_tracker.apply_head(block.clone()).unwrap() {
            HeadTrackerResponse::BlockGap(head) => head,
            _ => panic!("expected BlockGap"),
        };
        assert!(response.number == 100);

        let missing_block = BlockHeader {
            number: 101,
            hash: TestBlockHash::new(101, 0),
            parent_hash: TestBlockHash::new(100, 0),
        };
        head_tracker.apply_head(missing_block).unwrap();

        let response = match head_tracker.apply_head(block).unwrap() {
            HeadTrackerResponse::NewHead(head) => head,
            _ => panic!("expected NewHead"),
        };
        assert!(response.number == 102);
    }

    #[test]
    pub fn test_simple_reorg() {
        // This test case simulates a chain reorg. The provided block has the correct
        // sequence number but the parent hash doesn't match the current head's hash.
        let _ = env_logger::builder().is_test(true).try_init();
        let path = tempdir().unwrap();
        let db = Environment::<NoWriteMap>::open(path.path()).unwrap();
        let head_tracker = HeadTracker::<TestBlockHash, _>::new(Arc::new(db), 4).unwrap();

        let blocks = (1..100)
            .map(|idx| BlockHeader {
                number: idx,
                hash: TestBlockHash::new(idx, 0),
                parent_hash: TestBlockHash::new(idx - 1, 0),
            })
            .collect();
        head_tracker.supply_missing_blocks(blocks).unwrap();

        let block = BlockHeader {
            number: 100,
            hash: TestBlockHash::new(100, 0),
            parent_hash: TestBlockHash::new(99, 0),
        };

        let response = match head_tracker.apply_head(block).unwrap() {
            HeadTrackerResponse::NewHead(head) => head,
            _ => panic!("expected NewHead"),
        };
        assert!(response.number == 100);

        let block = BlockHeader {
            number: 100,
            hash: TestBlockHash::new(100, 1),
            parent_hash: TestBlockHash::new(99, 0),
        };

        let response = match head_tracker.apply_head(block.clone()).unwrap() {
            HeadTrackerResponse::Reorg(blocks) => blocks,
            _ => panic!("expected Reorg"),
        };

        assert!(response.len() == 1);
        assert!(response[0].number == 100);
        assert!(response[0].hash == TestBlockHash::new(100, 1));
    }

    #[test]
    pub fn test_deep_reorg() {
        // This test case simulates a chain reorg. The provided block is part of a chain
        // that split deeper in the chain than the most recent block
        //
        // 99  100  101
        //  o---o----o
        //   \--x----x
        //
        // This reorg detection happens in two phases: first the tracker asks for information
        // about block `x100`, then the second time `x101` is passed it detects the reorg and
        // provides `x100` and `x101` as the new blocks.
        let _ = env_logger::builder().is_test(true).try_init();
        let path = tempdir().unwrap();
        let db = Environment::<NoWriteMap>::open(path.path()).unwrap();
        let head_tracker = HeadTracker::<TestBlockHash, _>::new(Arc::new(db), 4).unwrap();

        let blocks = (1..=100)
            .map(|idx| BlockHeader {
                number: idx,
                hash: TestBlockHash::new(idx, 0),
                parent_hash: TestBlockHash::new(idx - 1, 0),
            })
            .collect();
        head_tracker.supply_missing_blocks(blocks).unwrap();

        let block = BlockHeader {
            number: 101,
            hash: TestBlockHash::new(101, 0),
            parent_hash: TestBlockHash::new(100, 0),
        };

        let response = match head_tracker.apply_head(block).unwrap() {
            HeadTrackerResponse::NewHead(head) => head,
            _ => panic!("expected NewHead"),
        };
        assert!(response.number == 101);

        let block = BlockHeader {
            number: 101,
            hash: TestBlockHash::new(101, 1),
            parent_hash: TestBlockHash::new(100, 1),
        };

        let missing_blocks = match head_tracker.apply_head(block).unwrap() {
            HeadTrackerResponse::MissingBlockHeaders(missing_blocks) => missing_blocks,
            _ => panic!("expected MissingBlockHeaders"),
        };
        assert!(missing_blocks.len() == 1);
        let (missing_hash, missing_number) = missing_blocks[0].as_hash_and_number().unwrap();
        assert!(*missing_number == 100);
        assert!(missing_hash == &TestBlockHash::new(100, 1));

        head_tracker
            .supply_missing_blocks(vec![BlockHeader {
                number: 100,
                hash: TestBlockHash::new(100, 1),
                parent_hash: TestBlockHash::new(99, 0),
            }])
            .unwrap();

        let block = BlockHeader {
            number: 101,
            hash: TestBlockHash::new(101, 1),
            parent_hash: TestBlockHash::new(100, 1),
        };

        let reorg = match head_tracker.apply_head(block).unwrap() {
            HeadTrackerResponse::Reorg(blocks) => blocks,
            _ => panic!("expected Reorg"),
        };

        assert!(reorg.len() == 2);

        let block_100 = &reorg[0];
        assert!(block_100.number == 100);
        let block_101 = &reorg[1];
        assert!(block_101.number == 101);
    }

    #[test]
    pub fn test_already_seen_block() {
        // This test checks that the head tracker detects already seen blocks that belong to
        // the canonical chain.
        let _ = env_logger::builder().is_test(true).try_init();
        let path = tempdir().unwrap();
        let db = Environment::<NoWriteMap>::open(path.path()).unwrap();
        let head_tracker = HeadTracker::<TestBlockHash, _>::new(Arc::new(db), 4).unwrap();

        let blocks = (1..=100)
            .map(|idx| BlockHeader {
                number: idx,
                hash: TestBlockHash::new(idx, 0),
                parent_hash: TestBlockHash::new(idx - 1, 0),
            })
            .collect();
        head_tracker.supply_missing_blocks(blocks).unwrap();

        let block = BlockHeader {
            number: 101,
            hash: TestBlockHash::new(101, 0),
            parent_hash: TestBlockHash::new(100, 0),
        };

        let response = match head_tracker.apply_head(block.clone()).unwrap() {
            HeadTrackerResponse::NewHead(head) => head,
            _ => panic!("expected NewHead"),
        };
        assert!(response.number == 101);

        assert_matches!(
            head_tracker.apply_head(block).unwrap(),
            HeadTrackerResponse::AlreadySeenBlock
        );

        let block = BlockHeader {
            number: 99,
            hash: TestBlockHash::new(99, 0),
            parent_hash: TestBlockHash::new(98, 0),
        };

        assert_matches!(
            head_tracker.apply_head(block).unwrap(),
            HeadTrackerResponse::AlreadySeenBlock
        );
    }
}
