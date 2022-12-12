//! Abstraction over raw db tables.

use std::sync::Arc;

use apibara_node::db::{
    libmdbx::{self, Environment, EnvironmentKind, Transaction, RW},
    MdbxErrorExt, MdbxTransactionExt, TableCursor,
};

use crate::core::{pb::starknet::v1alpha2, GlobalBlockId};

use super::tables;

/// An object to read chain data from storage.
pub trait StorageReader {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Returns the highest accepted block that was indexed.
    fn highest_accepted_block(&self) -> Result<Option<GlobalBlockId>, Self::Error>;

    /// Returns the highest finalized block that was indexed.
    fn highest_finalized_block(&self) -> Result<Option<GlobalBlockId>, Self::Error>;

    /// Returns the block id for the block at the given height, or `None` if the
    /// canonical chain is shorter.
    fn canonical_block_id(&self, number: u64) -> Result<Option<GlobalBlockId>, Self::Error>;

    /// Returns the block status for the given block.
    fn read_status(&self, id: &GlobalBlockId)
        -> Result<Option<v1alpha2::BlockStatus>, Self::Error>;

    /// Returns the block header for the given block.
    fn read_header(&self, id: &GlobalBlockId)
        -> Result<Option<v1alpha2::BlockHeader>, Self::Error>;

    /// Returns all transactions in the given block.
    fn read_body(&self, id: &GlobalBlockId) -> Result<Vec<v1alpha2::Transaction>, Self::Error>;

    /// Returns all receipts in the given block.
    fn read_receipts(
        &self,
        id: &GlobalBlockId,
    ) -> Result<Vec<v1alpha2::TransactionReceipt>, Self::Error>;

    /// Returns the state update for the given block.
    fn read_state_update(
        &self,
        id: &GlobalBlockId,
    ) -> Result<Option<v1alpha2::StateUpdate>, Self::Error>;
}

/// An object to write chain data to storage in a single transaction.
pub trait StorageWriter {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Commit writes to storage.
    fn commit(self) -> Result<(), Self::Error>;

    /// Adds the given block to the canonical chain.
    fn update_canonical_chain(&mut self, id: &GlobalBlockId) -> Result<(), Self::Error>;

    /// Writes the block status.
    fn write_status(
        &mut self,
        id: &GlobalBlockId,
        status: v1alpha2::BlockStatus,
    ) -> Result<(), Self::Error>;

    /// Writes the block header.
    fn write_header(
        &mut self,
        id: &GlobalBlockId,
        header: v1alpha2::BlockHeader,
    ) -> Result<(), Self::Error>;

    /// Writes the transactions in a block.
    fn write_body(
        &mut self,
        id: &GlobalBlockId,
        transactions: Vec<v1alpha2::Transaction>,
    ) -> Result<(), Self::Error>;

    /// Writes the receipts in a block.
    fn write_receipts(
        &mut self,
        id: &GlobalBlockId,
        receipts: Vec<v1alpha2::TransactionReceipt>,
    ) -> Result<(), Self::Error>;

    /// Writes the block state update.
    fn write_state_update(
        &mut self,
        id: &GlobalBlockId,
        state_update: v1alpha2::StateUpdate,
    ) -> Result<(), Self::Error>;
}

#[derive(Debug, Clone)]
pub struct DatabaseStorage<E: EnvironmentKind> {
    db: Arc<Environment<E>>,
}

pub struct DatabaseStorageWriter<'env, 'txn, E: EnvironmentKind> {
    txn: Transaction<'env, RW, E>,
    status_cursor: TableCursor<'txn, tables::BlockStatusTable, RW>,
    header_cursor: TableCursor<'txn, tables::BlockHeaderTable, RW>,
    body_cursor: TableCursor<'txn, tables::BlockBodyTable, RW>,
    receipts_cursor: TableCursor<'txn, tables::BlockReceiptsTable, RW>,
    state_update_cursor: TableCursor<'txn, tables::StateUpdateTable, RW>,
    canonical_chain_cursor: TableCursor<'txn, tables::CanonicalChainTable, RW>,
}

impl<E: EnvironmentKind> DatabaseStorage<E> {
    pub fn new(db: Arc<Environment<E>>) -> Self {
        DatabaseStorage { db }
    }

    pub fn begin_txn(&self) -> Result<DatabaseStorageWriter<'_, '_, E>, libmdbx::Error> {
        let txn = self.db.begin_rw_txn()?;
        let status_cursor = txn.open_cursor::<tables::BlockStatusTable>()?;
        let header_cursor = txn.open_cursor::<tables::BlockHeaderTable>()?;
        let body_cursor = txn.open_cursor::<tables::BlockBodyTable>()?;
        let receipts_cursor = txn.open_cursor::<tables::BlockReceiptsTable>()?;
        let state_update_cursor = txn.open_cursor::<tables::StateUpdateTable>()?;
        let canonical_chain_cursor = txn.open_cursor::<tables::CanonicalChainTable>()?;
        let writer = DatabaseStorageWriter {
            txn,
            status_cursor,
            header_cursor,
            body_cursor,
            receipts_cursor,
            state_update_cursor,
            canonical_chain_cursor,
        };
        Ok(writer)
    }
}

impl<E: EnvironmentKind> StorageReader for DatabaseStorage<E> {
    type Error = libmdbx::Error;

    #[tracing::instrument(skip(self))]
    fn highest_accepted_block(&self) -> Result<Option<GlobalBlockId>, Self::Error> {
        let txn = self.db.begin_ro_txn()?;
        let mut cursor = txn.open_cursor::<tables::CanonicalChainTable>()?;
        let block_id = match cursor.last()? {
            None => None,
            Some((number, hash)) => {
                let hash = (&hash).try_into().map_err(libmdbx::Error::decode_error)?;
                Some(GlobalBlockId::new(number, hash))
            }
        };
        txn.commit()?;
        Ok(block_id)
    }

    #[tracing::instrument(skip(self))]
    fn highest_finalized_block(&self) -> Result<Option<GlobalBlockId>, Self::Error> {
        let txn = self.db.begin_ro_txn()?;
        let mut canon_cursor = txn.open_cursor::<tables::CanonicalChainTable>()?;
        let mut status_cursor = txn.open_cursor::<tables::BlockStatusTable>()?;
        let mut maybe_block_id = canon_cursor.last()?;
        while let Some((block_num, block_hash)) = maybe_block_id {
            let block_hash = (&block_hash)
                .try_into()
                .map_err(libmdbx::Error::decode_error)?;
            let block_id = GlobalBlockId::new(block_num, block_hash);
            let (_, status) = status_cursor
                .seek_exact(&block_id)?
                .expect("database is in inconsistent state.");

            if status.status().is_finalized() {
                txn.commit()?;
                return Ok(Some(block_id));
            }

            maybe_block_id = canon_cursor.prev()?;
        }
        txn.commit()?;
        Ok(None)
    }

    #[tracing::instrument(skip(self))]
    fn canonical_block_id(&self, number: u64) -> Result<Option<GlobalBlockId>, Self::Error> {
        let txn = self.db.begin_ro_txn()?;
        let mut cursor = txn.open_cursor::<tables::CanonicalChainTable>()?;
        match cursor.seek_exact(&number)? {
            None => {
                txn.commit()?;
                Ok(None)
            }
            Some((_, block_hash)) => {
                let block_hash = (&block_hash)
                    .try_into()
                    .map_err(libmdbx::Error::decode_error)?;
                let block_id = GlobalBlockId::new(number, block_hash);
                txn.commit()?;
                Ok(Some(block_id))
            }
        }
    }

    #[tracing::instrument(skip(self))]
    fn read_status(
        &self,
        id: &GlobalBlockId,
    ) -> Result<Option<v1alpha2::BlockStatus>, Self::Error> {
        let txn = self.db.begin_ro_txn()?;
        let mut cursor = txn.open_cursor::<tables::BlockStatusTable>()?;
        let status = cursor.seek_exact(id.into())?.map(|t| t.1.status());
        txn.commit()?;
        Ok(status)
    }

    #[tracing::instrument(skip(self))]
    fn read_header(
        &self,
        id: &GlobalBlockId,
    ) -> Result<Option<v1alpha2::BlockHeader>, Self::Error> {
        let txn = self.db.begin_ro_txn()?;
        let mut cursor = txn.open_cursor::<tables::BlockHeaderTable>()?;
        let header = cursor.seek_exact(id.into())?.map(|t| t.1);
        txn.commit()?;
        Ok(header)
    }

    #[tracing::instrument(skip(self))]
    fn read_body(&self, id: &GlobalBlockId) -> Result<Vec<v1alpha2::Transaction>, Self::Error> {
        let txn = self.db.begin_ro_txn()?;
        let mut cursor = txn.open_cursor::<tables::BlockBodyTable>()?;
        let transactions = cursor
            .seek_exact(id.into())?
            .map(|t| t.1.transactions)
            .unwrap_or_default();
        txn.commit()?;
        Ok(transactions)
    }

    #[tracing::instrument(skip(self))]
    fn read_receipts(
        &self,
        id: &GlobalBlockId,
    ) -> Result<Vec<v1alpha2::TransactionReceipt>, Self::Error> {
        let txn = self.db.begin_ro_txn()?;
        let mut cursor = txn.open_cursor::<tables::BlockReceiptsTable>()?;
        let receipts = cursor
            .seek_exact(id.into())?
            .map(|t| t.1.receipts)
            .unwrap_or_default();
        txn.commit()?;
        Ok(receipts)
    }

    #[tracing::instrument(skip(self))]
    fn read_state_update(
        &self,
        id: &GlobalBlockId,
    ) -> Result<Option<v1alpha2::StateUpdate>, Self::Error> {
        let txn = self.db.begin_ro_txn()?;
        let mut cursor = txn.open_cursor::<tables::StateUpdateTable>()?;
        let state_update = cursor.seek_exact(id.into())?.map(|t| t.1);
        txn.commit()?;
        Ok(state_update)
    }
}

impl<'env, 'txn, E: EnvironmentKind> StorageWriter for DatabaseStorageWriter<'env, 'txn, E> {
    type Error = libmdbx::Error;

    #[tracing::instrument(skip(self))]
    fn commit(self) -> Result<(), Self::Error> {
        self.txn.commit()?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    fn update_canonical_chain(&mut self, id: &GlobalBlockId) -> Result<(), Self::Error> {
        let number = id.number();
        let hash = id.hash().into();
        self.canonical_chain_cursor.seek_exact(&number)?;
        self.canonical_chain_cursor.put(&number, &hash)?;
        Ok(())
    }

    #[tracing::instrument(skip(self, status))]
    fn write_status(
        &mut self,
        id: &GlobalBlockId,
        status: v1alpha2::BlockStatus,
    ) -> Result<(), Self::Error> {
        let status_v = super::BlockStatus {
            status: status as i32,
        };
        self.status_cursor.seek_exact(id)?;
        self.status_cursor.put(id, &status_v)?;
        Ok(())
    }

    #[tracing::instrument(skip(self, header))]
    fn write_header(
        &mut self,
        id: &GlobalBlockId,
        header: v1alpha2::BlockHeader,
    ) -> Result<(), Self::Error> {
        self.header_cursor.seek_exact(id)?;
        self.header_cursor.put(id, &header)?;
        Ok(())
    }

    #[tracing::instrument(skip(self, transactions))]
    fn write_body(
        &mut self,
        id: &GlobalBlockId,
        transactions: Vec<v1alpha2::Transaction>,
    ) -> Result<(), Self::Error> {
        let body = super::BlockBody { transactions };
        self.body_cursor.seek_exact(id)?;
        self.body_cursor.put(id, &body)?;
        Ok(())
    }

    #[tracing::instrument(skip(self, receipts))]
    fn write_receipts(
        &mut self,
        id: &GlobalBlockId,
        receipts: Vec<v1alpha2::TransactionReceipt>,
    ) -> Result<(), Self::Error> {
        let body = super::BlockReceipts { receipts };
        self.receipts_cursor.seek_exact(id)?;
        self.receipts_cursor.put(id, &body)?;
        Ok(())
    }

    #[tracing::instrument(skip(self, state_update))]
    fn write_state_update(
        &mut self,
        id: &GlobalBlockId,
        state_update: v1alpha2::StateUpdate,
    ) -> Result<(), Self::Error> {
        self.state_update_cursor.seek_exact(id)?;
        self.state_update_cursor.put(id, &state_update)?;
        Ok(())
    }
}
