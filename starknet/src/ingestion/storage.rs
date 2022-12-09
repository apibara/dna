//! Ingest a block and all its data.

use std::sync::Arc;

use apibara_node::db::{
    libmdbx::{Environment, EnvironmentKind, Transaction, RW},
    MdbxTransactionExt, TableCursor,
};

use crate::{
    core::pb::v1alpha2,
    core::GlobalBlockId,
    db::{self, tables},
};

use super::BlockIngestionError;

pub struct IngestionStorage<E: EnvironmentKind> {
    db: Arc<Environment<E>>,
}

pub struct BlockWriter<'env, 'txn, E: EnvironmentKind> {
    txn: Transaction<'env, RW, E>,
    status_cursor: TableCursor<'txn, tables::BlockStatusTable, RW>,
    header_cursor: TableCursor<'txn, tables::BlockHeaderTable, RW>,
    body_cursor: TableCursor<'txn, tables::BlockBodyTable, RW>,
    receipts_cursor: TableCursor<'txn, tables::BlockReceiptsTable, RW>,
    state_update_cursor: TableCursor<'txn, tables::StateUpdateTable, RW>,
}

impl<E: EnvironmentKind> IngestionStorage<E> {
    pub fn new(db: Arc<Environment<E>>) -> Self {
        IngestionStorage { db }
    }

    /// Returns the id of the last block that was inserted in the canonical chain.
    pub fn latest_indexed_block(&self) -> Result<Option<GlobalBlockId>, BlockIngestionError> {
        let txn = self.db.begin_ro_txn()?;
        let mut cursor = txn.open_table::<tables::CanonicalChainTable>()?.cursor()?;
        let block_id = match cursor.last()? {
            None => None,
            Some((number, hash)) => {
                let hash = (&hash).try_into()?;
                Some(GlobalBlockId::new(number, hash))
            }
        };
        txn.commit()?;
        Ok(block_id)
    }

    pub fn canonical_block_id_by_number(
        &self,
        number: u64,
    ) -> Result<GlobalBlockId, BlockIngestionError> {
        let txn = self.db.begin_ro_txn()?;
        let mut cursor = txn.open_table::<tables::CanonicalChainTable>()?.cursor()?;
        let (_, block_hash) = cursor
            .seek_exact(&number)?
            .ok_or(BlockIngestionError::BlockNotCanonical)?;
        let block_hash = (&block_hash).try_into()?;
        let block_id = GlobalBlockId::new(number, block_hash);
        txn.commit()?;
        Ok(block_id)
    }

    /// Returns the id of the last block that was finalized.
    pub fn latest_finalized_block(&self) -> Result<Option<GlobalBlockId>, BlockIngestionError> {
        let txn = self.db.begin_ro_txn()?;
        let mut canon_cursor = txn.open_table::<tables::CanonicalChainTable>()?.cursor()?;
        let mut status_cursor = txn.open_table::<tables::BlockStatusTable>()?.cursor()?;
        let mut maybe_block_id = canon_cursor.last()?;
        while let Some((block_num, block_hash)) = maybe_block_id {
            let block_hash = (&block_hash).try_into()?;
            let block_id = GlobalBlockId::new(block_num, block_hash);
            let (_, status) = status_cursor
                .seek_exact(&block_id)?
                .ok_or(BlockIngestionError::InconsistentDatabase)?;

            if status.status().is_finalized() {
                txn.commit()?;
                return Ok(Some(block_id));
            }

            maybe_block_id = canon_cursor.prev()?;
        }
        txn.commit()?;
        Ok(None)
    }

    pub fn update_canonical_block(
        &self,
        global_id: &GlobalBlockId,
    ) -> Result<(), BlockIngestionError> {
        let number = global_id.number();
        let txn = self.db.begin_rw_txn()?;
        let mut cursor = txn.open_table::<tables::CanonicalChainTable>()?.cursor()?;
        let hash = global_id.hash().into();
        cursor.seek_exact(&number)?;
        cursor.put(&number, &hash)?;
        txn.commit()?;
        Ok(())
    }

    pub fn begin_txn(&self) -> Result<BlockWriter<'_, '_, E>, BlockIngestionError> {
        let txn = self.db.begin_rw_txn()?;
        let status_cursor = txn.open_table::<tables::BlockStatusTable>()?.cursor()?;
        let header_cursor = txn.open_table::<tables::BlockHeaderTable>()?.cursor()?;
        let body_cursor = txn.open_table::<tables::BlockBodyTable>()?.cursor()?;
        let receipts_cursor = txn.open_table::<tables::BlockReceiptsTable>()?.cursor()?;
        let state_update_cursor = txn.open_table::<tables::StateUpdateTable>()?.cursor()?;
        let writer = BlockWriter {
            txn,
            status_cursor,
            header_cursor,
            body_cursor,
            receipts_cursor,
            state_update_cursor,
        };
        Ok(writer)
    }
}

impl<'env, 'txn, E: EnvironmentKind> BlockWriter<'env, 'txn, E> {
    pub fn commit(self) -> Result<(), BlockIngestionError> {
        self.txn.commit()?;
        Ok(())
    }

    #[tracing::instrument(skip(self, status))]
    pub fn write_status(
        &mut self,
        global_id: &GlobalBlockId,
        status: v1alpha2::BlockStatus,
    ) -> Result<(), BlockIngestionError> {
        let status_v = db::BlockStatus {
            status: status as i32,
        };
        self.status_cursor.seek_exact(global_id)?;
        self.status_cursor.put(global_id, &status_v)?;
        Ok(())
    }

    #[tracing::instrument(skip(self, header))]
    pub fn write_header(
        &mut self,
        global_id: &GlobalBlockId,
        header: v1alpha2::BlockHeader,
    ) -> Result<(), BlockIngestionError> {
        self.header_cursor.seek_exact(global_id)?;
        self.header_cursor.put(global_id, &header)?;
        Ok(())
    }

    #[tracing::instrument(skip(self, transactions))]
    pub fn write_body(
        &mut self,
        global_id: &GlobalBlockId,
        transactions: Vec<v1alpha2::Transaction>,
    ) -> Result<(), BlockIngestionError> {
        let body = db::BlockBody { transactions };
        self.body_cursor.seek_exact(global_id)?;
        self.body_cursor.put(global_id, &body)?;
        Ok(())
    }

    #[tracing::instrument(skip(self, receipts))]
    pub fn write_receipts(
        &mut self,
        global_id: &GlobalBlockId,
        receipts: Vec<v1alpha2::TransactionReceipt>,
    ) -> Result<(), BlockIngestionError> {
        let body = db::BlockReceipts { receipts };
        self.receipts_cursor.seek_exact(global_id)?;
        self.receipts_cursor.put(global_id, &body)?;
        Ok(())
    }

    #[tracing::instrument(skip(self, update))]
    pub fn write_state_update(
        &mut self,
        global_id: &GlobalBlockId,
        update: v1alpha2::StateUpdate,
    ) -> Result<(), BlockIngestionError> {
        self.state_update_cursor.seek_exact(global_id)?;
        self.state_update_cursor.put(global_id, &update)?;
        Ok(())
    }
}
