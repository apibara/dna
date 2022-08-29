//! Manage persistent state.

use std::sync::Arc;

use apibara_node::db::{
    libmdbx::{Environment, EnvironmentKind, Error as MdbxError},
    MdbxRWTransactionExt, MdbxTransactionExt,
};
use starknet::core::types::FieldElement;

use crate::db::tables;

pub struct StateStorage<E: EnvironmentKind> {
    db: Arc<Environment<E>>,
}

#[derive(Debug, thiserror::Error)]
pub enum StateStorageError {
    #[error("database error")]
    Database(#[from] MdbxError),
}

pub type Result<T> = std::result::Result<T, StateStorageError>;

impl<E> StateStorage<E>
where
    E: EnvironmentKind,
{
    pub fn new(db: Arc<Environment<E>>) -> Result<Self> {
        let txn = db.begin_rw_txn()?;
        txn.ensure_table::<tables::NodeStateTable>(None)?;
        txn.commit()?;
        Ok(StateStorage { db })
    }

    pub fn update_state(&self, block_number: u64, block_hash: Vec<u8>) -> Result<()> {
        let new_state = tables::NodeState {
            block_number,
            block_hash,
        };

        let txn = self.db.begin_rw_txn()?;
        let mut cursor = txn.open_table::<tables::NodeStateTable>()?.cursor()?;
        cursor.put(&(), &new_state)?;
        txn.commit()?;
        Ok(())
    }

    pub fn get_state(&self) -> Result<Option<tables::NodeState>> {
        let txn = self.db.begin_ro_txn()?;
        let db = txn.open_table::<tables::NodeStateTable>()?;
        let mut cursor = db.cursor()?;
        let state = cursor.seek_exact(&())?.map(|d| d.1);
        txn.commit()?;
        Ok(state)
    }
}
