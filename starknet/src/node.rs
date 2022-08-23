//! # StarkNet source node
//!
//! This node indexes all StarkNet blocks and produces a stream of
//! blocks with transaction data.
use std::{sync::Arc, time::Duration};

use apibara_node::db::{
    libmdbx::{Environment, EnvironmentKind, Error as MdbxError},
    MdbxRWTransactionExt, MdbxTransactionExt,
};
use tokio::task::JoinHandle;
use tracing::info;

use crate::db::tables;

#[derive(Debug)]
pub struct StarkNetSourceNode<E: EnvironmentKind> {
    db: Arc<Environment<E>>,
}

#[derive(Debug, thiserror::Error)]
pub enum SourceNodeError {
    #[error("database error")]
    Database(#[from] MdbxError),
}

pub type Result<T> = std::result::Result<T, SourceNodeError>;

impl<E: EnvironmentKind> StarkNetSourceNode<E> {
    pub fn new(db: Arc<Environment<E>>) -> Result<Self> {
        let txn = db.begin_rw_txn()?;
        txn.ensure_table::<tables::NodeStateTable>(None)?;
        txn.commit()?;
        Ok(StarkNetSourceNode { db })
    }

    pub async fn start(self) -> Result<()> {
        // Check latest indexed block.
        // - compare stored hash with live hash to detect reorgs
        //   while node was not running.
        let state = self.get_state()?;
        info!("state = {:?}", state);
        todo!()
    }

    pub fn get_state(&self) -> Result<tables::NodeState> {
        let txn = self.db.begin_ro_txn()?;
        let db = txn.open_table::<tables::NodeStateTable>()?;
        let mut cursor = db.cursor()?;
        let state = cursor.seek_exact(&())?.map(|d| d.1).unwrap_or_default();
        txn.commit()?;
        Ok(state)
    }
}
