use std::{path::PathBuf, sync::Arc};

use apibara_node::db::{
    libmdbx::{Environment, Error as MdxError, NoWriteMap},
    MdbxEnvironmentExt, MdbxTransactionExt,
};
use ethers_core::types::H256;

use super::{tables, Forkchoice};

pub struct ErigonDB {
    db: Arc<Environment<NoWriteMap>>,
}

#[derive(Debug, thiserror::Error)]
pub enum ErigonDBError {
    #[error(transparent)]
    Db(#[from] MdxError),
}

impl ErigonDB {
    pub fn open(dir: &PathBuf) -> Result<Self, ErigonDBError> {
        let db = Environment::<NoWriteMap>::builder().open(dir)?;

        Ok(Self { db: Arc::new(db) })
    }

    pub fn read_forkchoice(&self, choice: &Forkchoice) -> Result<Option<H256>, ErigonDBError> {
        let txn = self.db.begin_ro_txn()?;
        println!("HERE");
        let mut curr = txn.open_cursor::<tables::LastForkchoiceTable>()?;
        println!("NOT HERE");
        let xxx = curr.first().unwrap();
        println!("xxx = {:?}", xxx.is_some());
        let hash = curr.seek_exact(choice)?.map(|t| t.1 .0);
        txn.commit()?;
        Ok(hash)
    }

    pub fn test(&self) -> Result<(), ErigonDBError> {
        let txn = self.db.begin_ro_txn()?;
        let mut curr = txn.open_cursor::<tables::HeaderTable>()?;
        let xxx = curr.first().unwrap();
        println!("xxx = {:?}", xxx.is_some());
        txn.commit()?;
        todo!()
    }
}
