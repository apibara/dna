mod block;
mod chain;
mod state;
mod storage;
mod transaction;

pub use self::block::BlockStatus;
pub use self::storage::{DatabaseStorage, DatabaseStorageWriter, StorageReader, StorageWriter};
pub use self::transaction::{BlockBody, BlockReceipts};

pub mod tables {
    use apibara_node::db::libmdbx::{EnvironmentKind, Error as MdbxError, Transaction, RW};
    use apibara_node::db::MdbxRWTransactionExt;

    pub use super::block::{BlockHeaderTable, BlockStatusTable};
    pub use super::chain::CanonicalChainTable;
    pub use super::state::StateUpdateTable;
    pub use super::transaction::{BlockBodyTable, BlockReceiptsTable};

    /// Ensures all tables exist.
    pub fn ensure<E: EnvironmentKind>(txn: &Transaction<RW, E>) -> Result<(), MdbxError> {
        txn.ensure_table::<self::BlockBodyTable>(None)?;
        txn.ensure_table::<self::BlockHeaderTable>(None)?;
        txn.ensure_table::<self::BlockStatusTable>(None)?;
        txn.ensure_table::<self::CanonicalChainTable>(None)?;
        txn.ensure_table::<self::BlockReceiptsTable>(None)?;
        txn.ensure_table::<self::StateUpdateTable>(None)?;
        Ok(())
    }
}
