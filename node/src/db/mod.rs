//! # Node Database
//!
//! This module provides all the abstractions over storage.
mod cli;
mod mdbx;
mod table;

pub use self::cli::default_data_dir;
pub use self::mdbx::{
    MdbxEnvironmentExt, MdbxErrorExt, MdbxRWTransactionExt, MdbxTable, MdbxTransactionExt,
    TableCursor,
};
pub use self::table::{ByteVec, Decodable, DecodeError, DupSortTable, Encodable, Table, TableKey};

pub mod libmdbx {
    pub use libmdbx::*;
}
