//! # Node Database
//!
//! This module provides all the abstractions over storage.
mod cli;
mod head_tracker;
mod mdbx;
mod sequencer;
mod table;

pub use self::cli::{node_data_dir, DatabaseClapCommandExt};
pub use self::mdbx::{
    MdbxEnvironmentExt, MdbxRWTransactionExt, MdbxTable, MdbxTransactionExt, TableCursor,
};
pub use self::table::{ByteVec, DupSortTable, KeyDecodeError, Table, TableKey};

pub mod tables {
    pub use super::head_tracker::{
        BlockHash, BlockHeader, BlockHeaderTable, CanonicalBlockHeader, CanonicalBlockHeaderTable,
    };
    pub use super::sequencer::{SequencerState, SequencerStateTable};
}

pub mod libmdbx {
    pub use libmdbx::*;
}
