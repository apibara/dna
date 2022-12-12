//! # Node Database
//!
//! This module provides all the abstractions over storage.
mod chain_tracker;
mod cli;
mod mdbx;
mod message_storage;
mod node;
mod sequencer;
mod table;

pub use self::cli::{node_data_dir, DatabaseClapCommandExt};
pub use self::mdbx::{
    MdbxEnvironmentExt, MdbxErrorExt, MdbxRWTransactionExt, MdbxTable, MdbxTransactionExt,
    TableCursor,
};
pub use self::table::{ByteVec, DupSortTable, KeyDecodeError, Table, TableKey};

pub mod tables {
    pub use super::chain_tracker::{
        Block, BlockHash, BlockTable, CanonicalBlock, CanonicalBlockTable,
    };
    pub use super::message_storage::MessageTable;
    pub use super::node::{Configuration, ConfigurationTable};
    pub use super::sequencer::{
        SequencerState, SequencerStateTable, StreamState, StreamStateTable,
    };
}

pub mod libmdbx {
    pub use libmdbx::*;
}
