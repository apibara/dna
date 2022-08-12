//! # Node Database
//!
//! This module provides all the abstractions over storage.
mod mdbx;
mod sequencer;
mod table;

pub use self::mdbx::{MdbxEnvironmentExt, MdbxRWTransactionExt, MdbxTable, MdbxTransactionExt};
pub use self::table::{Table, TableKey};

pub mod tables {
    pub use super::sequencer::{
        SequencerInputTable, SequencerInputToOutputTable, SequencerOutputTriggerTable,
    };
}
