//! Sequencer-related tables.

use apibara_core::stream::{Sequence, StreamId};
use libmdbx::{EnvironmentKind, Error as MdbxError, TransactionKind};
use prost::Message;

use super::{MdbxTable, Table};

/// Table that maps input's sequence to the output start sequence.
#[derive(Debug, Clone, Copy, Default)]
pub struct SequencerInputToOutputTable;

#[derive(Clone, PartialEq, Message)]
pub struct SequencerInputToOutput {}

/// Table that tracks input's sequence number.
#[derive(Debug, Clone, Copy, Default)]
pub struct SequencerInputTable;

#[derive(Clone, PartialEq, Message)]
pub struct SequencerInput {
    #[prost(uint64)]
    pub sequence: u64,
}

/// Table that tracks what input trigger the output starting at sequence.
#[derive(Debug, Clone, Copy, Default)]
pub struct SequencerOutputTriggerTable;

#[derive(Clone, PartialEq, Message)]
pub struct SequencerOutputTrigger {}

impl Table for SequencerInputTable {
    type Key = StreamId;
    type Value = SequencerInput;

    fn db_name() -> &'static str {
        "SequencerInput"
    }
}

impl Table for SequencerInputToOutputTable {
    type Key = (StreamId, Sequence);
    type Value = SequencerInputToOutput;

    fn db_name() -> &'static str {
        "SequencerInputToOutput"
    }
}

impl Table for SequencerOutputTriggerTable {
    type Key = Sequence;
    type Value = SequencerOutputTrigger;

    fn db_name() -> &'static str {
        "SequencerOutputTrigger"
    }
}

impl From<&Sequence> for SequencerInput {
    fn from(seq: &Sequence) -> Self {
        SequencerInput {
            sequence: seq.as_u64(),
        }
    }
}
