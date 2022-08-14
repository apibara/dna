//! Sequencer-related tables.

use apibara_core::stream::{Sequence, StreamId};
use libmdbx::{EnvironmentKind, Error as MdbxError, TransactionKind};
use prost::Message;

use super::{DupSortTable, MdbxTable, Table};

/// Table with the state of each input sequence, together with the respective
/// output range.
#[derive(Debug, Clone, Copy, Default)]
pub struct SequencerStateTable;

/// Store the input's sequence, together with the start and end (inclusive)
/// output range.
///
/// Since the output sequence is strictly increasing, we can use the input
/// sequence to order the state and that will also keep the output's sequence
/// ordered.
///
/// Mark fields as optional to enforce serializing the `0` value.
#[derive(Clone, PartialEq, Message)]
pub struct SequencerState {
    #[prost(fixed64, optional, tag = "1")]
    pub input_sequence: Option<u64>,
    #[prost(fixed64, optional, tag = "2")]
    pub output_sequence_start: Option<u64>,
    #[prost(fixed64, optional, tag = "3")]
    pub output_sequence_end: Option<u64>,
}

impl Table for SequencerStateTable {
    type Key = StreamId;
    type Value = SequencerState;

    fn db_name() -> &'static str {
        "SequencerState"
    }
}

impl DupSortTable for SequencerStateTable {}

#[cfg(test)]
mod tests {
    use apibara_core::stream::StreamId;
    use libmdbx::{DatabaseFlags, Environment, EnvironmentKind, NoWriteMap};
    use std::sync::Arc;
    use tempfile::tempdir;

    use crate::db::{MdbxEnvironmentExt, MdbxRWTransactionExt, MdbxTransactionExt};

    use super::{SequencerState, SequencerStateTable};

    #[test]
    fn test_state_order() {
        let path = tempdir().unwrap();
        let db = Environment::<NoWriteMap>::open(path.path()).unwrap();
        let stream_id = StreamId::from_u64(1);
        let value_low = SequencerState {
            input_sequence: Some(0),
            output_sequence_start: Some(0),
            output_sequence_end: Some(1),
        };
        let value_mid = SequencerState {
            input_sequence: Some(1),
            output_sequence_start: Some(2),
            output_sequence_end: Some(2),
        };
        let value_high = SequencerState {
            input_sequence: Some(2),
            output_sequence_start: Some(3),
            output_sequence_end: Some(4),
        };

        let txn = db.begin_rw_txn().unwrap();
        txn.ensure_table::<SequencerStateTable>(Some(DatabaseFlags::DUP_SORT));
        let table = txn.open_table::<SequencerStateTable>().unwrap();
        let mut cursor = table.cursor().unwrap();
        cursor.first().unwrap();
        // insert three values in the wrong order, then check if they're stored in order.
        cursor.put(&stream_id, &value_mid).unwrap();
        cursor.put(&stream_id, &value_high).unwrap();
        cursor.put(&stream_id, &value_low).unwrap();
        txn.commit().unwrap();

        let txn = db.begin_ro_txn().unwrap();
        let table = txn.open_table::<SequencerStateTable>().unwrap();
        let mut cursor = table.cursor().unwrap();
        let (key, value) = cursor.seek_exact(&stream_id).unwrap().unwrap();
        assert!(key.as_u64() == 1);
        assert!(value.input_sequence == Some(0));
        let (key, value) = cursor.next_dup().unwrap().unwrap();
        assert!(key.as_u64() == 1);
        assert!(value.input_sequence == Some(1));
        let (key, value) = cursor.next_dup().unwrap().unwrap();
        assert!(key.as_u64() == 1);
        assert!(value.input_sequence == Some(2));
        let value = cursor.next_dup().unwrap();
        assert!(value.is_none());
        txn.commit().unwrap();
    }
}
