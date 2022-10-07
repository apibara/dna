//! Sequencer-related tables.

use apibara_core::stream::{Sequence, StreamId};
use prost::Message;

use super::Table;

/// Table with the state of each input sequence, together with the respective
/// output range.
#[derive(Debug, Clone, Copy, Default)]
pub struct SequencerStateTable;

/// Store the output's start and end sequence for a given stream id and input
/// sequence.
///
/// Since the output sequence is strictly increasing, we can use the input
/// sequence to order the state and that will also keep the output's sequence
/// ordered.
///
/// Mark fields as optional to enforce serializing the `0` value.
#[derive(Clone, PartialEq, Message)]
pub struct SequencerState {
    #[prost(fixed64, optional, tag = "1")]
    pub output_sequence_start: Option<u64>,
    #[prost(fixed64, optional, tag = "2")]
    pub output_sequence_end: Option<u64>,
}

impl Table for SequencerStateTable {
    type Key = (StreamId, Sequence);
    type Value = SequencerState;

    fn db_name() -> &'static str {
        "SequencerState"
    }
}

/// Table with the state of each input stream.
#[derive(Debug, Clone, Copy, Default)]
pub struct StreamStateTable;

/// Contains the most recent sequence number of each input stream.
#[derive(Clone, PartialEq, Message)]
pub struct StreamState {
    #[prost(fixed64, optional, tag = "1")]
    pub sequence: Option<u64>,
}

impl Table for StreamStateTable {
    type Key = StreamId;
    type Value = StreamState;

    fn db_name() -> &'static str {
        "StreamState"
    }
}

#[cfg(test)]
mod tests {
    use apibara_core::stream::{Sequence, StreamId};
    use libmdbx::{Environment, NoWriteMap};
    use tempfile::tempdir;

    use crate::db::{
        sequencer::StreamStateTable, MdbxEnvironmentExt, MdbxRWTransactionExt, MdbxTransactionExt,
    };

    use super::{SequencerState, SequencerStateTable};

    #[test]
    fn test_state_order() {
        let path = tempdir().unwrap();
        let db = Environment::<NoWriteMap>::open(path.path()).unwrap();
        let stream_id = StreamId::from_u64(1);
        let value_low = SequencerState {
            output_sequence_start: Some(0),
            output_sequence_end: Some(1),
        };
        let value_mid = SequencerState {
            output_sequence_start: Some(2),
            output_sequence_end: Some(2),
        };
        let value_high = SequencerState {
            output_sequence_start: Some(3),
            output_sequence_end: Some(4),
        };

        let txn = db.begin_rw_txn().unwrap();
        txn.ensure_table::<SequencerStateTable>(None).unwrap();
        txn.ensure_table::<StreamStateTable>(None).unwrap();
        let table = txn.open_table::<SequencerStateTable>().unwrap();
        let mut cursor = table.cursor().unwrap();
        cursor.first().unwrap();
        // insert three values in the wrong order, then check if they're stored in order.
        cursor
            .put(&(stream_id, Sequence::from_u64(1)), &value_mid)
            .unwrap();
        cursor
            .put(&(stream_id, Sequence::from_u64(2)), &value_high)
            .unwrap();
        cursor
            .put(&(stream_id, Sequence::from_u64(0)), &value_low)
            .unwrap();
        txn.commit().unwrap();

        let txn = db.begin_ro_txn().unwrap();
        let table = txn.open_table::<SequencerStateTable>().unwrap();
        let mut cursor = table.cursor().unwrap();
        let ((stream_id, input_seq), _value) = cursor
            .seek_range(&(stream_id, Sequence::from_u64(0)))
            .unwrap()
            .unwrap();
        assert_eq!(stream_id.as_u64(), 1);
        assert_eq!(input_seq.as_u64(), 0);

        let ((stream_id, input_seq), _value) = cursor.next().unwrap().unwrap();
        assert_eq!(stream_id.as_u64(), 1);
        assert_eq!(input_seq.as_u64(), 1);

        let ((stream_id, input_seq), _value) = cursor.next().unwrap().unwrap();
        assert_eq!(stream_id.as_u64(), 1);
        assert_eq!(input_seq.as_u64(), 2);

        let value = cursor.next().unwrap();
        assert!(value.is_none());
        txn.commit().unwrap();
    }
}
