//! # Sequencer
//!
//! The sequencer is used to track input and output sequence numbers.
//! All messages in the Apibara protocol contain a sequence number without
//! gaps. Chain reorganizations create an additional issue: the same sequence
//! number can repeat and all data following a reorged block must be
//! invalidated.
//!
//! The sequencer tracks input sequence numbers from multiple sources and
//! associates them with the output sequence number.
//! The sequencer can invalidate its output sequence numbers in response to
//! an input sequence invalidation.
//!
//! ## Example
//!
//! Imagine a system with three inputs `A`, `B`, and `C`.
//! Each input message is handled by an application that produces zero or more
//! output messages.
//! Notice that in this example is not concerned how input messages are received
//! or how outputs are produced. The sequencer is only involved in tracking and
//! mapping sequence numbers.
//!
//! The first message comes from `A` and has sequence `0`, the application produces
//! two messages. The diagram also includes the state of the input and output sequences.
//!
//! ```txt
//!  IN |A 0|
//!
//! OUT |O 0|O 1|
//!
//! INPUT SEQUENCE
//!   A: 0
//!
//! OUTPUT SEQUENCE: 1
//! ```
//!
//! Then it receives another message from `A`, this time producing a single output.
//!
//! ```txt
//!  IN |A 0|   |A 1|
//!
//! OUT |O 0|O 1|O 2|
//!
//! INPUT SEQUENCE
//!   A: 1
//!
//! OUTPUT SEQUENCE: 2
//! ```
//!
//! After several messages the state of the stream is the following:
//!
//! ```txt
//!  IN |A 0|   |A 1|B 0|B 1|A 2|       |C 0|B 2|
//!
//! OUT |O 0|O 1|O 2|   |O 3|O 4|O 5|O 6|O 7|O 8|O 9|
//!
//! INPUT SEQUENCE
//!   A: 2
//!   B: 2
//!   C: 0
//!
//! OUTPUT SEQUENCE: 9
//! ```
//!
//! Imagine that the stream receives a message invalidating all data produced by
//! `B` after (and including) sequence `1`. This is denoted as `Bx1` in the diagram.
//! The sequencer must rollback its state to just before receiving `B 1` for the first
//! time.
//!
//! ```txt
//!  IN |A 0|   |A 1|B 0|B 1|A 2|       |C 0|B 2|   |Bx1|
//!
//! OUT |O 0|O 1|O 2|   |O 3|O 4|O 5|O 6|O 7|O 8|O 9|
//!
//! INPUT SEQUENCE
//!   A: 1
//!   B: 0
//!
//! OUTPUT SEQUENCE: 2
//! ```
//!
//! Then the stream receives the new message `B'1` and operations resume.
//!
//! ```txt
//!  IN |A 0|   |A 1|B 0|B 1|A 2|       |C 0|B 2|   |Bx1|B'1|
//!
//! OUT |O 0|O 1|O 2|   |O 3|O 4|O 5|O 6|O 7|O 8|O 9|   |O 3|
//!
//! INPUT SEQUENCE
//!   A: 1
//!   B: 1
//!
//! OUTPUT SEQUENCE: 3
//! ```

use std::sync::Arc;

use apibara_core::stream::{Sequence, SequenceRange, StreamId};
use libmdbx::{Environment, EnvironmentKind, Error as MdbxError, WriteFlags};

use crate::db::{
    tables, MdbxEnvironmentExt, MdbxRWTransactionExt, MdbxTransactionExt, Table, TableKey,
};

pub struct Sequencer<E: EnvironmentKind> {
    db: Arc<Environment<E>>,
}

#[derive(Debug, thiserror::Error)]
pub enum SequencerError {
    #[error("invalid input stream sequence number")]
    InvalidInputSequence,
    #[error("error originating from database")]
    Database(#[from] MdbxError),
}

pub type Result<T> = std::result::Result<T, SequencerError>;

impl<E: EnvironmentKind> Sequencer<E> {
    /// Create a new sequencer, persisting data to the given mdbx environment.
    pub fn new(db: Arc<Environment<E>>) -> Result<Self> {
        let txn = db.begin_rw_txn()?;
        txn.ensure_table::<tables::SequencerStateTable>(None);
        txn.commit()?;
        Ok(Sequencer { db })
    }

    /// Register a new input message `(stream_id, sequence)` that generates
    /// `output_len` output messages.
    ///
    /// Returns a sequence range for the output.
    pub fn register(
        &mut self,
        stream_id: &StreamId,
        sequence: &Sequence,
        output_len: usize,
    ) -> Result<SequenceRange> {
        let txn = self.db.begin_rw_txn()?;
        txn.commit()?;
        todo!()
    }

    /// Invalidates all messages received after (inclusive) `(stream_id, sequence)`.
    ///
    /// Returns the sequence number of the first invalidated messages of the output stream.
    pub fn invalidate(&mut self, stream_id: &StreamId, sequence: &Sequence) -> Result<Sequence> {
        todo!()
    }

    /// Returns the start sequence of the next output message.
    pub fn output_sequence_start(&self) -> Sequence {
        todo!()
    }

    /// Returns the latest/current sequence of the given input `stream_id`.
    pub fn input_sequence(&self, stream_id: &StreamId) -> Result<Option<Sequence>> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::{env, sync::Arc};

    use apibara_core::stream::{Sequence, StreamId};
    use libmdbx::{Environment, EnvironmentKind, NoWriteMap};

    use crate::db::MdbxEnvironmentExt;

    use super::Sequencer;

    fn create_db() -> Environment<NoWriteMap> {
        let path = env::temp_dir();
        Environment::<NoWriteMap>::open(path.as_path()).unwrap()
    }

    /*
    #[tokio::test]
    pub async fn test_sequencer() {
        let db = Arc::new(create_db());
        let mut sequencer = Sequencer::new(db).unwrap();

        let s_a = StreamId::from_u64(0);
        let s_b = StreamId::from_u64(1);
        let s_c = StreamId::from_u64(2);

        let output_range = sequencer.register(&s_a, &Sequence::from_u64(0), 2).unwrap();
        assert!(sequencer.output_sequence_start().as_u64() == 1);
        assert!(sequencer.input_sequence(&s_a).unwrap().unwrap().as_u64() == 1);
        assert!(sequencer.input_sequence(&s_b).unwrap().is_none());
        assert!(sequencer.input_sequence(&s_c).unwrap().is_none());
    }
    */
}
