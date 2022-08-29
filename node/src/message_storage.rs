//! Store messages in mdbx

use std::{marker::PhantomData, sync::Arc};

use apibara_core::stream::Sequence;
use libmdbx::{Environment, EnvironmentKind, Error as MdbxError, Transaction, RO};
use prost::Message;

use crate::db::{tables, MdbxRWTransactionExt, MdbxTransactionExt, TableCursor};

/// Store messages in mdbx.
pub struct MessageStorage<E: EnvironmentKind, M: Message> {
    db: Arc<Environment<E>>,
    phantom: PhantomData<M>,
}

/// [MessageStorage]-related error.
#[derive(Debug, thiserror::Error)]
pub enum MessageStorageError {
    #[error("message has the wrong sequence number")]
    InvalidMessageSequence { expected: u64, actual: u64 },
    #[error("error originating from database")]
    Database(#[from] MdbxError),
}

pub type Result<T> = std::result::Result<T, MessageStorageError>;

pub struct MessageIterator<'txn, E: EnvironmentKind, M: Message + Default> {
    _txn: Transaction<'txn, RO, E>,
    current: Option<Result<M>>,
    cursor: TableCursor<'txn, tables::MessageTable<M>, RO>,
}

impl<E, M> MessageStorage<E, M>
where
    E: EnvironmentKind,
    M: Message + Default,
{
    /// Create a new message store, persisting data to the given mdbx environment.
    pub fn new(db: Arc<Environment<E>>) -> Result<Self> {
        let txn = db.begin_rw_txn()?;
        txn.ensure_table::<tables::MessageTable<M>>(None)?;
        txn.commit()?;
        Ok(MessageStorage {
            db,
            phantom: PhantomData::default(),
        })
    }

    /// Insert the given `message` in the store.
    ///
    /// Expect `sequence` to be the successor of the current highest sequence number.
    pub fn insert(&self, sequence: &Sequence, message: &M) -> Result<()> {
        let txn = self.db.begin_rw_txn()?;
        let table = txn.open_table::<tables::MessageTable<M>>()?;
        let mut cursor = table.cursor()?;

        match cursor.last()? {
            None => {
                // First element, assert sequence is 0
                if sequence.as_u64() != 0 {
                    return Err(MessageStorageError::InvalidMessageSequence {
                        expected: 0,
                        actual: sequence.as_u64(),
                    });
                }
                cursor.put(sequence, message)?;
                txn.commit()?;
                Ok(())
            }
            Some((prev_sequence, _)) => {
                if sequence.as_u64() != prev_sequence.as_u64() + 1 {
                    return Err(MessageStorageError::InvalidMessageSequence {
                        expected: prev_sequence.as_u64() + 1,
                        actual: sequence.as_u64(),
                    });
                }
                cursor.put(sequence, message)?;
                txn.commit()?;
                Ok(())
            }
        }
    }

    /// Delete all messages with sequence number greater than or equal the given `sequence`.
    ///
    /// Returns the number of messages deleted.
    pub fn invalidate(&self, sequence: &Sequence) -> Result<usize> {
        let txn = self.db.begin_rw_txn()?;
        let table = txn.open_table::<tables::MessageTable<M>>()?;
        let mut cursor = table.cursor()?;

        let mut count = 0;
        loop {
            match cursor.last()? {
                None => break,
                Some((key, _)) => {
                    if key.as_u64() < sequence.as_u64() {
                        break;
                    }
                    cursor.del()?;
                    count += 1;
                }
            }
        }
        txn.commit()?;
        Ok(count)
    }

    /// Returns an iterator over all messages, starting at the given `start` index.
    pub fn iter_from(&self, start: &Sequence) -> Result<MessageIterator<'_, E, M>> {
        let txn = self.db.begin_ro_txn()?;
        let table = txn.open_table::<tables::MessageTable<M>>()?;
        let mut cursor = table.cursor()?;
        let current = cursor.seek_exact(start)?.map(|v| Ok(v.1));
        Ok(MessageIterator {
            cursor,
            _txn: txn,
            current,
        })
    }
}

impl<'txn, E, M> Iterator for MessageIterator<'txn, E, M>
where
    E: EnvironmentKind,
    M: Message + Default,
{
    type Item = Result<M>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current.take() {
            None => None,
            Some(value) => {
                self.current = match self.cursor.next() {
                    Err(err) => Some(Err(err.into())),
                    Ok(None) => None,
                    Ok(Some(value)) => Some(Ok(value.1)),
                };
                Some(value)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use apibara_core::stream::Sequence;
    use libmdbx::{Environment, NoWriteMap};
    use prost::Message;
    use tempfile::tempdir;

    use crate::db::MdbxEnvironmentExt;

    use super::MessageStorage;

    #[derive(Clone, PartialEq, Message)]
    pub struct Transfer {
        #[prost(string, tag = "1")]
        pub sender: String,
        #[prost(string, tag = "2")]
        pub receiver: String,
    }

    #[test]
    pub fn test_message_storage() {
        let path = tempdir().unwrap();
        let db = Environment::<NoWriteMap>::open(path.path()).unwrap();
        let storage = MessageStorage::<_, Transfer>::new(Arc::new(db)).unwrap();

        // first message must have index 0
        let t0 = Transfer {
            sender: "ABC".to_string(),
            receiver: "XYZ".to_string(),
        };
        assert!(storage.insert(&Sequence::from_u64(1), &t0).is_err());
        storage.insert(&Sequence::from_u64(0), &t0).unwrap();

        // next message must have index 1
        let t1 = Transfer {
            sender: "AOE".to_string(),
            receiver: "TNS".to_string(),
        };
        assert!(storage.insert(&Sequence::from_u64(0), &t1).is_err());
        assert!(storage.insert(&Sequence::from_u64(2), &t1).is_err());
        storage.insert(&Sequence::from_u64(1), &t1).unwrap();

        let all_messages = storage
            .iter_from(&Sequence::from_u64(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert!(all_messages.len() == 2);
        assert!(all_messages[0] == t0);
        assert!(all_messages[1] == t1);

        // invalidate latest message
        let count = storage.invalidate(&Sequence::from_u64(1)).unwrap();
        assert!(count == 1);
        // second time is a noop
        let count = storage.invalidate(&Sequence::from_u64(1)).unwrap();
        assert!(count == 0);

        let all_messages = storage
            .iter_from(&Sequence::from_u64(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(all_messages.len() == 1);
        assert!(all_messages[0] == t0);

        // insert value again
        assert!(storage.insert(&Sequence::from_u64(0), &t1).is_err());
        assert!(storage.insert(&Sequence::from_u64(2), &t1).is_err());
        storage.insert(&Sequence::from_u64(1), &t1).unwrap();

        let all_messages = storage
            .iter_from(&Sequence::from_u64(1))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(all_messages.len() == 1);
        assert!(all_messages[0] == t1);
    }
}
