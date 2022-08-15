//! Type-safe database access.

use std::io::Cursor;

use apibara_core::stream::{Sequence, StreamId};
use byteorder::{BigEndian, ReadBytesExt};
use prost::Message;

#[derive(Debug, thiserror::Error)]
pub enum KeyDecodeError {
    #[error("invalid key bytes size")]
    InvalidByteSize { expected: usize, actual: usize },
    #[error("error reading key from bytes")]
    ReadError(#[from] std::io::Error),
}

pub trait TableKey: Send + Sync + Sized {
    type Encoded: AsRef<[u8]> + Send + Sync;

    fn encode(&self) -> Self::Encoded;
    fn decode(b: &[u8]) -> Result<Self, KeyDecodeError>;
}

pub trait Table: Send + Sync {
    type Key: TableKey;
    type Value: Message + Default;

    fn db_name() -> &'static str;
}

pub trait DupSortTable: Table {}

impl TableKey for StreamId {
    type Encoded = [u8; 8];

    fn encode(&self) -> Self::Encoded {
        self.as_u64().to_be_bytes()
    }

    fn decode(b: &[u8]) -> Result<Self, KeyDecodeError> {
        if b.len() != 8 {
            return Err(KeyDecodeError::InvalidByteSize {
                expected: 8,
                actual: b.len(),
            });
        }
        let mut cursor = Cursor::new(b);
        let stream_id = cursor
            .read_u64::<BigEndian>()
            .map_err(KeyDecodeError::ReadError)?;
        Ok(StreamId::from_u64(stream_id))
    }
}

impl TableKey for Sequence {
    type Encoded = [u8; 8];

    fn encode(&self) -> Self::Encoded {
        self.as_u64().to_be_bytes()
    }

    fn decode(b: &[u8]) -> Result<Self, KeyDecodeError> {
        if b.len() != 8 {
            return Err(KeyDecodeError::InvalidByteSize {
                expected: 8,
                actual: b.len(),
            });
        }
        let mut cursor = Cursor::new(b);
        let sequence = cursor
            .read_u64::<BigEndian>()
            .map_err(KeyDecodeError::ReadError)?;
        Ok(Sequence::from_u64(sequence))
    }
}

impl TableKey for (StreamId, Sequence) {
    type Encoded = [u8; 16];

    fn encode(&self) -> Self::Encoded {
        let mut out = [0; 16];
        out[..8].copy_from_slice(&self.0.encode());
        out[8..].copy_from_slice(&self.1.encode());
        out
    }

    fn decode(b: &[u8]) -> Result<Self, KeyDecodeError> {
        if b.len() != 16 {
            return Err(KeyDecodeError::InvalidByteSize {
                expected: 16,
                actual: b.len(),
            });
        }
        let mut cursor = Cursor::new(b);
        let stream_id = cursor
            .read_u64::<BigEndian>()
            .map_err(KeyDecodeError::ReadError)?;
        let sequence = cursor
            .read_u64::<BigEndian>()
            .map_err(KeyDecodeError::ReadError)?;
        Ok((StreamId::from_u64(stream_id), Sequence::from_u64(sequence)))
    }
}
