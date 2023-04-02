//! Type-safe database access.

use std::io::Cursor;

use apibara_core::stream::{Sequence, StreamId};
use arrayvec::ArrayVec;
use byteorder::{BigEndian, ReadBytesExt};

/// Error related to decoding keys.
#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("invalid key bytes size")]
    InvalidByteSize { expected: usize, actual: usize },
    #[error("error reading key from bytes")]
    ReadError(#[from] std::io::Error),
    #[error("Other type of error")]
    Other(Box<dyn std::error::Error + Send + Sync + 'static>),
}

/// A fixed-capacity vector of bytes.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct ByteVec<const CAP: usize>(ArrayVec<u8, CAP>);

pub trait Encodable {
    type Encoded: AsRef<[u8]> + Send + Sync;

    fn encode(&self) -> Self::Encoded;
}

pub trait Decodable: Sized {
    fn decode(b: &[u8]) -> Result<Self, DecodeError>;
}

pub trait TableKey: Decodable + Encodable + Send + Sync {}

impl<T> TableKey for T where T: Encodable + Decodable + Send + Sync {}

pub trait Table: Send + Sync {
    type Key: TableKey;
    type Value: Default + Clone;

    fn db_name() -> &'static str;
}

pub trait DupSortTable: Table {}

impl<const CAP: usize> AsRef<[u8]> for ByteVec<CAP> {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Encodable for StreamId {
    type Encoded = [u8; 8];

    fn encode(&self) -> Self::Encoded {
        self.as_u64().to_be_bytes()
    }
}

impl Decodable for StreamId {
    fn decode(b: &[u8]) -> Result<Self, DecodeError> {
        if b.len() != 8 {
            return Err(DecodeError::InvalidByteSize {
                expected: 8,
                actual: b.len(),
            });
        }
        let mut cursor = Cursor::new(b);
        let stream_id = cursor
            .read_u64::<BigEndian>()
            .map_err(DecodeError::ReadError)?;
        Ok(StreamId::from_u64(stream_id))
    }
}

impl Encodable for Sequence {
    type Encoded = [u8; 8];

    fn encode(&self) -> Self::Encoded {
        self.as_u64().to_be_bytes()
    }
}

impl Decodable for Sequence {
    fn decode(b: &[u8]) -> Result<Self, DecodeError> {
        if b.len() != 8 {
            return Err(DecodeError::InvalidByteSize {
                expected: 8,
                actual: b.len(),
            });
        }
        let mut cursor = Cursor::new(b);
        let sequence = cursor
            .read_u64::<BigEndian>()
            .map_err(DecodeError::ReadError)?;
        Ok(Sequence::from_u64(sequence))
    }
}

impl Encodable for (StreamId, Sequence) {
    type Encoded = [u8; 16];

    fn encode(&self) -> Self::Encoded {
        let mut out = [0; 16];
        out[..8].copy_from_slice(&self.0.encode());
        out[8..].copy_from_slice(&self.1.encode());
        out
    }
}

impl Decodable for (StreamId, Sequence) {
    fn decode(b: &[u8]) -> Result<Self, DecodeError> {
        if b.len() != 16 {
            return Err(DecodeError::InvalidByteSize {
                expected: 16,
                actual: b.len(),
            });
        }
        let mut cursor = Cursor::new(b);
        let stream_id = cursor
            .read_u64::<BigEndian>()
            .map_err(DecodeError::ReadError)?;
        let sequence = cursor
            .read_u64::<BigEndian>()
            .map_err(DecodeError::ReadError)?;
        Ok((StreamId::from_u64(stream_id), Sequence::from_u64(sequence)))
    }
}

impl Encodable for () {
    type Encoded = [u8; 0];

    fn encode(&self) -> Self::Encoded {
        []
    }
}

impl Decodable for () {
    fn decode(b: &[u8]) -> Result<Self, DecodeError> {
        if !b.is_empty() {
            return Err(DecodeError::InvalidByteSize {
                expected: 0,
                actual: b.len(),
            });
        }
        Ok(())
    }
}

impl Encodable for u64 {
    type Encoded = [u8; 8];

    fn encode(&self) -> Self::Encoded {
        self.to_be_bytes()
    }
}

impl Decodable for u64 {
    fn decode(b: &[u8]) -> Result<Self, DecodeError> {
        if b.len() != 8 {
            return Err(DecodeError::InvalidByteSize {
                expected: 8,
                actual: b.len(),
            });
        }
        let mut cursor = Cursor::new(b);
        cursor
            .read_u64::<BigEndian>()
            .map_err(DecodeError::ReadError)
    }
}

impl Decodable for Vec<u8> {
    fn decode(b: &[u8]) -> Result<Self, DecodeError> {
        Ok(b.to_vec())
    }
}
