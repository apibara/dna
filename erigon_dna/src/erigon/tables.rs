use std::io::Cursor;

use apibara_node::db::{KeyDecodeError, Table, TableKey};
use byteorder::{BigEndian, ReadBytesExt};
use ethers_core::types::H256;
use reth_rlp::Decodable;

use crate::erigon::types::{BlockHash, Forkchoice, GlobalBlockId, Header};

/// Map block numbers to block hashes.
#[derive(Clone, Debug, Copy, Default)]
pub struct CanonicalHeaderTable;

/// Contains block haders.
#[derive(Clone, Debug, Copy, Default)]
pub struct HeaderTable;

/// Current fork choice.
#[derive(Clone, Debug, Copy, Default)]
pub struct LastForkchoiceTable;

/// Map header hash to number.
#[derive(Clone, Debug, Copy, Default)]
pub struct HeaderNumberTable;

pub trait TableValue: Sized {
    fn decode(b: &[u8]) -> Result<Self, KeyDecodeError>;
}

impl TableKey for GlobalBlockId {
    type Encoded = [u8; 40];

    fn encode(&self) -> Self::Encoded {
        // encode block_num_u64 + hash
        let mut out = [0; 40];

        let block_number = self.0.to_be_bytes();
        out[..8].copy_from_slice(&block_number);

        let block_hash = self.1.to_fixed_bytes();
        out[8..].copy_from_slice(&block_hash);

        out
    }

    fn decode(b: &[u8]) -> Result<Self, KeyDecodeError> {
        if b.len() != 40 {
            return Err(KeyDecodeError::InvalidByteSize {
                expected: 40,
                actual: b.len(),
            });
        }
        let mut cursor = Cursor::new(b);
        let block_number = cursor
            .read_u64::<BigEndian>()
            .map_err(KeyDecodeError::ReadError)?;
        let block_hash = H256::from_slice(&b[8..]);
        Ok(GlobalBlockId(block_number, block_hash))
    }
}

impl Table for HeaderTable {
    type Key = GlobalBlockId;
    type Value = Header;

    fn db_name() -> &'static str {
        "Header"
    }
}

impl Table for CanonicalHeaderTable {
    type Key = u64;
    type Value = H256;

    fn db_name() -> &'static str {
        "CanonicalHeader"
    }
}

impl TableValue for H256 {
    fn decode(b: &[u8]) -> Result<Self, KeyDecodeError> {
        Ok(H256::from_slice(b))
    }
}

impl TableValue for Header {
    fn decode(b: &[u8]) -> Result<Self, KeyDecodeError> {
        let mut t = b;
        let decoded = <Header as Decodable>::decode(&mut t).expect("failed to decode rlp header");
        Ok(decoded)
    }
}

impl Table for LastForkchoiceTable {
    type Key = Forkchoice;
    type Value = H256;

    fn db_name() -> &'static str {
        "LastForkchoice"
    }
}

impl TableKey for Forkchoice {
    type Encoded = Vec<u8>;
    fn decode(b: &[u8]) -> Result<Self, KeyDecodeError> {
        let str_value = String::from_utf8(b.to_vec()).expect("key decode error");
        match str_value.as_str() {
            "headBlockHash" => Ok(Forkchoice::HeadBlockHash),
            "safeBlockHash" => Ok(Forkchoice::SafeBlockHash),
            "finalizedBlockHash" => Ok(Forkchoice::FinalizedBlockHash),
            _ => panic!("invalid forkchoice"),
        }
    }

    fn encode(&self) -> Self::Encoded {
        let str_value = match self {
            Forkchoice::HeadBlockHash => "headBlockHash",
            Forkchoice::SafeBlockHash => "safeBlockHash",
            Forkchoice::FinalizedBlockHash => "finalizedBlockHash",
        };
        str_value.as_bytes().to_vec()
    }
}

impl Table for HeaderNumberTable {
    type Key = BlockHash;
    type Value = u64;

    fn db_name() -> &'static str {
        "HeaderNumber"
    }
}

impl TableKey for BlockHash {
    type Encoded = [u8; 32];

    fn encode(&self) -> Self::Encoded {
        self.0.to_fixed_bytes()
    }

    fn decode(b: &[u8]) -> Result<Self, KeyDecodeError> {
        if b.len() != 32 {
            return Err(KeyDecodeError::InvalidByteSize {
                expected: 32,
                actual: b.len(),
            });
        }
        let inner = H256::from_slice(b);
        Ok(BlockHash(inner))
    }
}

impl From<BlockHash> for H256 {
    fn from(b: BlockHash) -> Self {
        b.0
    }
}

impl From<H256> for BlockHash {
    fn from(b: H256) -> Self {
        BlockHash(b)
    }
}

impl TableValue for u64 {
    fn decode(b: &[u8]) -> Result<Self, KeyDecodeError> {
        let mut cursor = Cursor::new(b);
        let block_number = cursor
            .read_u64::<BigEndian>()
            .map_err(KeyDecodeError::ReadError)?;
        Ok(block_number)
    }
}
