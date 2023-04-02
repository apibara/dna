use std::io::Cursor;

use apibara_node::db::{Decodable, DecodeError, Encodable, Table};
use byteorder::{BigEndian, ReadBytesExt};
use ethers_core::types::H256;

use crate::erigon::types::{BlockHash, Forkchoice, GlobalBlockId, Header, LogId};

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

/// Canonical chain's transaction receipts.
#[derive(Clone, Debug, Copy, Default)]
pub struct ReceiptTable;

/// Transaction logs.
#[derive(Clone, Debug, Copy, Default)]
pub struct LogTable;

/// Block body.
#[derive(Clone, Debug, Copy, Default)]
pub struct BlockBodyTable;

#[derive(Default, Clone)]
pub struct Rlp<T: reth_rlp::Decodable>(pub T);

impl Encodable for GlobalBlockId {
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
}

impl Decodable for GlobalBlockId {
    fn decode(b: &[u8]) -> Result<Self, DecodeError> {
        if b.len() != 40 {
            return Err(DecodeError::InvalidByteSize {
                expected: 40,
                actual: b.len(),
            });
        }
        let mut cursor = Cursor::new(b);
        let block_number = cursor
            .read_u64::<BigEndian>()
            .map_err(DecodeError::ReadError)?;
        let block_hash = H256::from_slice(&b[8..]);
        Ok(GlobalBlockId(block_number, block_hash))
    }
}

impl Encodable for LogId {
    type Encoded = [u8; 12];

    fn encode(&self) -> Self::Encoded {
        // encode block_num_u64 + hash
        let mut out = [0; 12];

        let block_number = self.0.to_be_bytes();
        out[..8].copy_from_slice(&block_number);

        let log_index = self.1.to_be_bytes();
        out[8..].copy_from_slice(&log_index);

        out
    }
}

impl Decodable for LogId {
    fn decode(b: &[u8]) -> Result<Self, DecodeError> {
        if b.len() != 12 {
            return Err(DecodeError::InvalidByteSize {
                expected: 12,
                actual: b.len(),
            });
        }
        let mut cursor = Cursor::new(b);
        let block_number = cursor
            .read_u64::<BigEndian>()
            .map_err(DecodeError::ReadError)?;
        let log_index = cursor
            .read_u32::<BigEndian>()
            .map_err(DecodeError::ReadError)?;
        Ok(LogId(block_number, log_index))
    }
}

impl Table for HeaderTable {
    type Key = GlobalBlockId;
    type Value = Rlp<Header>;

    fn db_name() -> &'static str {
        "Header"
    }
}

impl Table for CanonicalHeaderTable {
    type Key = u64;
    type Value = Rlp<H256>;

    fn db_name() -> &'static str {
        "CanonicalHeader"
    }
}

impl<T: reth_rlp::Decodable> Decodable for Rlp<T> {
    fn decode(mut b: &[u8]) -> Result<Self, DecodeError> {
        let decoded = T::decode(&mut b).map_err(|err| DecodeError::Other(Box::new(err)))?;
        Ok(Rlp(decoded))
    }
}

impl Table for LastForkchoiceTable {
    type Key = Forkchoice;
    type Value = Rlp<H256>;

    fn db_name() -> &'static str {
        "LastForkchoice"
    }
}

impl Encodable for Forkchoice {
    type Encoded = Vec<u8>;

    fn encode(&self) -> Self::Encoded {
        let str_value = match self {
            Forkchoice::HeadBlockHash => "headBlockHash",
            Forkchoice::SafeBlockHash => "safeBlockHash",
            Forkchoice::FinalizedBlockHash => "finalizedBlockHash",
        };
        str_value.as_bytes().to_vec()
    }
}

impl Decodable for Forkchoice {
    fn decode(b: &[u8]) -> Result<Self, DecodeError> {
        let str_value = String::from_utf8(b.to_vec()).expect("key decode error");
        match str_value.as_str() {
            "headBlockHash" => Ok(Forkchoice::HeadBlockHash),
            "safeBlockHash" => Ok(Forkchoice::SafeBlockHash),
            "finalizedBlockHash" => Ok(Forkchoice::FinalizedBlockHash),
            _ => panic!("invalid forkchoice"),
        }
    }
}

impl Table for HeaderNumberTable {
    type Key = BlockHash;
    type Value = u64;

    fn db_name() -> &'static str {
        "HeaderNumber"
    }
}

impl Encodable for BlockHash {
    type Encoded = [u8; 32];

    fn encode(&self) -> Self::Encoded {
        self.0.to_fixed_bytes()
    }
}

impl Decodable for BlockHash {
    fn decode(b: &[u8]) -> Result<Self, DecodeError> {
        if b.len() != 32 {
            return Err(DecodeError::InvalidByteSize {
                expected: 32,
                actual: b.len(),
            });
        }
        let inner = H256::from_slice(b);
        Ok(BlockHash(inner))
    }
}

impl Table for ReceiptTable {
    type Key = u64;
    type Value = Vec<u8>;

    fn db_name() -> &'static str {
        "Receipt"
    }
}

impl Table for LogTable {
    type Key = LogId;
    type Value = Vec<u8>;

    fn db_name() -> &'static str {
        "TransactionLog"
    }
}

impl Table for BlockBodyTable {
    type Key = GlobalBlockId;
    type Value = Vec<u8>;

    fn db_name() -> &'static str {
        "BlockBody"
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
