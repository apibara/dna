//! Type-safe database access.

use apibara_core::stream::{Sequence, StreamId};
use prost::Message;

pub trait TableKey: Send + Sync {
    type Encoded: AsRef<[u8]> + Send + Sync;

    fn encode(&self) -> Self::Encoded;
}

pub trait Table: Send + Sync {
    type Key: TableKey;
    type Value: Message + Default;

    fn db_name() -> &'static str;
}

impl TableKey for StreamId {
    type Encoded = [u8; 8];

    fn encode(&self) -> Self::Encoded {
        self.as_u64().to_be_bytes()
    }
}

impl TableKey for Sequence {
    type Encoded = [u8; 8];

    fn encode(&self) -> Self::Encoded {
        self.as_u64().to_be_bytes()
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
}
