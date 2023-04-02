use apibara_node::db::{Decodable, DecodeError, Encodable};
use prost::Message;

use super::block::{BlockBody, BlockReceipts, BlockStatus};

/// Encoded prost message.
#[derive(Default, Clone)]
pub struct EncodedMessage<T: Message + Default>(pub T);

impl<T: Message + Default> Encodable for EncodedMessage<T> {
    type Encoded = Vec<u8>;

    fn encode(&self) -> Self::Encoded {
        self.0.encode_to_vec()
    }
}

impl<T: Message + Default> Decodable for EncodedMessage<T> {
    fn decode(b: &[u8]) -> Result<Self, DecodeError> {
        T::decode(b)
            .map(Self)
            .map_err(|err| DecodeError::Other(Box::new(err)))
    }
}

impl Encodable for BlockStatus {
    type Encoded = Vec<u8>;

    fn encode(&self) -> Self::Encoded {
        self.encode_to_vec()
    }
}

impl Decodable for BlockStatus {
    fn decode(b: &[u8]) -> Result<Self, DecodeError> {
        <Self as Message>::decode(b).map_err(|err| DecodeError::Other(Box::new(err)))
    }
}

impl Encodable for BlockBody {
    type Encoded = Vec<u8>;

    fn encode(&self) -> Self::Encoded {
        self.encode_to_vec()
    }
}

impl Decodable for BlockBody {
    fn decode(b: &[u8]) -> Result<Self, DecodeError> {
        <Self as Message>::decode(b).map_err(|err| DecodeError::Other(Box::new(err)))
    }
}

impl Encodable for BlockReceipts {
    type Encoded = Vec<u8>;

    fn encode(&self) -> Self::Encoded {
        self.encode_to_vec()
    }
}

impl Decodable for BlockReceipts {
    fn decode(b: &[u8]) -> Result<Self, DecodeError> {
        <Self as Message>::decode(b).map_err(|err| DecodeError::Other(Box::new(err)))
    }
}
