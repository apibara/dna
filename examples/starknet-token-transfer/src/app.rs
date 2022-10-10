use apibara_core::stream::{Sequence, StreamId};
use apibara_node::application::{pb, Application};
use apibara_starknet::pb as starknet_pb;
use prost::{DecodeError, Message};

lazy_static::lazy_static! {
    // pedersen hash of `Transfer`.
    static ref TRANSFER_KEY: Vec<u8> = hex::decode("0099cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9").unwrap();
}

pub struct SimpleApplication {}

#[derive(Debug, thiserror::Error)]
pub enum SimpleApplicationError {
    #[error("decode error")]
    DecodeError(#[from] DecodeError),
}

#[apibara_node::async_trait]
impl Application for SimpleApplication {
    type Message = starknet_pb::Event;
    type Error = SimpleApplicationError;

    async fn init(&mut self) -> Result<pb::InitResponse, Self::Error> {
        let input = pb::InputStream {
            id: 0,
            url: "goerli.starknet.stream.apibara.com:443".to_string(),
            starting_sequence: 100_000,
        };
        let output = pb::OutputStream {
            message_type: "Event".to_string(),
            file_descriptor_proto: starknet_pb::starknet_file_descriptor_set().to_vec(),
        };
        Ok(pb::InitResponse {
            inputs: vec![input],
            output: Some(output),
        })
    }

    async fn receive_data(
        &mut self,
        _input_id: &StreamId,
        _sequence: &Sequence,
        data: &[u8],
    ) -> Result<Vec<Self::Message>, Self::Error> {
        let block = starknet_pb::Block::decode(data)?;

        let mut transfers = Vec::default();

        for receipt in block.transaction_receipts {
            for event in receipt.events {
                // Transfer(felt from, felt to, Uint256 amount or tokenId)
                if event.keys.len() != 1 {
                    continue;
                }
                if event.keys[0] != *TRANSFER_KEY {
                    continue;
                }
                if event.data.len() != 4 {
                    continue;
                }

                transfers.push(event);
            }
        }

        Ok(transfers)
    }
}
