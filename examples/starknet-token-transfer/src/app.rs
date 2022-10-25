use apibara_sdk::{
    application::Application,
    pb::application as app_pb,
    stream::{Sequence, StreamId},
};
use apibara_starknet::pb::{self as starknet_pb};
use prost::{DecodeError, Message};
use tracing::info;

lazy_static::lazy_static! {
    // pedersen hash of `Transfer`.
    static ref TRANSFER_KEY: Vec<u8> = hex::decode("0099cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9").unwrap();
}

mod pb {
    tonic::include_proto!("example.org.v1alpha1");

    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("transfer_descriptor");

    pub fn transfer_file_descriptor_set() -> &'static [u8] {
        FILE_DESCRIPTOR_SET
    }
}

pub struct SimpleApplication {}

#[derive(Debug, thiserror::Error)]
pub enum SimpleApplicationError {
    #[error("decode error")]
    DecodeError(#[from] DecodeError),
}

#[apibara_sdk::async_trait]
impl Application for SimpleApplication {
    type Message = pb::Transfer;
    type Error = SimpleApplicationError;

    async fn init(&mut self) -> Result<app_pb::InitResponse, Self::Error> {
        let input = app_pb::InputStream {
            id: 0,
            url: "goerli.starknet.stream.apibara.com:443".to_string(),
            starting_sequence: 100_000,
        };
        let output = app_pb::OutputStream {
            message_type: "example.org.v1alpha1.Transfer".to_string(),
            filename: "transfer.proto".to_string(),
            file_descriptor_proto: pb::transfer_file_descriptor_set().to_vec(),
        };
        Ok(app_pb::InitResponse {
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
        let block_hash = block.block_hash.unwrap_or_default();
        let block_hash = pb::BlockHash {
            hash: block_hash.hash,
        };

        info!(block = %block.block_number, "receive data");

        let mut transfers = Vec::default();

        for (tx_idx, receipt) in block.transaction_receipts.iter().enumerate() {
            let transaction = &block.transactions[tx_idx];
            let common = match &transaction.transaction {
                None => None,
                Some(starknet_pb::transaction::Transaction::Declare(declare)) => {
                    declare.common.as_ref()
                }
                Some(starknet_pb::transaction::Transaction::Invoke(invoke)) => {
                    invoke.common.as_ref()
                }
                Some(starknet_pb::transaction::Transaction::Deploy(deploy)) => {
                    deploy.common.as_ref()
                }
                Some(starknet_pb::transaction::Transaction::L1Handler(handler)) => {
                    handler.common.as_ref()
                }
                Some(starknet_pb::transaction::Transaction::DeployAccount(handler)) => {
                    handler.common.as_ref()
                }
            };

            let transaction_hash = common.map(|tx| tx.hash.clone()).unwrap_or_default();

            for (log_index, event) in receipt.events.iter().enumerate() {
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

                let from_address = event.data[0].clone();
                let to_address = event.data[1].clone();
                // TODO: parse to uint256 and back
                let token_id_or_amount = event.data[2].clone();

                let transfer = pb::Transfer {
                    from_address,
                    to_address,
                    token_id_or_amount,
                    block_hash: Some(block_hash.clone()),
                    block_number: block.block_number,
                    timestamp: block.timestamp.clone(),
                    transaction_hash: transaction_hash.clone(),
                    log_index: log_index as u32,
                };
                transfers.push(transfer);
            }
        }

        Ok(transfers)
    }
}
