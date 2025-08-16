use std::sync::Arc;

use apibara_dna_sdk::{
    proto::{Cursor, DnaMessage},
    starknet, BearerTokenFromEnv, StreamClient, StreamDataRequestBuilder,
};
use prost::Message;
use snafu::{ResultExt, Whatever};
use tokio_stream::StreamExt;
use tonic::transport::Uri;

const STARKNET_MAINNET_URL: &str = "https://mainnet.starknet.a5a.ch";

#[tokio::main]
#[snafu::report]
pub async fn main() -> Result<(), Whatever> {
    let url: Uri = STARKNET_MAINNET_URL
        .parse()
        .with_whatever_context(|_| format!("could not parse stream url"))?;
    let mut client = StreamClient::builder()
        .with_bearer_token_provider(Arc::new(BearerTokenFromEnv::default()))
        .connect(url)
        .await
        .with_whatever_context(|_| format!("could not connect to stream"))?;

    let status = client
        .status()
        .await
        .with_whatever_context(|_| format!("could not get status"))?;

    let last_ingested = status.last_ingested.unwrap_or_default();
    let start_block = Cursor::new_with_block_number(last_ingested.order_key.saturating_sub(100));
    println!("Last ingested block: {}", last_ingested);

    let usdc_address = starknet::FieldElement::from_hex(
        "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
    )
    .with_whatever_context(|_| format!("could not parse usdc address"))?;
    let transfer_hash = starknet::FieldElement::from_hex(
        "0x99cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9",
    )
    .with_whatever_context(|_| format!("could not parse transfer signature hash"))?;

    // Create a request to get all USDC events (together with the transaction).
    //
    // Here we use `strict` to ensure that only events that have the specified
    // number of keys (event signature + indexed fields) are returned.
    let request = StreamDataRequestBuilder::new()
        .with_starting_cursor(start_block)
        .add_filter(
            starknet::FilterBuilder::new()
                .add_event(
                    starknet::EventFilterBuilder::single_contract(usdc_address)
                        .with_keys(true, vec![Some(transfer_hash)])
                        .with_transaction()
                        .build(),
                )
                .build(),
        )
        .build();

    let mut stream = client
        .stream_data(request)
        .await
        .with_whatever_context(|_| format!("failed to start stream"))?;

    while let Some(message) = stream
        .try_next()
        .await
        .with_whatever_context(|_| format!("failed to get next message in stream"))?
    {
        // Ignore non-data messages
        let DnaMessage::Data(data) = message else {
            continue;
        };

        // This is the end cursor. Use this to resume the stream from the last ingested block.
        //
        // Store both the order_key (block number) and unique_key (block hash) to detect offline reorgs.
        let _end_cursor = &data.end_cursor;
        // This is used to know if the message is backfilled or live.
        let _data_production = data.production();

        for block_bytes in data.data {
            // Decode the raw bytes into a Starknet block.
            let block = starknet::Block::decode(block_bytes)
                .with_whatever_context(|_| format!("failed to parse starknet block"))?;
            // Notice that gRPC does not send values over if they have their default values.
            // So if something is `Option<_>`, it's safe to call `unwrap_or_default()` on it.
            let header = block.header.unwrap_or_default();
            println!(
                "Block {} produced at {} has {} USDC transfers in {} transactions",
                header.block_number,
                header.timestamp.unwrap_or_default(),
                block.events.len(),
                block.transactions.len(),
            );
        }
    }

    Ok(())
}
