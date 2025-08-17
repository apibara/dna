# Apibara DNA SDK

This crate provides the Rust SDK for connecting and streaming data from Apibara DNA services.

## Usage

Start by creating a client and connecting to the service URL. If the stream is protected, specify a bearer token provider.
In this example, we use `BearerTokenFromEnv` to read the bearer token from the `DNA_TOKEN` environment variable.


```rust
let url: Uri = my_url
    .parse()?;
let mut client = StreamClient::builder()
    .with_bearer_token_provider(Arc::new(BearerTokenFromEnv::default()))
    .connect(url)
    .await?;
```

Once connected, you can make your first request to the service. Here, we call the `status` method to get information about the last indexed block.

```rust
let status = client
    .status()
    .await?;

let last_ingested = status.last_ingested.unwrap_or_default();
```

To stream data, you need to build a `StreamDataRequest` (using the `StreamDataRequestBuilder`) and call the `stream_data` method.
This method returns a stream of `DnaMessage`.

```rust
let usdc_address = starknet::FieldElement::from_hex(
    "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
)?;
let transfer_hash = starknet::FieldElement::from_hex(
    "0x99cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9",
)?;
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

let mut stream = client.stream_data(request).await?;

while let Some(message) = stream.try_next().await? {
    // Process the message here
}
```

## License

Copyright 2025 GNC Labs Limited

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
