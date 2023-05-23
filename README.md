<p align="center">
    <img width="400" src="https://user-images.githubusercontent.com/282580/176315678-e7ab5a9b-5561-41e4-b314-62f99fd90d2f.png" />
</p>

---

# Apibara Direct Node Access and SDK

This repository contains the canonical implementation of the Apibara Direct
Node Access (DNA) protocol.  
The protocol enables developers to easily and efficiently stream any on-chain
data, directly into their application. The protocol works by giving developers
a way to access and filter a node's tables.

The protocol reduces development time by taking care of the following:

 - **data decoding**: data is deserialized from the binary format used by nodes,
   into higher-level protobuf messages.
 - **chain reorganizations**: the protocol detects chain reorganizations and
   informs clients about them.


### Stream

Stream data directly from the node into your application. DNA enables you to
get exactly the data you need using _filters_. Filters are a collection of
rules that are applied to each block to select what data to send in the stream.

Filters are network-specific, you can find them in [core/proto](https://github.com/apibara/dna/tree/main/core/proto)
folder:

 - [Starknet Filter](https://github.com/apibara/dna/blob/main/core/proto/starknet/v1alpha2/filter.proto)

For example, the following Starknet filter matches all `Transfer` events (key =
`0x99cd..6e9`) emitted by the `0x053c...8a8` contract.

```json
{
  "header": { "weak": true },
  "events": [
    {
      "from_address": "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
      "keys": [
        "0x99cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9"
      ]
    }
  ]
}
```


### Transform

Data is transformed evaluating a Javascript or Typescrip function on
each batch of data. The input of the program is a list of network-specific data
(usually, blocks) and the output can be anything. The resulting output is sent
to the integration.

This example flattens blocks into a list of all events and their data:

```js
export default function flatten(batch) {
  return batch.flatMap(flattenBlock);
}

function flattenBlock(block) {
  const { header, events } = block;
  return (events ?? []).map(event => ({ ...event, ...header }));
}
```


### Integrate

We are developing a collection of integrations that send data directly where
it's needed. Our integrations support all data from genesis block to the
current pending block and they ensure data is kept up-to-date.

 - [x] Webhook: call an HTTP endpoint for each batch of data.
 - [ ] PostgreSQL: stream data into a specific table, keeping it up-to-date on
   new blocks and chain reorganizations.
 - [ ] MongoDB: store data into a specific collection, keeping it up-to-date on
   new blocks and chain reorganizations.
 - [ ] Parquet: generate Parquet files to be used for data analysis.


## Getting started

You can get started using Apibara using one of the official SDKs:

 - [Typescript](https://www.apibara.com/docs/typescript-sdk)
 - [Python](https://www.apibara.com/docs/python-sdk)


## Docker images

We publish docker images on quay.io:

 - [Starknet DNA](https://quay.io/repository/apibara/starknet?tab=tags)


## Running locally

You can run the DNA servers locally using one of the provided docker images.

### StarkNet

**Requirements**

 - Pathfinder node, fully synced.
 - Alternatively, a starknet-devnet image with the RPC port exposed.

Run the docker image, pointing it to the node RPC address:

```
docker run quay.io/apibara/starknet:<commit-sha> start --rpc http://my.node.addr:port
```


## Project Structure

The project is comprised of several crates, refer to their READMEs to learn
more about each one of them.

 - `core`: types shared by all other crates.
 - `node`: used to build and run Apibara nodes.
 - `starknet`: StarkNet source node.
 - `sdk`: connect to streams using Rust.


## License

   Copyright 2023 GNC Labs Limited

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
