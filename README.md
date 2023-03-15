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


## Getting started

You can get started using Apibara using one of the official SDKs:

 - [Typescript](https://www.apibara.com/docs/typescript-sdk)
 - [Python](https://www.apibara.com/docs/python-sdk)


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
