<p align="center">
    <img width="400" src="https://user-images.githubusercontent.com/282580/176315678-e7ab5a9b-5561-41e4-b314-62f99fd90d2f.png" />
</p>

---

# Apibara Node and SDK

The Apibara SDK provides an high-level interface to build applications that
transform streams of web3 data. Streams can be composed to form more complex
applications. Streams can be transformed using any programming language.


## Overview

The Apibara SDK enables developers to build nodes that combine and transform
multiple streams of data into a new stream. Nodes don't transform data directly,
but delegate the operation to _applications_. Applications can run externally of
the node or they can be built-in. External applications can be implemented in any
language and communicate with the node through gRPC, while built-in applications
are implemented in Rust and run directly within the node.

### The problem

As smart contract developers, we need to expose our data in a format easily accessible by frontend libraries so that our users are delighted by a responsive application that loads fast, even on slower networks.

The main challenge is that StarkNet, like other networks, doesn't provide an API to access a contract's storage like a database. Smart contracts include functions to read specific values from storage, like the owner of an NFT or the ERC-20 balance of an address, but that's not enough if we need more complex queries like a list of NFTs owned by an address or their historical ERC-20 balances. This issue becomes even more critical when we implement smart contracts for complex systems like on-chain games that need to minimize the amount of storage used to reduce transaction costs.

### Solution

Apibara can be used as a tool to index smart contract events on StarkNet and, in the future, other EVM-compatible chains like Ethereum. Apibara runs locally on your machine and its job is to:

- fetch events from the blockchain and send them to the indexer to index
- track the current chain's head and detect chain reorganizations
- track and store the indexers progress


## Project Structure

The SDK is comprised of several crates, refer to their READMEs to learn
more about each one of them.

 - `core`: types shared by all other crates.
 - `node`: used to build and run Apibara nodes.
 - `starknet`: StarkNet source node.
 - `cli`: the CLI interface to manage and start Apibara nodes.


## License

   Copyright 2022 GNC Labs Limited

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
