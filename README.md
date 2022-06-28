<p align="center">
    <img width="400" src="https://user-images.githubusercontent.com/282580/176315678-e7ab5a9b-5561-41e4-b314-62f99fd90d2f.png" />
</p>

---

# Apibara Server

Apibara is a tool to build serverless APIs that integrate web3 data and web2 services.


## Architecture

Apibara applications are made of two components: the Apibara Server and the
client application. The server is responsible for all common tasks such as
indexing on-chain events, exposing a GraphQL API, and providing monitoring
metrics. The client application implements the application-specific business
logic and communicates with the server through a gRPC stream.

Developers implement applications using the Apibara SDK, available in the following
languages:

 - [Python](https://github.com/apibara/python-sdk)
 - Typescript: _coming soon_


## Roadmap

### Blockchain indexing

Index events from Ethereum-like chains and StarkNet. The Apibara Server listens
for events on-chain and sends them to the registered indexers. The server
handles all logic to fetch events and handle chain reorganizations, while the
clients focus on the business logic.


### Chain-aware storage

Provide a document storage interface that indexers use to store their data. Data
is automatically deleted in response to a chain reorganization.


### Automatic API generation

Applications can declaratively define the resources they wish to expose.
Apibara Server automatically creates the GraphQL API to serve them.

#### Streaming Support

Support GraphQL subscriptions so that clients can stream resources as they are
stored in the database.


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
