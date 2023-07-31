<p align="center">
    <img width="400" src="https://user-images.githubusercontent.com/282580/176315678-e7ab5a9b-5561-41e4-b314-62f99fd90d2f.png" />
</p>

---

# Apibara Direct Node Access and SDK

This repository contains the canonical implementation of the Apibara Direct Node
Access (DNA) protocol and the integrations built on top of it.\
The protocol enables developers to easily and efficiently stream any onchain
data directly into their application.

### Stream

Stream data directly from the node into your application. DNA enables you to get
exactly the data you need using _filters_. Filters are a collection of rules
that are applied to each block to select what data to send in the stream.

### Transform

Data is transformed evaluating a Javascript or Typescript function on each batch
of data. The resulting data is automatically sent and synced with the target
integration.

### Integrate

We provide a collection of integrations that send data directly where it's
needed. Our integrations support all data from genesis block to the current
pending block and they ensure data is kept up-to-date.

- **Webhook**: call an HTTP endpoint for each batch of data.
- **PostgreSQL**: stream data into a specific table, keeping it up-to-date on
  new blocks and chain reorganizations.
- **MongoDB**: store data into a specific collection, keeping it up-to-date on
  new blocks and chain reorganizations.
- **Parquet**: generate Parquet files to be used for data analysis.

## Getting started

You can get started using Apibara by installing the official CLI tool.

If you'd like to interact with the DNA protocol directly, you can use one of the
official SDKs:

- [Typescript](https://www.apibara.com/docs/typescript-sdk)
- [Python](https://www.apibara.com/docs/python-sdk)

## Docker images

We publish docker images on quay.io:

- [Starknet DNA](https://quay.io/repository/apibara/starknet?tab=tags)

## Development

Apibara DNA is developed against stable Rust. We provide a
[nix](https://nixos.org/) environment to simplify installing all dependencies
required by DNA.

- if you have nix installed, simply run `nix develop`.
- otherwise, you can launch a devcontainer which installs and configures nix for
  you.

## Project Structure

The project is comprised of several crates, refer to their READMEs to learn more
about each one of them.

- `core`: types shared by all other crates.
- `starknet`: StarkNet source node.
- `sdk`: connect to streams using Rust.
- `sink-common`: base crate to develop custom sinks.
- `sink-*`: sink implementations.

## License

Copyright 2023 GNC Labs Limited

Licensed under the Apache License, Version 2.0 (the "License"); you may not use
this file except in compliance with the License. You may obtain a copy of the
License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
