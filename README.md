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

-   **Webhook**: call an HTTP endpoint for each batch of data.
-   **PostgreSQL**: stream data into a specific table, keeping it up-to-date on
    new blocks and chain reorganizations.
-   **MongoDB**: store data into a specific collection, keeping it up-to-date on
    new blocks and chain reorganizations.
-   **Parquet**: generate Parquet files to be used for data analysis.

## Getting started

You can get started using Apibara by installing the official CLI tool.
We provide [detailed instructions in the official documentation](https://www.apibara.com/docs).

## Docker images

We publish docker images on quay.io. Images are available for both the x86_64
and aarch64 architectures.

**Sinks**

-   [MongoDB](https://quay.io/repository/apibara/sink-mongo?tab=tags)
-   [PostgreSQL](https://quay.io/repository/apibara/sink-postgres?tab=tags)
-   [Parquet](https://quay.io/repository/apibara/sink-parquet?tab=tags)
-   [Webhook](https://quay.io/repository/apibara/sink-webhook?tab=tags)

**Server**

-   [Starknet DNA](https://quay.io/repository/apibara/starknet?tab=tags)

## Contributing

We are open to contributions.

-   Read the
    [CONTRIBUTING.md](https://github.com/apibara/dna/blob/main/CONTRIBUTING.md)
    guide to learn more about the process.
-   Some contributions are [rewarded on OnlyDust](https://app.onlydust.com/p/apibara).
    If you're interested in paid contributions, get in touch before submitting a PR.

## Development

Apibara DNA is developed against stable Rust. We provide a
[nix](https://nixos.org/) environment to simplify installing all dependencies
required by the project.

-   if you have nix installed, simply run `nix develop`.
-   if you don't have nix installed, you should install Rust using your favorite
    tool.

## Platform Support

**Tier 1**

These platforms are tested against every pull request.

-   linux-x86_64
-   macos-aarch64

**Tier 2**

These platform are tested on new releases.

-   linux-aarch64 - used for multi-arch docker images.

**Unsupported**

These platforms are not supported.

-   windows - if you're a developer using Windows, we recommend the [Windows
    Subsystem for Linux (WSL)](https://learn.microsoft.com/en-us/windows/wsl/).
-   macos-x86_64 - given the slowness of CI runners for this platform, we cannot
    provide builds for it.

## Project Structure

The project is comprised of several crates, refer to their READMEs to learn more
about each one of them.

-   `core`: types shared by all other crates.
-   `starknet`: StarkNet source node.
-   `sdk`: connect to streams using Rust.
-   `sinks`: contains the code for all sinks.

## License

Copyright 2024 GNC Labs Limited

Licensed under the Apache License, Version 2.0 (the "License"); you may not use
this file except in compliance with the License. You may obtain a copy of the
License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
