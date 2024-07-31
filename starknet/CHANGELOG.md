# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Common Changelog](https://common-changelog.org/), and
this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.6.1] - 2024-07-31

_Support filtering invoke transactions v3._

### Added

-   Add support for filtering invoke transactions v3.

## [1.6.0] - 2024-04-09

_Add support for RPC v0.7.1._

### Added

-   Add fields related to the new blob DA layer.
-   Add execution resources fields to `TransactionReceipt`. You can use this fields to
    retrieve how many resources (like Cairo steps, builtin applications, or L1 gas) each
    transaction used.

### Changed

-   The ingestion service now uses Starknet RPC v0.7.1. To ensure that you're using
    the right version, append `/rpc/v0_7` to the RPC url.

## [1.5.0] - 2024-03-15

_Stream reverted transactions and events._

### Changed

-   Respect the `include_reverted` option in the filter to stream reverted
    transactions and events.

## [1.4.1] - 2024-01-27

_Bump database size._

### Changed

-   Increase the maximum database size to 512Gb to support mainnet data.

## [1.4.0] - 2024-01-11

_Support multiple filters in the same stream._

### Added

-   Add support for "multi filter" mode. Clients can optionally include multiple filters in the same stream request. In this case, the server will match each filter individually and return each filter's data separately. Notice that multi filter mode requires that the batch size is set to 1.

## [1.3.0] - 2023-12-21

_Support RPC v0.6 and Starknet 0.13._

### Added

-   Add Starknet 0.13 transaction types, including Invoke, Declare, and Deploy Account transactions v3.
-   Add Starknet 0.13 transaction metadata fields.

### Changed

-   The ingestion service now uses Starknet RPC v0.6. To ensure that you're using
    the right version, append `/rpc/v0_6` to the RPC url.

## [1.2.0] - 2023-11-29

_Use Starknet RPC v0.5._

### Changed

-   The ingestion service now uses Starknet RPC v0.5. To ensure that you're using
    the right version, append `/rpc/v0.5` to the RPC url for Pathfinder nodes and
    `/v0_5` for Juno nodes.

## [1.1.7] - 2023-11-18

_Improve memory usage._

### Changed

-   Switch the memory allocator to jemalloc. This reduces memory usage to
    between one half and a third.

### Fixed

-   Remove a memory leak in the block ingestion service. Ingestion messages were
    queued but never consumed by some receivers, resulting in the process memory
    usage growing linearly with the number of ingested blocks.

## [1.1.6] - 2023-11-06

_Control the amount of data delivered to clients._

### Changed

-   Add the `include_transaction` and `include_receipt` options to
    `EventFilter`. These flags enable clients to control how much data is
    delivered to them by not including the transaction and/or receipt that
    emitted an event. Users that don't use this data should enable this flag
    to improve their indexer's performance.

## [1.1.5] - 2023-11-01

_Improve compatibility with Juno._

### Fixed

-   Fix a bug ingesting Starknet data in the presence of reverted transactions.
    Nodes such as Juno don't always include the revert reason (the field in the
    JSON-RPC is optional), which caused the DNA node to fail to ingest the
    transaction and block. This release allows the revert reason field to be
    missing.

## [1.1.4] - 2023-09-30

_Check usage quota._

### Added

-   Check clients usage quota while streaming by querying an external Quota
    service. The quota service checks if the user has exceeded their quota
    based on the team, client, and network name. Configure the quota service
    specifying the `--quota-server-address` flag.

## [1.1.3] - 2023-09-22

_Fix an issue with Starknet Mainnet deployments._

### Fixed

-   Update the MDBX size parameters to be in line with Starknet Mainnet. This
    fixes the ingestion stopped because of an MDBX "Full Map" error.

## [1.1.2] - 2023-09-04

_Add a new Status method to the gRPC service._

### Added

-   Add a `Status` method to the `Stream` gRPC service. This method is used to
    query the current service ingestion state.
-   Add `--address` CLI flag to change on which address the DNA service listens
    for connections. Defaults to `0.0.0.0:7171` for backward compatibility.

## [1.1.1] - 2023-08-18

_Improve compatibility with Starknet 0.12.1 and RPC 0.4.0._

### Changed

-   Update the Starknet client used. This ensures we have a better compatibility
    with RPC 0.4.

## [1.1.0] - 2023-08-08

_Add support for Starknet 0.12.1 and RPC 0.4.0._

### Changed

-   Connect to the Starknet node using RPC v0.4.0. You need to ensure your node
    supports this RPC version before deploying.
-   Add `execution_status` and `revert_reason` fields to the
    `TransactionReceipt` message. Notice that you need to resync the DNA service
    to have these fields populated.
-   Add the `include_reverted` field to `EventFilter`, `L2ToL1MessageFilter`,
    and `TransactionFilter` to request transactions that have been reverted.
    At the moment, these flags are ignored and the stream never includes data
    from reverted transactions.

## [1.0.4] - 2023-08-03

_Control maximum stream speed._

### Added

-   Add a new `--blocks-per-second-limit` flag to control how many blocks per
    second each stream is allowed to stream.
    This is needed to avoid that a few clients use all available bandwidth and
    reduce service quality for all other clients.
-   Add the `stream_bytes_sent` metric that tracks how much data is sent to each
    client.

## [1.0.3] - 2023-08-01

_Handle Madara more frequent blocks._

### Added

-   Add a new `--head-refresh-interval-ms` flag to control how often the server
    checks for new blocks from the Starknet node. Defaults to 3 seconds.

## [1.0.2] - 2023-07-27

_Improve logging and tracing._

### Changed

-   Show a log message when starting and finishing the ingestion of a block.
-   Reduce the amount of spans sent to OpenTemeletry by default, by changing the
    span level to `DEBUG`.

## [1.0.1] - 2023-07-24

_Start publishing versioned Docker images._

## [1.0.0] - 2023-07-24

_First tagged release 🎉_

[1.4.1]: https://github.com/apibara/dna/releases/tag/starknet/v1.4.1
[1.4.0]: https://github.com/apibara/dna/releases/tag/starknet/v1.4.0
[1.3.0]: https://github.com/apibara/dna/releases/tag/starknet/v1.3.0
[1.2.0]: https://github.com/apibara/dna/releases/tag/starknet/v1.2.0
[1.1.7]: https://github.com/apibara/dna/releases/tag/starknet/v1.1.7
[1.1.6]: https://github.com/apibara/dna/releases/tag/starknet/v1.1.6
[1.1.5]: https://github.com/apibara/dna/releases/tag/starknet/v1.1.5
[1.1.4]: https://github.com/apibara/dna/releases/tag/starknet/v1.1.4
[1.1.3]: https://github.com/apibara/dna/releases/tag/starknet/v1.1.3
[1.1.2]: https://github.com/apibara/dna/releases/tag/starknet/v1.1.2
[1.1.1]: https://github.com/apibara/dna/releases/tag/starknet/v1.1.1
[1.1.0]: https://github.com/apibara/dna/releases/tag/starknet/v1.1.0
[1.0.4]: https://github.com/apibara/dna/releases/tag/starknet/v1.0.4
[1.0.3]: https://github.com/apibara/dna/releases/tag/starknet/v1.0.3
[1.0.2]: https://github.com/apibara/dna/releases/tag/starknet/v1.0.2
[1.0.1]: https://github.com/apibara/dna/releases/tag/starknet/v1.0.1
[1.0.0]: https://github.com/apibara/dna/releases/tag/starknet/v1.0.0
