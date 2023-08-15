# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Common Changelog](https://common-changelog.org/), and
this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.1] - 2023-08-18

_Improve compatibility with Starknet 0.12.1 and RPC 0.4.0._

### Change

 - Update the Starknet client used. This ensures we have a better compatibility
   with RPC 0.4.

## [1.1.0] - 2023-08-08

_Add support for Starknet 0.12.1 and RPC 0.4.0._

### Changed

 - Connect to the Starknet node using RPC v0.4.0. You need to ensure your node
   supports this RPC version before deploying.
 - Add `execution_status` and `revert_reason` fields to the
   `TransactionReceipt` message. Notice that you need to resync the DNA service
   to have these fields populated.
 - Add the `include_reverted` field to `EventFilter`, `L2ToL1MessageFilter`,
   and `TransactionFilter` to request transactions that have been reverted.
   At the moment, these flags are ignored and the stream never includes data
   from reverted transactions.

## [1.0.4] - 2023-08-03

_Control maximum stream speed._

### Added

 - Add a new `--blocks-per-second-limit` flag to control how many blocks per
   second each stream is allowed to stream.
   This is needed to avoid that a few clients use all available bandwidth and
   reduce service quality for all other clients.
 - Add the `stream_bytes_sent` metric that tracks how much data is sent to each
   client.

## [1.0.3] - 2023-08-01

_Handle Madara more frequent blocks._

### Added

 - Add a new `--head-refresh-interval-ms` flag to control how often the server
   checks for new blocks from the Starknet node. Defaults to 3 seconds.

## [1.0.2] - 2023-07-27

_Improve logging and tracing._

### Changed

- Show a log message when starting and finishing the ingestion of a block.
- Reduce the amount of spans sent to OpenTemeletry by default, by changing the
  span level to `DEBUG`.

## [1.0.1] - 2023-07-24

_Start publishing versioned Docker images._

## [1.0.0] - 2023-07-24

_First tagged release 🎉_

[1.1.1]: https://github.com/apibara/dna/releases/tag/starknet/v1.1.1
[1.1.0]: https://github.com/apibara/dna/releases/tag/starknet/v1.1.0
[1.0.4]: https://github.com/apibara/dna/releases/tag/starknet/v1.0.4
[1.0.3]: https://github.com/apibara/dna/releases/tag/starknet/v1.0.3
[1.0.2]: https://github.com/apibara/dna/releases/tag/starknet/v1.0.2
[1.0.1]: https://github.com/apibara/dna/releases/tag/starknet/v1.0.1
[1.0.0]: https://github.com/apibara/dna/releases/tag/starknet/v1.0.0
