# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Common Changelog](https://common-changelog.org/), and
this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[1.0.2]: https://github.com/apibara/dna/releases/tag/starknet/v1.0.2
[1.0.1]: https://github.com/apibara/dna/releases/tag/starknet/v1.0.1
[1.0.0]: https://github.com/apibara/dna/releases/tag/starknet/v1.0.0
