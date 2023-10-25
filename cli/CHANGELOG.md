# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Common Changelog](https://common-changelog.org/), and
this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.3] - 2023-10-25

_Minor quality of life improvements._

### Fixed

 - Don't attempt to install plugins (like sinks) from pre-release releases.
 - Show the correct plugin installation command when trying to run an indexer
   that requires a missing sink.

## [0.3.2] - 2023-10-24

_Error message improvements._

### Changed

 - This version changes how errors are handled to improve error messages.
   Errors now show more context and additional information that will help
   developers debug their indexers.

## [0.3.1] - 2023-10-17

_Minor bug fixes in the `apibara test` command._

### Fixed

 - Avoid storing sensitive information such as authentication tokens in the
   test snapshots.

## [0.3.0] - 2023-09-26

_Add the `apibara test` command._

### Added

 - Introduce a `test` command to test indexers. This command implements
   snapshot testing for indexers. The first time you run it, it downloads data
   from a live DNA stream and records the output of the script. After the first
   run, it replays the saved stream and compares the output from the script with
   the output in the snapshot. A test is successful if the outputs match.

### Changed

 - The `plugins` command is now also available as `plugin`.

## [0.2.0] - 2023-09-16

_Introduce sink status gRPC service._

### Changed

 - The status server is now a gRPC service. This service returns the sink
   indexing status, the starting block, and the chain's current head block
   from the upstream DNA service. 
 - The status server now binds on a random port. This means it's easier to run
   multiple sinks at the same time.

## [0.1.0] - 2023-08-08

_First tagged release 🎉_


[0.3.3]: https://github.com/apibara/dna/releases/tag/cli/v0.3.3
[0.3.2]: https://github.com/apibara/dna/releases/tag/cli/v0.3.2
[0.3.1]: https://github.com/apibara/dna/releases/tag/cli/v0.3.1
[0.3.0]: https://github.com/apibara/dna/releases/tag/cli/v0.3.0
[0.2.0]: https://github.com/apibara/dna/releases/tag/cli/v0.2.0
[0.1.0]: https://github.com/apibara/dna/releases/tag/cli/v0.1.0

