# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Common Changelog](https://common-changelog.org/), and
this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.3] - 2023-10-27

_Fix exit code on disconnect._

### Fixed

 - In some cases, the sink would exit with a `0` exit code on error. This
   version ensures that the sink will return a non-zero exit code on all
   errors.

## [0.4.2] - 2023-10-24

_Error message improvements._

### Changed

 - This version changes how errors are handled to improve error messages.
   Errors now show more context and additional information that will help
   developers debug their indexers.
 - The sink will return a non-zero error code on failure. We use the standard
   unix exit codes in `sysexit.h`. Developers can use exit codes to decide
   whether to restart the indexer or not.

## [0.4.1] - 2023-10-11

_Disconnect when stream hangs._

### Added

 - Add a new `--timeout-duration-seconds` flag to control the timeout between
   stream messages. If the stream doesn't receive any message in this interval,
   the sink exits. Defaults to 45 seconds.


## [0.4.0] - 2023-09-16

_Introduce sink status gRPC service._

### Changed

 - The status server is now a gRPC service. This service returns the sink
   indexing status, the starting block, and the chain's current head block
   from the upstream DNA service. 
 - The status server now binds on a random port. This means it's easier to run
   multiple sinks at the same time.


## [0.3.0] - 2023-08-29

_Introduce entity mode to index stateful entities._

### Added

 - Add _entity mode_ to index stateful entities. When this mode is turned on
   (by setting the `entityMode` option to `true`), the indexer behaviour changes
   to enable updating entities while indexing.
   The return value of the transform is expected to be a list of `{ entity, update }`
   objects, where `update` is a MongoDB update document or pipeline.
   Please refer to the document to read more about entity mode.

## [0.2.0] - 2023-08-21

_This release improves the developer experience when running locally._

### Added

 - Add a `--persist-to-fs=my-dir` flag to persist the indexer's state to the
   filesystem. This option creates a new directory (specified by the user) and
   writes the indexer's current state to a file, with one file per indexer.
   Developers shouldn't use this option for production, but only for
   development since it lacks any locking mechanism to prevent multiple copies
   of the same indexer running at the same time.

### Changed

 - The transform function now is invoked with a single block of data.
 Batching is a low-level mechanism used to control how often data is written to Mongo,
 but it shouldn't affect the transform step.

## [0.1.0] - 2023-08-08

_First tagged release 🎉_


[0.4.3]: https://github.com/apibara/dna/releases/tag/sink-mongo/v0.4.2
[0.4.2]: https://github.com/apibara/dna/releases/tag/sink-mongo/v0.4.3
[0.4.1]: https://github.com/apibara/dna/releases/tag/sink-mongo/v0.4.1
[0.4.0]: https://github.com/apibara/dna/releases/tag/sink-mongo/v0.4.0
[0.3.0]: https://github.com/apibara/dna/releases/tag/sink-mongo/v0.3.0
[0.2.0]: https://github.com/apibara/dna/releases/tag/sink-mongo/v0.2.0
[0.1.0]: https://github.com/apibara/dna/releases/tag/sink-mongo/v0.1.0
