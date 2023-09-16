# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Common Changelog](https://common-changelog.org/), and
this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2023-09-16

_Introduce sink status gRPC service._

### Changed

 - The status server is now a gRPC service. This service returns the sink
   indexing status, the starting block, and the chain's current head block
   from the upstream DNA service. 
 - The status server now binds on a random port. This means it's easier to run
   multiple sinks at the same time.

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
 - In "raw mode", each value returned by the transform function is sent as an
   individual request.


## [0.1.0] - 2023-08-08

_First tagged release 🎉_


[0.3.0]: https://github.com/apibara/dna/releases/tag/sink-webhook/v0.3.0
[0.2.0]: https://github.com/apibara/dna/releases/tag/sink-webhook/v0.2.0
[0.1.0]: https://github.com/apibara/dna/releases/tag/sink-webhook/v0.1.0
