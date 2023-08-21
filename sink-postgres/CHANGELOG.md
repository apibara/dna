# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Common Changelog](https://common-changelog.org/), and
this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
 Batching is a low-level mechanism used to control how often data is written to Postgres,
 but it shouldn't affect the transform step.

## [0.1.0] - 2023-08-08

_First tagged release 🎉_


[0.2.0]: https://github.com/apibara/dna/releases/tag/sink-postgres/v0.2.0
[0.1.0]: https://github.com/apibara/dna/releases/tag/sink-postgres/v0.1.0
