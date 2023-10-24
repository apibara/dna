# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Common Changelog](https://common-changelog.org/), and
this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

## [0.3.0] - 2023-09-11

_Bring support for TLS connections._

### Changed

 - **Breaking**: use TLS by default. You can revert to the old insecure
   connection by using the `--no-tls=true` CLI flag, or setting
   `sinkOptions.noTls: true` in your script.

### Added

 - You can now connect to PostgreSQL securely using TLS connections. The TLS
   connection can be customized by providing a self-signed certificate, or by
   enabling/disabling certificate and hostname validation.


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


[0.4.1]: https://github.com/apibara/dna/releases/tag/sink-postgres/v0.4.1
[0.4.0]: https://github.com/apibara/dna/releases/tag/sink-postgres/v0.4.0
[0.3.0]: https://github.com/apibara/dna/releases/tag/sink-postgres/v0.3.0
[0.2.0]: https://github.com/apibara/dna/releases/tag/sink-postgres/v0.2.0
[0.1.0]: https://github.com/apibara/dna/releases/tag/sink-postgres/v0.1.0
