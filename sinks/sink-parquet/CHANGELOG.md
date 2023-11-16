# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Common Changelog](https://common-changelog.org/), and
this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.6] - 2023-11-16

_Add new environment-related options._

### Added

 - Add new `--allow-env-from-env` flag to allow the indexer script to access
 the parent process environment variables. Users can pass a list of
 comma-separated variables to this option.

### Changed

 - Cleanup the default logs to only show the current block number.
 - To restore the previous, more detailed logs, set the log level to debug.

## [0.3.5] - 2023-11-11

_Fix an issue on Linux._

### Fixed

 - Link against GLIBC 3.5. The most recent build was linking against GLIBC 3.8
   which caused some issues on non-rolling release distributions.

## [0.3.4] - 2023-11-07

_Improve performance for data-heavy indexers._ 

### Added

 - Update Starknet's event filter to support the new `includeTransaction`,
   and `includeReceipt` options. These options control whether the server will
   send the transaction and/or receipt that generated an event. For indexers
   that don't need this data, toggling this option on can improve performance
   drastically.

### Changed

 - Update the Deno runtime to `deno_core v0.244` and `deno_runtime v0.130`.
 - Use the new
   [`#[op2]`](https://docs.rs/deno_core/0.224.0/deno_core/attr.op2.html) macro
   to exchange data between Deno and Rust. Data serialization and
   deserialization between the sink and the script is now faster.

## [0.3.3] - 2023-10-27

_Fix exit code on disconnect._

### Fixed

 - In some cases, the sink would exit with a `0` exit code on error. This
   version ensures that the sink will return a non-zero exit code on all
   errors.

## [0.3.2] - 2023-10-24

_Error message improvements._

### Changed

 - This version changes how errors are handled to improve error messages.
   Errors now show more context and additional information that will help
   developers debug their indexers.
 - The sink will return a non-zero error code on failure. We use the standard
   unix exit codes in `sysexit.h`. Developers can use exit codes to decide
   whether to restart the indexer or not.

## [0.3.1] - 2023-10-11

_Disconnect when stream hangs._

### Added

 - Add a new `--timeout-duration-seconds` flag to control the timeout between
   stream messages. If the stream doesn't receive any message in this interval,
   the sink exits. Defaults to 45 seconds.


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

## [0.1.0] - 2023-08-08

_First tagged release 🎉_


[0.3.6]: https://github.com/apibara/dna/releases/tag/sink-parquet/v0.3.6
[0.3.5]: https://github.com/apibara/dna/releases/tag/sink-parquet/v0.3.5
[0.3.4]: https://github.com/apibara/dna/releases/tag/sink-parquet/v0.3.4
[0.3.3]: https://github.com/apibara/dna/releases/tag/sink-parquet/v0.3.3
[0.3.2]: https://github.com/apibara/dna/releases/tag/sink-parquet/v0.3.2
[0.3.1]: https://github.com/apibara/dna/releases/tag/sink-parquet/v0.3.1
[0.3.0]: https://github.com/apibara/dna/releases/tag/sink-parquet/v0.3.0
[0.2.0]: https://github.com/apibara/dna/releases/tag/sink-parquet/v0.2.0
[0.1.0]: https://github.com/apibara/dna/releases/tag/sink-parquet/v0.1.0
