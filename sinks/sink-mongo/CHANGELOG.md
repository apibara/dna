# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Common Changelog](https://common-changelog.org/), and
this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.9.3] - 2024-10-13

_Fix an issue caused by empty update operations._

### Fixed

-   Fix crashes when the transform function returns an empty list of updates.


## [0.9.2] - 2024-08-30

_Improve observability on MongoDB operations._

### Added

-   Emit OpenTelemetry traces for MongoDB operations. You can enable tracing
    by setting the `OTEL_SDK_DISABLED` env variable to `false` and configuring
    the SDK with the [standard opentelemetry environment variables](https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/).

## [0.9.1] - 2024-07-31

_Add support for filtering invoke transactions v3._

### Added

-   Add support for filtering invoke transactions v3. For example:

```ts
export const config = {
    filter: {
        transactions: [{ invokeV3: { senderAddress: "0x123...F" } }],
    }
}
```

## [0.9.0] - 2024-06-10

_Simplify access to all environment variables._

### Changed

-   If the argument to `--allow-env-from-env` is an empty string, grant access to
    all environment variables.

## [0.8.0] - 2024-04-09

_Support Starknet 0.13.1 and the new RPC 0.7.1 data._

### Added

-   Add the new blob-related fields in the Starknet RPC 0.7.1 spec.
-   Add fields related to execution resources in `TransactionReceipt`.

## [0.7.0] - 2024-03-28

_Add flag to replace pending data inside transaction._

### Added

-   Add `--replace-data-inside-transaction` (env:
    `MONGO_REPLACE_DATA_INSIDE_TRANSACTION`) flag to replace pending data in one
    transaction. Notice that MongoDB transactions require a MongoDB deployment with
    replication turned on.

## [0.6.2] - 2024-03-21

_Fix issue when transform does not return any data._

### Fixed

-   Fix a MongoDB error if the sink received no data from the transform
    function. This usually happens once the indexer reaches the tip of the chain.

### Added

-   Support secure connections when connecting to Redis for persistence.
    You can now use Redis connection strings that start with `rediss://`.

## [0.6.1] - 2024-03-17

_Fix compatibility with GLIBC < 2.38 (Ubuntu 22.04)._

### Fixed

-   Compile against GLIBC 2.34 to fix binaries not working on Linuxes
    that use GLIBC 2.38 (e.g. Ubuntu 22.04).

## [0.6.0] - 2024-03-15

_Batch insert documents._

### Added

-   Introduce the new `--batch-seconds` option to insert documents into the
    collection at the specified interval. You should use this option if you find
    the indexer is making too many small writes and wish to group them into
    larger ones. This option only affects backfilling finalized data, recent
    onchain data is inserted as soon as it's produced.

## [0.5.3] - 2024-01-25

_Persist state to Redis._

### Added

-   You can now persist state to Redis! Use the `--persist-to-redis` flag with
    [the connection string to your Redis
    instance](https://docs.rs/redis/latest/redis/#connection-handling) and the sink
    will store its state there. Data is persisted under the `apibara:sink:{sink_id}`
    key and you can easily manage it with `redis-cli`.

## [0.5.2] - 2024-01-19

_Improve `--allow-net` flag usage._

### Changed

-   When the `--allow-net` flag is used and the value passed to it is an empty
    string, treat it as equivalent to allowing any host. This is especially useful
    if you're setting the flag with the `ALLOW_NET` environment variable.

## [0.5.1] - 2024-01-16

_Enable network access._

### Added

-   Indexers can now access the network to make HTTP/TCP calls. Use the `--allow-net` flag without arguments to allow connecting to any address, or restrict access to selected domains by specifying the domains as comma-separated values.

## [0.5.0] - 2024-01-13

_Introduce factory mode to dynamically update the stream filter._

### Added

-   Introduce factory mode. Use this to dynamically update the stream filter, for
    example to start receiving data from a smart contract deployed by another smart
    contract. Enable factory mode by exporting a `factory` function from your script.
-   The sink now emits OpenTelemetry metrics to track the sync status.

### Changed

-   The status response from the status server now includes the chain's head.

### Fixed

-   Fixed an issue with the status server response timing out during the initial sync.

## [0.4.10] - 2023-12-02

_Index data in a specific block range._

### Added

-   Add a new `--ending-block` (`endingBlock` if configured from the script)
    option to stop the indexer at a specific block.

## [0.4.9] - 2023-11-30

_Control indexer timeouts._

### Added

-   Add new `--script-load-timeout-seconds` and
    `--script-transform-timeout-seconds` options to control the maximum time the
    indexer script has to initialize and transform data respectively.

## [0.4.8] - 2023-11-29

_Write to multiple collections from one indexer._

### Added

-   Add a new option to write to multiple collections from a single indexer.
    Change `collectionName` to `collectionNames` and specify a list of
    collections. When multi collection mode is enabled, the transform function
    return value changes. The transform must return data with the following type
    `{ collection: string, data: T }[]`, that is data together with the
    collection where to insert it. Refer to the documentation to learn more about
    multi collection mode.

## [0.4.7] - 2023-11-16

_Add new environment-related options._

### Added

-   Add new `--allow-env-from-env` flag to allow the indexer script to access
    the parent process environment variables. Users can pass a list of
    comma-separated variables to this option.

### Changed

-   Cleanup the default logs to only show the current block number.
-   To restore the previous, more detailed logs, set the log level to debug.

## [0.4.6] - 2023-11-11

_Fix an issue on Linux._

### Fixed

-   Link against GLIBC 3.5. The most recent build was linking against GLIBC 3.8
    which caused some issues on non-rolling release distributions.

## [0.4.5] - 2023-11-09

_Write to the same collection from multiple indexers._

### Added

-   Add a new `invalidate` option used to add additional conditions to the
    invalidate query. Developers can constrain which documents are delete by an
    indexer on data invalidation, so that multiple indexers can write to the
    same collection.

## [0.4.4] - 2023-11-07

_Improve performance for data-heavy indexers._

### Added

-   Update Starknet's event filter to support the new `includeTransaction`,
    and `includeReceipt` options. These options control whether the server will
    send the transaction and/or receipt that generated an event. For indexers
    that don't need this data, toggling this option on can improve performance
    drastically.

### Changed

-   Update the Deno runtime to `deno_core v0.244` and `deno_runtime v0.130`.
-   Use the new
    [`#[op2]`](https://docs.rs/deno_core/0.224.0/deno_core/attr.op2.html) macro
    to exchange data between Deno and Rust. Data serialization and
    deserialization between the sink and the script is now faster.

## [0.4.3] - 2023-10-27

_Fix exit code on disconnect._

### Fixed

-   In some cases, the sink would exit with a `0` exit code on error. This
    version ensures that the sink will return a non-zero exit code on all
    errors.

## [0.4.2] - 2023-10-24

_Error message improvements._

### Changed

-   This version changes how errors are handled to improve error messages.
    Errors now show more context and additional information that will help
    developers debug their indexers.
-   The sink will return a non-zero error code on failure. We use the standard
    unix exit codes in `sysexit.h`. Developers can use exit codes to decide
    whether to restart the indexer or not.

## [0.4.1] - 2023-10-11

_Disconnect when stream hangs._

### Added

-   Add a new `--timeout-duration-seconds` flag to control the timeout between
    stream messages. If the stream doesn't receive any message in this interval,
    the sink exits. Defaults to 45 seconds.

## [0.4.0] - 2023-09-16

_Introduce sink status gRPC service._

### Changed

-   The status server is now a gRPC service. This service returns the sink
    indexing status, the starting block, and the chain's current head block
    from the upstream DNA service.
-   The status server now binds on a random port. This means it's easier to run
    multiple sinks at the same time.

## [0.3.0] - 2023-08-29

_Introduce entity mode to index stateful entities._

### Added

-   Add _entity mode_ to index stateful entities. When this mode is turned on
    (by setting the `entityMode` option to `true`), the indexer behaviour changes
    to enable updating entities while indexing.
    The return value of the transform is expected to be a list of `{ entity, update }`
    objects, where `update` is a MongoDB update document or pipeline.
    Please refer to the document to read more about entity mode.

## [0.2.0] - 2023-08-21

_This release improves the developer experience when running locally._

### Added

-   Add a `--persist-to-fs=my-dir` flag to persist the indexer's state to the
    filesystem. This option creates a new directory (specified by the user) and
    writes the indexer's current state to a file, with one file per indexer.
    Developers shouldn't use this option for production, but only for
    development since it lacks any locking mechanism to prevent multiple copies
    of the same indexer running at the same time.

### Changed

-   The transform function now is invoked with a single block of data.
    Batching is a low-level mechanism used to control how often data is written to Mongo,
    but it shouldn't affect the transform step.

## [0.1.0] - 2023-08-08

_First tagged release 🎉_

[0.5.3]: https://github.com/apibara/dna/releases/tag/sink-mongo/v0.5.3
[0.5.2]: https://github.com/apibara/dna/releases/tag/sink-mongo/v0.5.2
[0.5.1]: https://github.com/apibara/dna/releases/tag/sink-mongo/v0.5.1
[0.5.0]: https://github.com/apibara/dna/releases/tag/sink-mongo/v0.5.0
[0.4.10]: https://github.com/apibara/dna/releases/tag/sink-mongo/v0.4.10
[0.4.9]: https://github.com/apibara/dna/releases/tag/sink-mongo/v0.4.9
[0.4.8]: https://github.com/apibara/dna/releases/tag/sink-mongo/v0.4.8
[0.4.7]: https://github.com/apibara/dna/releases/tag/sink-mongo/v0.4.7
[0.4.6]: https://github.com/apibara/dna/releases/tag/sink-mongo/v0.4.6
[0.4.5]: https://github.com/apibara/dna/releases/tag/sink-mongo/v0.4.5
[0.4.4]: https://github.com/apibara/dna/releases/tag/sink-mongo/v0.4.4
[0.4.3]: https://github.com/apibara/dna/releases/tag/sink-mongo/v0.4.3
[0.4.2]: https://github.com/apibara/dna/releases/tag/sink-mongo/v0.4.2
[0.4.1]: https://github.com/apibara/dna/releases/tag/sink-mongo/v0.4.1
[0.4.0]: https://github.com/apibara/dna/releases/tag/sink-mongo/v0.4.0
[0.3.0]: https://github.com/apibara/dna/releases/tag/sink-mongo/v0.3.0
[0.2.0]: https://github.com/apibara/dna/releases/tag/sink-mongo/v0.2.0
[0.1.0]: https://github.com/apibara/dna/releases/tag/sink-mongo/v0.1.0
