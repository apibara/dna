# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Common Changelog](https://common-changelog.org/), and
this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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


[0.2.0]: https://github.com/apibara/dna/releases/tag/cli/v0.2.0
[0.1.0]: https://github.com/apibara/dna/releases/tag/cli/v0.1.0

