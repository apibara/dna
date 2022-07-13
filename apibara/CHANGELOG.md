# Changelog

## Apibara 0.2.0

### Changed

 - Send `BlockHeader` with `NewEvents`.
 - Send crate version to clients on connect.
 - Use `starknet-rs` to connect to StarkNet.
 - Indexer state is updated when a block range has no events in it.

### Fixed

 - Detect chain reorganizations and send reorganization message to clients.


## Apibara 0.1.2 (2022-06-30)

### Added

 - Support multiple filters per indexer


## Apibara 0.1.1 (2022-06-28)

### Added

 - Manage indexers
 - Stream block events to indexers
 - Index data from StarkNet