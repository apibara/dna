# Sink helper traits and functions

This crate provides a `Sink` and a `SinkConnector` that are useful when
implementing integrations. It also includes `ConfigurationArgs` that
can be used to quickly add the common command line flags that are used with
integrations.

## Usage

If you're developing an integration in this repository:

 * Create a new crate for the integration with `cargo new --lib sink-my-integration`.
   The crate folder should start with `sink-`, and the crate name with `apibara-sink-`.
 * Add `apibara-core`, `apibara-observability` and `apibara-sink-common` to its dependencies.
 * Create a `bin.rs` file and update `Cargo.toml` to provide both a library and binary.
   Use `sink-webhook` as a reference.

