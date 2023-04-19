# Apibara StarkNet DNA Server

This crate provides `apibara-starknet`, used to start a source node that
streams data from StarkNet.


## Running

The Starknet DNA server works by ingesting data from a Starknet node and
storing it into a format that's optimized for streaming. Running the DNA
nodes requires a Starknet RPC server (like Pathfinder).
You can use a self-hosted Starknet node (recommended), or an hosted one.
You can also use devnet to create temporary networks used for local development.

Start the node by pointing it to the RPC server:

```
RUST_LOG=info apibara-starknet start --rpc https://path.to/rpc
```

You can view a list of all options by running `apibara-starknet --help`.

### Metrics

The node can export data to any service that can ingest OpenTelemetry data.
When developing locally, you can run a service with:

```
docker run --rm -p 4317:4317 otel/opentelemetry-collector-dev:latest
```

The docker container will periodically output the metric and trace data.

To disable collecting metrics, set the `OTEL_SDK_DISABLED` env variable to `true`.

## Testing

You can run unit tests with:

```
cargo test -p apibara-starknet --lib
```

