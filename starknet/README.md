# Apibara StarkNet Support

This crate provides the `apibara-starknet` plugin, used to start a source
node that streams data from StarkNet.

## Running

The node can export data to any service that can ingest OpenTelemetry data.
When developing locally, you can run a service with:

```
docker run --rm -p 4317:4317 otel/opentelemetry-collector-dev:latest
```

The docker container will periodically output the metric and trace data.

## Testing

You can run unit tests with:

```
cargo test -p apibara-starknet --lib
```

The project uses `starknet-devnet` for integration tests.
Start the devnet container with:

```
docker run --rm -p 5050:5050 shardlabs/starknet-devnet:40d5b6f75fd80e5adea989e5f45ebb76c5c25cde-seed0
```

Then you can run integration tests on it.

```
cargo test -p apibara-starknet test_starknet_integration
```