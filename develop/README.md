# Apibara development docs

This folder contains information related to the development of Apibara.


## Observability

All Apibara services (`node`, and `starknet`) can send metrics and spans to an Open Telemetry collector.

Start a pre-configured otel collector using the `docker-compose.otel.yml` file: this configuration will simply print any metric and trace to console.

Apibara implements the [OpenTelemetry Environment Variable Specification](https://opentelemetry.io/docs/reference/specification/sdk-environment-variables/) and the [OpenTelemetry Protocol Exporter Options](https://opentelemetry.io/docs/reference/specification/protocol/exporter/).

For example, start the `apibara-starknet` process and export metrics to the local collector:

```bash
RUST_LOG=info \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
OTEL_SERVICE_NAME=local-starknet \
cargo run -p apibara-starknet -- start --testnet 
```
