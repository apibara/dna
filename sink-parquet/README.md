# Apibara 🤝 Parquet

`apibara-sink-parquet` is an utility to write on-chain data from Apibara DNA to
Parquet files.

## Example usage

Run the binary:

```
cargo run --bin apibara-sink-parquet -- \
    --stream-url https://mainnet.starknet.a5a.ch \
    --output-dir /tmp/apibara_parquet \
    --filter /path/to/filter.json \
    --transform /path/to/transform.js \
    --starting-block 30000 \
    --starknet
```

For a complete description of the CLI arguments, run
`apibara-sink-parquet --help`. To read more about the `--filter` and
`--transform` flags, look at the `sink-common` README.
