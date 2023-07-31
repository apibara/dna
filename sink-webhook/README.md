# Apibara 🤝 Webhooks

`apibara-sink-webhook` is an utility to automatically invoke a webhook with
on-chain data from Apibara DNA.

## Example usage

(Optional) Bring up a local http server with
`docker run --rm -p 8080:8080  mendhak/http-https-echo`.

Compile the project with `cargo build -p apibara-sink-webhook`.

Run the binary (usually in `target/build`):

```
apibara-sink-webhook \
    --stream-url https://mainnet.starknet.a5a.ch \
    --target-url http://localhost:8080/ \
    --filter /path/to/filter.json \
    --transform /path/to/transform.js \
    --starting-block 30000 \
    --starknet
```

For a complete description of the CLI arguments, run
`apibara-sink-webhook --help`. To read more about the `--filter` and
`--transform` flags, look at the `sink-common` README.
