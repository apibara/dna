# Apibara 🤝 Webhooks

`apibara-sink-webhook` is an utility to automatically invoke a webhook with
on-chain data from Apibara DNA.


## Example usage

```
apibara-sink-webhook \
    --stream-url https://mainnet.starknet.a5a.ch \
    --target-url https://example.org/post \
    --filter @/path/to/filter.json \
    --transform @/path/to/transform.json \
    --starting-block 30000 \
    --starknet
```

For a complete description of the CLI arguments, run `apibara-sink-webhook --help`.

