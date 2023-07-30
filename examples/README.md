# Apibara Example Scripts

This folder contains example scripts used to index onchain data with Apibara.

Notice that the scripts use well-known libraries such as viem and starknet.js,
this means it's possible to share code between frontend and indexer.

**Running**

Running the examples requires you to install the Apibara CLI tools (TODO: link
to instructions) and to create a free Apibara API key.

```bash
apibara run /path/to/script.js
```

## Folder Structure

- `common/`: this folder contains the scripts to transform data, shared between
  all examples.
- `webhook/`: show how to use the webhook integration.
- `postgres/`: show how to use the PostgreSQL integration.
- `mongo/`: show how to use the MongoDB integration.
- `parquet/`: show how to use the Parquet integration.

## Networks

### Starknet

These examples stream and decode ETH `Transfer` events. For each transfer, we
extract the following data:

- block number and hash
- timestamp
- transaction hash
- transfer id: the transaction hash + event index
- the address sending ETH
- the address receiving ETH
- the amount, both raw and formatted
