# Apibara 🤝 Mongo

`apibara-sink-mongo` is a utility to automatically write on-chain data from Apibara DNA to MongoDB.

- The "transform" step should return an array of records, otherwise a warning will be emitted to the user but the data is still written to the database as a single document
- Each element from the batch is written as a separate document
- A `_cursor` field is added to each document containing the value of `end_cursor.order_key` of the batch.
- On `Invalidate {cursor}`, all documents where `_cursor > cursor.order_key` are deleted

## Example usage

(Optional) Bring up a local mongodb and mongo-express (MongoDB UI) instance by writing the following yaml in a `docker-compose.yml` file and then run `docker-compose up`.

```yml
# Use root/example as user/password credentials
version: '3.1'

services:

  mongo:
    image: mongo
    restart: always
    ports:
      - 27017:27017
    environment:
      MONGO_INITDB_ROOT_USERNAME: root
      MONGO_INITDB_ROOT_PASSWORD: example

  mongo-express:
    image: mongo-express
    restart: always
    ports:
      - 8081:8081
    environment:
      ME_CONFIG_MONGODB_ADMINUSERNAME: root
      ME_CONFIG_MONGODB_ADMINPASSWORD: example
      ME_CONFIG_MONGODB_URL: mongodb://root:example@mongo:27017/
```

Run the binary with `cargo`

```
cargo run --bin apibara-sink-mongo \
    --stream-url https://mainnet.starknet.a5a.ch \
    --mongo-url "mongodb://root:example@localhost:27017/" \
    --filter @/path/to/filter.json \
    --transform @/path/to/transform.json \
    --starting-block 30000 \
    --db-name apibara \
    --collection-name apibara \
    --starknet
```
You can get a working `filter.json` and `transform.json` from the `sink-common` README

For a complete description of the CLI arguments, run `apibara-sink-mongo --help`.
To read more about the `--filter` and `--transform` flags, look at the
`sink-common` README.
