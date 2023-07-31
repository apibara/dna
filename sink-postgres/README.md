# Apibara 🤝 Postgres

`apibara-sink-postgres` is an utility to write on-chain data from Apibara DNA to
postgres.

## Example usage

- (Optional) Bring up a local postgres and adminer (Datbase UI) instance by
  writing the following yaml in a `docker-compose.yml` file and then run
  `docker-compose up`.

```yml
version: "3.1"
services:
    db:
        image: postgres
        restart: always
        environment:
            POSTGRES_PASSWORD: postgres
        ports:
            - 5432:5432

    adminer:
        image: adminer
        restart: always
        ports:
            - 8080:8080
```

- Create the database table (you can use the adminer UI at
  http://localhost:8080)

```sql
CREATE TABLE "public"."apibara" (
    "blockNumber" integer NOT NULL,
    "blockHash" text NOT NULL,
    "txHash" text NOT NULL,
    "eventKey" text NOT NULL,
    "data" text[] NOT NULL,
    "contract" text NOT NULL,
    "_cursor" bigint NOT NULL
)
```

> This table is meant to store the data transformed by the example in the
> [sink-common's README](../sink-common/README.md#transform)

- Create `filter.json` and `transform.json` files, you can use the examples from
  the [sink-common's README](../sink-common/README.md#filter)

- Run the binary:

```bash
cargo run --bin apibara-sink-postgres -- \
    --auth-token '<YOUR API KEY HERE>' \
    --stream-url https://mainnet.starknet.a5a.ch \
    --connection-string postgresql://postgres:postgres@localhost:5432/postgres \
    --table-name apibara \
    --filter /path/to/filter.json \
    --transform /path/to/transform.js \
    --starting-block 30000 \
    --starknet
```

For a complete description of the CLI arguments, run
`apibara-sink-postgres --help`. To read more about the `--filter` and
`--transform` flags, look at the `sink-common` README.
