# Apibara 🤝 PostgreSQL

_Mirror onchain data to a PostgreSQL table._

**Use cases**

- Quickly develop a backend for your dapp by sending data to Supabase or Hasura.
- Build internal dashboards with Retool or Illa.
- Join offchain and onchain data.

## Usage

You must set the `POSTGRES_CONNECTION_STRING` environment variable to the one
provided by your PostgreSQL provider.

For developing locally, we provide a `docker-compose.yml` file that
starts Hasura locally. Run it with:

```
docker-compose up
```

Then export the following environment variable:

```
export POSTGRES_CONNECTION_STRING='postgres://postgres:postgres@localhost:5432/postgres'
```

Follow
[the steps on the official Hasura
documentation](https://hasura.io/docs/latest/getting-started/docker-simple/#step-2-connect-a-database)
to connect to the database and create the following table (TL;DR: visit
http://localhost:8080 and use `PG_DATABASE_URL` to connect Hasura to
PostgreSQL).

**Notice**: the `_cursor` column is REQUIRED by Apibara to automatically
invalidate data following chain reorganizations.

### Standard mode

```sql
create table transfers(
  network text, -- network name, e.g. starknet-goerli
  symbol text, -- token symbol, e.g. ETH
  block_hash text, -- hex encoded block hash
  block_number bigint,
  block_timestamp timestamp,
  transaction_hash text, -- hex encoded transaction hash
  transfer_id text, -- unique transfer id
  from_address text, -- address sending the token
  to_address text, -- address receiving the token
  amount numeric, -- amount as float. Some precision is lost, but we can aggregate it
  amount_raw text, -- amount, as bigint
  _cursor bigint -- REQUIRED: needed for data invalidation
);
```

After creating the `transfers` table, you can run the script using
`apibara run`. Visit http://localhost:8080/console to play around with your new
GraphQL API! For example, run the following query to check the indexing
progress.

```graphql
query TransferCount {
  transfers_aggregate {
    aggregate {
      count
    }
  }
}
```

**Notice**: some queries may be slow because we haven't created any index yet.
Refer to the
[Hasura documentation to improve query
performance](https://hasura.io/docs/latest/queries/postgres/performance/).

### Entity mode

The `entity_mode.js` script shows how to use _entity mode_.
The script needs the following table. Notice that in this case the `_cursor` is
a range of blocks.

```sql
create table if not exists vrf_requests(
  request_id bigint,
  status bigint,
  created_at timestamp,
  created_at_tx text,
  updated_at timestamp,
  updated_at_tx text,
  _cursor int8range
);
```

To get the most recent state of an entity, you must add the `upper_inf(_cursor)`
condition to your query.

```sql
select * from vrf_requests where upper_inf(_cursor)
```

