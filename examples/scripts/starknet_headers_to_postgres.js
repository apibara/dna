/* Starknet Headers to Postgres
 *
 * This is an example showing how to stream Starknet block headers and store them in Postgres.
 *
 * To run this example:
 *
 * 1. Run a local postgres instance
 *
 * ```
 * docker run --name postgres -p 5432:5432 -e POSTGRES_PASSWORD=postgres -d postgres
 * ```
 *
 * 2. Create a table in the `postgres` database
 *
 * ```
 * create table if not exists starknet_headers (
 *   block_number bigint not null,
 *   block_hash text not null,
 *   parent_block_hash text not null,
 *   new_root text not null,
 *   timestamp timestamp not null,
 *   _cursor bigint not null -- <--- this is required. Used to handle chain reorgs.
 * );
 * ```
 *
 * 3. Get a free API key from https://apibara.com
 *
 * 4. Run the example `apibara run examples/scripts/starknet_headers_to_postgres.js -A <API_KEY>`
 */

export const config = {
  streamUrl: 'https://mainnet.starknet.a5a.ch',
  startingBlock: 131740,
  filter: {
    header: {
      weak: false,
    },
  },
  dataFinality: 'DATA_STATUS_PENDING',
  batchSize: 1,
  network: 'starknet',
  sinkType: 'postgres',
  sinkOptions: {
    // You should not store the connection string directly in code.
    // Set the environment variable `POSTGRES_CONNECTION_STRING` instead.
    // connectionString: 'postgres://postgres:postgres@localhost:5432/postgres',
    // Table name is safe to set in code.
    tableName: 'starknet_headers',
  }
}

// The postgres integration expects the return value to be an array of objects.
export default function transform(batch) {
  const data = batch.map(decodeBlock);
  return data;
}

function decodeBlock({ header }) {
  return {
    block_number: +(header.blockNumber ?? 0),
    block_hash: header.blockHash,
    parent_block_hash: header.parentBlockHash,
    new_root: header.newRoot,
    timestamp: header.timestamp,
  }
}
