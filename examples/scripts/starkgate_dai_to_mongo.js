/* Starkgate DAI Transfers to MongoDB
 *
 * This is an example showing how to stream Starkgate DAI Transfer eents and store them in MongoDB.
 *
 * To run this example:
 *
 * 1. Run a local MongoDB instance
 *
 * ```
 * docker run --name mongodb \
 * -p 27017:27017 \
 *  -e MONGODB_INITDB_ROOT_USERNAME=mongo \
 *  -e MONGODB_INITDB_ROOT_PASSWORD=mongo \
 *  mongo
 * ```
 *
 * 2. Get a free API key from https://apibara.com
 *
 * 3. Run the example `apibara run examples/scripts/starkgate_dai_to_mongo.js -A <API_KEY>`
 */

export const config = {
  streamUrl: 'https://mainnet.starknet.a5a.ch',
  startingBlock: 2045,
  filter: {
    header: {
      weak: true,
    },
  },
  dataFinality: 'DATA_STATUS_PENDING',
  batchSize: 1,
  network: 'starknet',
  sinkType: 'mongo',
  sinkOptions: {
    // You should not store the connection string directly in code.
    // Set the environment variable `POSTGRES_CONNECTION_STRING` instead.
    // connectionString: 'postgres://postgres:postgres@localhost:5432/postgres',
    // Table name is safe to set in code.
    tableName: 'starknet_headers',
  }
}
