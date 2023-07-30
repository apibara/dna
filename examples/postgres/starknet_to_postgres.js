// Before running this script, you must setup your database to include a `transfers` table.
// See README.md for instructions.
import { decodeTransfersInBlock, filter } from "../common/starknet.js";

// Configure indexer for streaming Starknet Goerli data starting at the specified block.
export const config = {
  streamUrl: "https://goerli.starknet.a5a.ch",
  startingBlock: 800_000,
  network: "starknet",
  filter,
  sinkType: "postgres",
  sinkOptions: {
    tableName: "transfers",
  },
};

// Transform each batch of data using the function defined in starknet.js.
export default function transform(batch) {
  return batch.flatMap(decodeTransfersInBlock);
}
