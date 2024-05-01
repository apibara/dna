// Before running this script, you must setup your database to include a `transfers` table.
// See README.md for instructions.
import { decodeTransfersInBlock, filter } from "../common/starknet.js";

// Configure indexer for streaming Starknet Sepolia data starting at the specified block.
export const config = {
  streamUrl: "https://sepolia.starknet.a5a.ch",
  startingBlock: 1_000,
  network: "starknet",
  filter,
  sinkType: "postgres",
  sinkOptions: {
    noTls: true,
    tableName: "transfers",
  },
};

// Transform each block using the function defined in starknet.js.
export default decodeTransfersInBlock;
