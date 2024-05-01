// See README.md for instructions.
import { decodeTransfersInBlock, filter } from "../common/starknet.js";

// Configure indexer for streaming Starknet Sepolia data starting at the specified block.
export const config = {
  streamUrl: "https://sepolia.starknet.a5a.ch",
  startingBlock: 1_000,
  network: "starknet",
  filter,
  sinkType: "parquet",
  sinkOptions: {
    // Files will have data for 100 blocks each.
    // In reality, you want this number to be higher (like 1_000),
    // but for the sake of this example, we keep it low to generate
    // files quickly.
    batchSize: 100,
  },
};

// Transform each block using the function defined in starknet.js.
export default decodeTransfersInBlock;
