import { decodeTransfersInBlock, filter } from "../common/starknet.js";

// Configure indexer for streaming Starknet Goerli data starting at the specified block.
export const config = {
  streamUrl: "https://goerli.starknet.a5a.ch",
  startingBlock: 800_000,
  network: "starknet",
  filter,
  sinkType: "console",
  sinkOptions: {
  },
};

// Transform each block using the function defined in starknet.js.
export default decodeTransfersInBlock;
