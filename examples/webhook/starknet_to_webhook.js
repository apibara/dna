import { decodeTransfersInBlock, filter } from "../common/starknet.js";

// Configure indexer for streaming Starknet Goerli data starting at the specified block.
export const config = {
  streamUrl: "https://goerli.starknet.a5a.ch",
  startingBlock: 800_000,
  network: "starknet",
  filter,
  sinkType: "webhook",
  sinkOptions: {
    // Send data as returned by `transform`.
    // When `raw = false`, the data is sent together with the starting and end cursor.
    raw: true,
  },
};

// Transform each block using the function defined in starknet.js.
export default decodeTransfersInBlock;
