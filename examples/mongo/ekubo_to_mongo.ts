// Before running this script, you must start a local MongoDB server.
// See README.md for instructions.
import { updateNftOwner, filter } from "../common/ekubo.ts";

// Configure indexer for streaming Starknet Goerli data starting at the specified block.
export const config = {
  streamUrl: "https://goerli.starknet.a5a.ch",
  startingBlock: 835_000,
  network: "starknet",
  filter,
  sinkType: "mongo",
  sinkOptions: {
    database: "example",
    collectionName: "owners",
    entityMode: true,
  },
};

// Transform each block using the function defined in ekubo.ts.
export default updateNftOwner;

