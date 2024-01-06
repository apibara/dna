import { filter, transform } from "../common/jediswap.js";

export { factory } from "../common/jediswap.js";

export const config = {
  // streamUrl: "https://mainnet.starknet.a5a.ch",
  streamUrl: "http://localhost:7171",
  startingBlock: 486_000,
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

export default transform;
