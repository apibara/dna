import { filter, transform } from "../common/jediswap.js";

export { factory } from "../common/jediswap.js";

export const config = {
  // streamUrl: "https://mainnet.starknet.a5a.ch",
  streamUrl: "http://localhost:7171",
  startingBlock: 486_000,
  network: "starknet",
  filter,
  sinkType: "console",
  sinkOptions: {},
};

export default transform;
