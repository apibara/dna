import {
  filter,
  transform as jediswapTransform,
  factory as jediswapFactory,
} from "../common/jediswap.js";

export const config = {
  streamUrl: "https://mainnet.starknet.a5a.ch",
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
    // Create multiple datasets with one indexer.
    datasets: [
      "PairCreated",
      "Upgraded",
      "AdminChanged",
      "Burn",
      "Mint",
      "Swap",
      "Sync",
      "Approval",
      "Transfer",
    ],
  },
};

export function factory({ header, events }) {
  const { filter, data } = jediswapFactory({ header, events });

  const datasets = data.map((data) => ({
    dataset: "PairCreated",
    data,
  }));

  return {
    filter,
    data: datasets,
  };
}

export default function transform({ header, events }) {
  // Dataset names can be dynamic.
  return jediswapTransform({ header, events }).map((data) => ({
    dataset: data.type,
    data,
  }));
}
