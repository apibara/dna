import {
  filter,
  transform as jediswapTransform,
  factory as jediswapFactory,
} from "../common/jediswap.js";

export const config = {
  // streamUrl: "https://mainnet.starknet.a5a.ch",
  streamUrl: "http://localhost:7171",
  startingBlock: 486_000,
  network: "starknet",
  filter,
  sinkType: "mongo",
  sinkOptions: {
    database: "example",
    collectionNames: ["pairs", "events"],
  },
};

export function factory({ header, events }) {
  const { filter, data } = jediswapFactory({ header, events });

  const dataWithCollection = data.map((data) => ({
    collection: "pairs",
    data,
  }));

  return {
    filter,
    data: dataWithCollection,
  };
}

export default function transform({ header, events }) {
  return jediswapTransform({ header, events }).map((data) => ({
    collection: "events",
    data,
  }));
}
