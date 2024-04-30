import {
  decodeEventLog,
  encodeEventTopics,
  parseAbi,
} from "https://esm.sh/viem";

const abi = parseAbi([
  "event PairCreated(address indexed token0, address indexed token1, address pair, uint)",
  "event Transfer(address indexed from, address indexed to, uint256 value)",
  "event Swap(address indexed sender, uint amount0In, uint amount1In, uint amount0Out, uint amount1Out, address indexed to)",
]);

const filter = {
  header: {},
  logs: [
    {
      topics: encodeEventTopics({ abi, eventName: "PairCreated" }),
    },
  ],
};

console.log(filter);

export const config = {
  streamUrl: "http://localhost:7007",
  startingBlock: 5_000_000,
  network: "evm",
  filter,
  sinkType: "console",
  sinkOptions: {},
};

export function factory({ header, logs }) {
  const pairs = (logs ?? []).flatMap(({ topics, data }) => {
    const decoded = decodeEventLog({ abi, topics, data });
    return {
      token0: decoded.args[0],
      token1: decoded.args[1],
      pair: decoded.args[2],
    };
  });

  const pairsFilter = pairs.map(({ pair }) => ({
    address: pair,
    topics: encodeEventTopics({ abi, eventName: "Transfer" }),
  }));

  return {
    data: pairs,
    filter: {
      header: {},
      logs: pairsFilter,
    },
  };
}

export default function transform({ header, logs }) {
  return {
    header,
    logs,
  };
}
