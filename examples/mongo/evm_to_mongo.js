import { decodeEventLog, parseAbi } from "https://esm.sh/viem";

const abi = parseAbi([
  "event Transfer(address indexed from, address indexed to, uint256 value)",
]);

export const config = {
  streamUrl: "http://localhost:7007",
  startingBlock: 5_000_000,
  network: "evm",
  filter: {
    header: {},
    logs: [
      {
        strict: true,
        topics: [
          "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
          null,
          null,
        ],
      },
    ],
  },
  sinkType: "mongo",
  sinkOptions: {
    database: "example",
    collectionName: "transfers",
  },
};

// Transform each block using the function defined in starknet.js.
export default function transform({ header, logs }) {
  return (logs ?? []).flatMap(
    ({
      address,
      topics,
      data,
      transactionHash,
      transactionIndex,
      logIndex,
    }) => {
      const decoded = decodeEventLog({ abi, topics, data });
      return [
        {
          eventName: decoded.eventName,
          tokenAddress: address,
          from: decoded.args.from,
          to: decoded.args.to,
          value: decoded.args.value.toString(10),

          transactionHash,
          transactionIndex: transactionIndex ?? 0,
          logIndex: logIndex ?? 0,

          blockNumber: header.number.toString(10),
          blockHash: header.hash,
          blockTimestamp: header.timestamp,
        },
      ];
    }
  );
}
