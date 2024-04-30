import { decodeEventLog, parseAbi } from "https://esm.sh/viem";

const abi = parseAbi([
  "event Transfer(address indexed from, address indexed to, uint256 value)",
]);

export const config = {
  streamUrl: "http://localhost:7007",
  // streamUrl: "https://sepolia.ethereum.a5a.ch",
  startingBlock: 5_000_000,
  network: "evm",
  filter: {
    header: {},
    logs: [
      {
        strict: false,
        topics: [
          "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
          null,
          null,
        ],
      },
    ],
  },
  sinkType: "console",
  sinkOptions: {
    database: "test",
    collectionName: "headers",
  },
};

// Transform each block using the function defined in starknet.js.
export default function transform({ header, logs }) {
  const count = [0, 0, 0, 0, 0];
  (logs ?? []).forEach(({ topics }) => {
    count[topics.length]++;
  });

  console.log(count);
  return {};
}
