export const config = {
  streamUrl: "http://localhost:7070",
  startingBlock: 0,
  network: "evm",
  filter: {
    logs: [
      {
        address: "0x25c4a76E7d118705e7Ea2e9b7d8C59930d8aCD3b",
      },
    ],
  },
  sinkType: "console",
  sinkOptions: {},
};

// Transform each block using the function defined in starknet.js.
export default function transform(block) {
  return block;
}
