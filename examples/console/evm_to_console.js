export const config = {
  streamUrl: "https://sepolia.ethereum.a5a.ch",
  startingBlock: 5_000_000,
  network: "evm",
  filter: {
    header: {},
    logs: [
      {
        address: "0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238",
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
export default function transform({
  header,
  withdrawals,
  transactions,
  receipts,
  logs,
}) {
  return {
    header: {
      number: header.number,
      hash: header.hash,
      timestamp: header.timestamp,
    },
    logs,
  };
}
