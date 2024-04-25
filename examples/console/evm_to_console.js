export const config = {
  streamUrl: "http://localhost:7007",
  startingBlock: 5_301_000,
  network: "evm",
  filter: {
    header: {},
    // transactions: [
    //   {
    //     includeLogs: true,
    //     includeReceipt: true,
    //   },
    // ],
    // withdrawals: [
    //   {
    //     address: "0x25c4a76E7d118705e7Ea2e9b7d8C59930d8aCD3b",
    //   },
    // ],
    logs: [
      {
        address: "0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238",
        // includeTransaction: true,
        // includeReceipt: true,
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
    // withdrawals: (withdrawals ?? []).length,
    // transactions: (transactions ?? []).length,
    // receipts: (receipts ?? []).length,
    // logs: (logs ?? []).length,
  };
}
