import {
  decodeEventLog,
  encodeEventTopics,
  parseAbi,
} from "https://esm.sh/viem";

const kv = globalThis.Script.kv;

const abi = parseAbi([
  "event Transfer(address indexed from, address indexed to, uint256 value)",
]);

export const config = {
  // streamUrl: "http://localhost:7007",
  streamUrl: "https://sepolia.ethereum.a5a.ch",
  startingBlock: 5_000_000,
  network: "evm",
  filter: {
    header: {},
    logs: [
      {
        strict: true,
        topics: encodeEventTopics({
          abi,
          eventName: "Transfer",
          args: [null, null],
        }),
      },
    ],
  },
  sinkType: "console",
  sinkOptions: {
    database: "test",
    collectionName: "headers",
  },
};

export default function transform({ header, logs }) {
  return (logs ?? []).flatMap(({ address, topics, data, transactionHash }) => {
    const decoded = decodeEventLog({ abi, topics, data });

    const key = `balance:${decoded.args.from}`;
    const { count } = kv.get(key) ?? { count: 0 };
    kv.set(key, { count: count + 1 });

    return [
      // {
      //   address: decoded.args.from,
      //   count,
      // },
    ];
    // return {
    //   tokenAddress: address,
    //   eventName: decoded.eventName,
    //   from: decoded.args.from,
    //   to: decoded.args.to,
    //   value: decoded.args.value.toString(10),

    //   transactionHash,
    //   blockNumber: header.number.toString(10),
    //   blockHash: header.hash,
    //   blockTimestamp: header.timestamp,
    // };
  });
}
