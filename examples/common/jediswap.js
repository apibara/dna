import { hash } from "https://esm.run/starknet@5.14";

const PAIR_CREATED = selector("PairCreated");
const UPGRADED = selector("Upgraded");
const ADMIN_CHANGED = selector("AdminChanged");
const BURN = selector("Burn");
const MINT = selector("Mint");
const SWAP = selector("Swap");
const SYNC = selector("Sync");
const APPROVAL = selector("Approval");
const TRANSFER = selector("Transfer");

// Stream `PairCreated` events from JediSwap.
export const filter = {
  header: { weak: true },
  events: [
    {
      fromAddress:
        "0x00dad44c139a476c7a17fc8141e6db680e9abc9f56fe249a105094c44382c2fd",
      keys: [PAIR_CREATED],
      includeReceipt: false,
    },
  ],
};

// Export a `factory` function to enable factory mode.
// This function is called with data from the `filter` above
// and returns a filter that is used to stream child data.
// Filters from subsequent calls to `factory` are merged.
export function factory({ header, events }) {
  // Create a filter with events for all pools created in this block.
  const poolEvents = events.flatMap(({ event }) => {
    const pairAddress = event.data[2];
    return [
      UPGRADED,
      ADMIN_CHANGED,
      BURN,
      MINT,
      SWAP,
      SYNC,
      APPROVAL,
      TRANSFER,
    ].map((eventKey) => ({
      fromAddress: pairAddress,
      keys: [eventKey],
      includeReceipt: false,
    }));
  });

  // Insert data about the pools created in this block.
  // Values returned in `data` are handled like values returned from
  // `transform`.
  const pools = events.flatMap(({ event, transaction }) => {
    const token0 = event.data[0];
    const token1 = event.data[1];
    const pairAddress = event.data[2];
    return {
      type: "PairCreated",
      createdAt: header.timestamp,
      createdAtBlockNumber: +header.blockNumber,
      createdAtTxHash: transaction.meta.hash,
      poolId: pairAddress,
      token0Id: token0,
      token1Id: token1,
    };
  });

  return {
    filter: {
      header: { weak: true },
      events: poolEvents,
    },
    data: pools,
  };
}

// The transform function is invoked with data from the child filter
// created by the factory function above.
export function transform({ header, events }) {
  return events.flatMap(({ event, transaction }) => {
    const txMeta = {
      pairId: event.fromAddress,
      createdAt: header.timestamp,
      createdAtBlockNumber: +header.blockNumber,
      createdAtTxHash: transaction.meta.hash,
    };

    switch (event.keys[0]) {
      case UPGRADED:
        return {
          type: "Upgraded",
          ...txMeta,
        };
      case ADMIN_CHANGED:
        return {
          type: "AdminChanged",
          ...txMeta,
        };
      case BURN:
        return {
          type: "Burn",
          ...txMeta,
        };
      case MINT:
        return {
          type: "Mint",
          ...txMeta,
        };
      case SWAP:
        return {
          type: "Swap",
          ...txMeta,
        };
      case SYNC:
        return {
          type: "Sync",
          ...txMeta,
        };
      case APPROVAL:
        return {
          type: "Approval",
          ...txMeta,
        };
      case TRANSFER:
        return {
          type: "Transfer",
          ...txMeta,
        };
      default:
        console.log("UNKNOWN");
        return [];
    }
  });
}

// Pad selector to match DNA felt length.
function selector(name) {
  const bn = BigInt(hash.getSelectorFromName(name));
  const padded = bn.toString(16).padStart(64, "0");
  return "0x" + padded;
}
