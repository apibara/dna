// Ekubo Position NFT
import type { Block, Filter } from "https://esm.sh/@apibara/indexer/starknet";
import { hash, uint256 } from "https://esm.sh/starknet";

const contractAddress =
  "0x07987d7865f098F348CaC18c853AE20eDdfe3ea1c029a49652e3ffd1b85F976E";

export const filter: Filter = {
  header: {
    weak: true,
  },
  events: [
    {
      fromAddress: contractAddress,
      keys: [hash.getSelectorFromName("Transfer")],
    },
  ],
};

// Iterate over all token transfers to pick the new NFT owner.
export function updateNftOwner({ header, events }: Block) {
  const tokenIdCount = new Map<string, number>();
  const tokenIdToOwner = new Map<string, string>();

  for (const { event } of events) {
    const dest = event.data[1];
    const tokenId = uint256.uint256ToBN({
      low: event.data[2],
      high: event.data[3],
    });
    tokenIdToOwner.set(tokenId.toString(), dest);
    tokenIdCount.set(
      tokenId.toString(),
      (tokenIdCount.get(tokenId.toString()) ?? 0) + 1,
    );
  }

  return [...tokenIdToOwner.entries()].map(([tokenId, owner]) => ({
    entity: {
      tokenId,
    },
    update: {
      "$set": {
        tokenId,
        owner,
        updateAt: header?.timestamp,
        updateAtBlock: header?.blockNumber,
      },
      "$inc": {
        transferCount: tokenIdCount.get(tokenId) ?? 0,
      },
    },
  }));
}
