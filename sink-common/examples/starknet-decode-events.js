// This is a Deno script that is invoked on every batch of data.

// Yes, we can import any Deno package!
import BigNumber from 'https://unpkg.com/bignumber.js@latest/bignumber.mjs';

// Define some token-specific constants.
const DECIMALS = 18;
const DENOMINATOR = BigNumber(10).pow(DECIMALS);

// Convert from uint256(low, high) to a decimal string.
function toAmount(low, high) {
  const amount = BigNumber(
    BigInt(low) + (BigInt(high) << 128n)
  );
  return amount.div(DENOMINATOR).toFixed();
}

export default function decodeEvents(batch) {
  return batch.flatMap(decodeBlock);
}

function decodeBlock(block) {
  const { header } = block;
  return (block?.events ?? []).map((evt, idx) => decodeEvent(header, evt, idx));
}

// Transform a single event by extracting the relevant data.
function decodeEvent(header, { event, transaction, receipt }, event_idx) {
  const { meta } = transaction;
  const transferFrom = event.data[0];
  const transferTo = event.data[1];
  const transferAmount = toAmount(event.data[2], event.data[3]);

  return {
    id: `${meta.hash}-${event_idx}`,
    blockHash: header.blockHash,
    blockNumber: header.blockNumber,
    blockTimestamp: header.timestamp,
    transactionHash: meta.hash,
    transferFrom,
    transferTo,
    transferAmount: transferAmount.toString(),
  }
}
