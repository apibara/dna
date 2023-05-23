// This transformation "flattens" each block into many rows of events.
// It adds information about the event's block and transaction.
export default function flatten(batch) {
  return batch.flatMap(flattenBlock);
}

function flattenBlock(block) {
  const { header, events } = block;
  return (events ?? []).map(event => combineEventWithHeader(event, header));
}

function combineEventWithHeader({ event, transaction, receipt }, header) {
  const { transactionHash, transactionIndex, actualFee } = receipt;
  return {
    transactionHash,
    transactionIndex,
    actualFee,
    ...event,
    ...header,
  };
}
