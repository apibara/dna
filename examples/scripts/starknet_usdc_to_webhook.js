export const config = {
  streamUrl: 'https://mainnet.starknet.a5a.ch',
  startingBlock: 100_000,
  filter: {
    header: {
      weak: false,
    },
  },
  batchSize: 1,
  network: 'starknet',
  sinkType: 'webhook',
  sinkOptions: {
    targetUrl: 'http://localhost:8080',
    raw: true,
  }
}

export default function transform(batch) {
  console.log('batch', batch)
  return {
    'batch': batch,
  }
}
