# Starknet WS Live Ingestion

This change adds an opt-in push-first live ingestion plane for Starknet pre-confirmed transaction and receipt data.

## Problem

The existing Starknet pending path polls `getBlockWithReceipts(PRE_CONFIRMED)` and then waits for `getStateUpdate(PRE_CONFIRMED)` before a pending DNA block can be written. Event and receipt-only consumers therefore inherit state update latency even though Starknet websocket subscriptions can deliver the relevant receipt/event payload earlier.

There is also a moving-tag failure mode: by the time DNA polls the `PRE_CONFIRMED` tag, the pushed websocket data may already refer to a block that the tag has moved past. Reducing the polling interval alone does not remove that race and does not make event delivery push-based.

## Architecture

Canonical HTTP ingestion remains the source of truth for:

- Backfill.
- Accepted and finalized blocks.
- Reorg reconciliation.
- State updates, storage diffs, nonces, and contract/class changes.
- Optional traces.

The new websocket live plane handles optimistic pending data:

- `starknet_subscribeNewTransactionReceipts` with `PRE_CONFIRMED`.
- `starknet_subscribeNewTransactions` with `PRE_CONFIRMED`.

Receipt and transaction notifications are correlated by `transaction_hash` in `StarknetLiveAssembler`. As soon as receipt/event data is available for the next pending block, DNA can write a pending block fragment without waiting for `getStateUpdate(PRE_CONFIRMED)`. If the transaction body has not arrived yet, receipt/event-only fragments are still emitted.

Accepted/finalized HTTP ingestion later writes canonical blocks and prunes live assembler entries through the accepted block number.

## Configuration

Live ingestion is disabled by default.

Enable it with:

```bash
STARKNET_WS_URL=ws://...
STARKNET_WS_LIVE_INGESTION_ENABLED=true
```

Setting `STARKNET_WS_LIVE_INGESTION_ENABLED=true` also enables the pending stream lane. The existing `STARKNET_INGEST_PRE_CONFIRMED=true` HTTP pending behavior remains available when websocket live ingestion is disabled.

## Benchmark

The benchmark command compares direct Starknet `subscribeEvents` first-seen time against DNA pending stream first-seen time for matching events. The DNA filter includes the matching event and receipt, but not the transaction body, so the measurement targets the receipt/event live path directly.

```bash
cargo run -p apibara-benchmark -- starknet-live-latency \
  --direct-ws-url ws://64.34.87.87:9545/ws/rpc/v0_10 \
  --stream-url http://localhost:7007 \
  --game-core-address 0x... \
  --game-event-key 0x... \
  --adventurer-id 0x...
```

It prints one CSV-style row per matched event:

```text
txHash,blockNumber,eventIndex,finality,directSubscribeEventsSeenMs,dnaStreamSeenMs,dnaMinusSubscribeEventsMs
```

Acceptance target: `p95DnaMinusSubscribeEventsMs <= 500` under normal live conditions.
