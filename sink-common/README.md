# Sink helper traits and functions

This crate provides a `Sink` and a `SinkConnector` that are useful when
implementing integrations. It also includes `ConfigurationArgs` that
can be used to quickly add the common command line flags that are used with
integrations.


## Usage

If you're developing an integration in this repository:

 * Create a new crate for the integration with `cargo new --lib sink-my-integration`.
   The crate folder should start with `sink-`, and the crate name with `apibara-sink-`.
 * Add `apibara-core`, `apibara-observability` and `apibara-sink-common` to its dependencies.
 * Create a `bin.rs` file and update `Cargo.toml` to provide both a library and binary.
   Use `sink-webhook` as a reference.


## Command Line Arguments

This section describes the two most important command line arguments: `--filter` and `--transform`.
Both flags support receiving their argument directly as string or, if the
argument starts with `@`, as a path to a file.


### Filter

The `--filter` flag is used to specify what data to receive in the stream.
This argument is network-specific (Starknet, Ethereum) and contains the
json-encoded `Filter` object.

**Notice**: Starknet `FieldElement` messages are encoded as hex strings to
improve developer experience.


**Example**: The following example streams `Transfer` events (key =
`0x99cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9`) for the
USDC contract. Block headers are included only if any event matches (`weak =
true`).

```json
{
  "header": { "weak": true },
  "events": [
    {
      "from_address": "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
      "keys": [
        "0x99cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9"
      ]
    }
  ]
}
```


### Transform

Data can be transformed before it's sent to the downstream integration. This is achieved
evaluating a Javascript or Typescript function on each batch of data.
The input of the program is a list of network-specific data (usually, blocks) and the
output can be a list of anything. The resulting output is sent to the integration.

**Example**: The following Javascript program "flattens" blocks into a list of all
their events.

```js
export default function flatten(batch) {
  return batch.flatMap(flattenBlock);
}

function flattenBlock(block) {
  const { header, events } = block;
  return (events ?? []).map(event => ({ ...event, ...header }));
}
```

A batch that looks like the following:

```json
[
  {
    "header": HeaderA,
    "events": [EventA, EventB]
  },
  {
    "header": HeaderB,
    "events": [EventC, EventD]
  }
]
```

Becomes the following "flat" list of events:

```json
[
  EventA,
  EventB,
  EventC,
  EventD
]
```

This list of events can be inserted directly into a database or appended to a
csv file.


## Persist state between restarts

Sinks can persist state (= information about the last indexed block) between
restarts by connecting to an etcd cluster.

Start a local development etcd cluster with:

```
docker run --rm -p 2370:2379 -e ALLOW_NONE_AUTHENTICATION=yes bitnami/etcd
```

Then use the following cli flags to persist state:

 - `--persist-to-etcd`: if set, turns on persistence. Pass the URL to the etcd
   cluster, for example `localhost:2379`.
 - `--sink-id`: the unique sink id, can be any string.

When persistence is enabled, the sink will acquire a lock on start to avoid
running multiple instances of the same indexer in parallel.
This behaviour is needed in case your scheduler (e.g. Kubernetes) accidentally
schedules two instances of the same indexer.
