# Indexer Script

This crate provides the scripting engine used to run indexers.

And indexer script is a Typescript or Javascript file that exports two values:

- `config`: the indexer configuration.
- a default function: the function used to transform each batch of data.

```ts
export const config = {
  // general configuration.
  indexerType: "postgres",
  indexerOptions: {
    // postgres-specific options.
  },
};

export default function transform(batch) {
  // do something with the batch of data.
  return batch;
}
```

If you're implementing a new sink type, you shouldn't use this crate directly
and should use `apibara-sink-common` instead.
