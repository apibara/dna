# Airfoil Wings: commands and style guide

## Project overview

Airfoil Wings (from now, Wings) is a message queue written in Rust. Its main purpose is to provide a decoupled event bus for agent to agent communication. It's designed to be lightweight, efficient, and easy to use.

**Open standards**: Wings use Parquet files for long-term storage. Topics are compacted and partitioned so that any system that can read Parquet and Iceberg can consume the data.

**Multi tenant**: Wings supports multi-tenancy, allowing multiple organizations to share the same instance of Wings.

**Built-in schema registry**: Topic creation requires a schema. All ingested data must conform to the schema.

**Partitioning**: Topics are partitioned by key, with one partition for each unique value. Messages within the same partition are ordered, while no ordering is guaranteed between partitions.

**Filtering**: Consumers can filter messages based on their content. Ordering is guaranteed as long as data belongs to the same partition.

**Decoupled data from metadata**: Wings separates data from metadata. The metadata store is the only stateful component of Wings. It stores information about tenants, namespaces, topics, and partitions.

**Decouple storage from compute**: Data is stored on object storage. The data layer (ingestor and query server) are stateless components that can be scaled horizontally.

## The stack

Wings builds on top of existing technologies:

- Rust
- Apache DataFusion
- Apache Arrow
- Apache Parquet
- Apache Iceberg

When possible, Wings uses open standards to ensure compatibility with other systems.

## Components

At a high-level, Wings consists of the following components:

**Ingestor**: Receives messages from producers and writes them to object storage. To limit S3 PUT requests, the ingestor batches messages and writes them in bulk. Roughly speaking, this is done every 250ms or 8MiB (configurable, use this to get a ballpark idea).

**Query**: Receives queries from consumers and serves them data. Queries can be one-off or streaming.

**Metadata Store**: The metadata store is a stateful component that stores information about tenants, namespaces, topics, and partitions. This component is also used to assign offsets (sequence numbers) to messages.

**Compaction**: This component is responsible for compacting data in Parquet files. It starts by fetching "live" data from the object store and generate the first generation of compacted Parquet files. Files are later compacted further into larger and larger files, until they reach a certain target file size (around 250MiB).

## Data

As we mentioned before, long-term storage is handled by Parquet files uploaded on object storage, with one Parquet file per partition.

Storing "live" data in Parquet would be too inefficient (all data in a single Parquet file must have the same schema). Wings stores live data from multiple topics and partitions in the same binary file to reduce S3 operations.

Partition columns are not physically stored in the Parquet files, but they are encoded in the file path. As such, topics can only be partitioned by a single column.

## Vocabulary

**Batch**: a batch is a group of messages (same topic and partition) pushed at the same time.

**Folio**: a folio is a collection of messages (by namespace). Data in a folio is grouped and sorted by topic and partition and contains data from multiple batches.

**Segment**: a segment file is a Parquet file containing compacted data from a single topic and partition.

**Segment's generation**: a segment's generation is the number of times it has been compacted.

## Commands

- `cargo check`: Check the project for errors.
- `cargo build`: Build the project.
- `cargo test`: Run the unit tests.

## Repository organization

**Common**

 - `common/object_store`: functions and traits to work with object storage.
 
**Metadata**
 
 - `metadata/core`: functions to work with metadata and in-memory implementation.
 
**Ingestor**
 
 - `ingestor/core`: functions and traits used by all ingestor implementations.
 - `ingestor/http`: ingestor that receives data over HTTP.

## Style guide

We follow the standard Rust style guide.

**Important**: Do not call it "Wings message queue system", simply use "Wings".

**Ultra Important**: You are an expert Rust developer, as such you do NOT need to add comments inside the implementation describing WHAT the code does. Your comments should describe WHY the code is written the way it is. The only place where you should describe WHAT the code does is in the documentation.

### Error handling

We use the `error_stack` and `thiserror` crates for error handling. We create a custom error type per module (usually, in the `error.rs` file). Both the error and result types are exported by the module.

We can use the `attach_printable` and `attach_printable_lazy` methods to attach additional information to errors. We do this to provide more context about the error to end users.

```rust
// in error.rs
use error_stack::ResultExt;
use thiserror::Error;

/// Example AppError.
#[derive(Error, Debug)]
pub enum AppError {
    #[error("validation error: {message}")]
    Validation { message: &'static str },
    #[error("file not found: {filename}")]
    FileNotFound { filename: String },
}

pub type AppResult<T> = error_stack::Result<T, AppError>;

// somewhere else
fn do_something(input: &str) -> AppResult<()> {
    do_fallible(input).change_context(AppError::FileNotFound {
        filename: input.to_string(),
    })
    .attach_printable("failed to do fallible")
}

```

### Task cancellation

All long-running async operations must be cancellable. We use `tokio_util::sync::CancellationToken` to signal cancellation to the task.

There is one root cancellation token, usually created by the main entry point. We create child tokens using either of the following methods:

- `child_token`: this creates a new child token that is cancelled when the current token is cancelled. Unlike a cloned token, cancelling a child token does not cancel the parent token.
- `clone`: this creates a new token that is cancelled when the current token is cancelled. Unlike a child token, cancelling a cloned token cancels the parent token.

Cancellation is signaled by calling `cancel` on the cancellation token. This is usually done by the main entry point when the user requests cancellation (e.g. by pressing ctrl-c).

If a service spawns a number of child tasks, it should also cancel them on exit. This is usally done by 1) creating a `child_token` for the current service, 2) creating a `drop_guard` for this new token, and 3) `clone`ing the token for each child task.

When cloning a cancellation token, we use a scoped shadowed variable to avoid naming the token `ct_clone`.

```rust
pub async fn do_something(ct: CancellationToken) {
    tokio::spawn({
        let ct = ct.clone();
        async move {
            do_something_else(ct).await;
        }
    })
}
```
