use std::collections::{HashMap, hash_map::Entry};
use std::sync::Arc;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use datafusion::scalar::ScalarValue;
use futures_util::StreamExt;
use parquet::errors::ParquetError;
use tokio::sync::{
    mpsc::{Receiver, Sender, channel},
    oneshot,
};
use tokio_util::sync::CancellationToken;
use tokio_util::time::{DelayQueue, delay_queue};
use wings_metadata_core::admin::{NamespaceName, NamespaceRef, TopicName, TopicRef};

use crate::batcher::{BatcherError, BatcherResult, PartitionBatch, PartitionBatcher};

pub type BatchReply<D> = oneshot::Sender<BatcherResult<D>>;

pub struct Batch<D> {
    namespace: NamespaceRef,
    topic: TopicRef,
    partition: Option<ScalarValue>,
    records: RecordBatch,
    reply: BatchReply<D>,
}

pub struct NamespaceBatch<D> {
    pub namespace: NamespaceName,
    pub batches: Vec<(
        TopicName,
        Option<ScalarValue>,
        PartitionBatch<BatchReply<D>>,
    )>,
}

pub struct RemovedBatch<D> {
    pub batch: Option<NamespaceBatch<D>>,
    pub timer_key: delay_queue::Key,
    pub errors: Vec<(Vec<BatchReply<D>>, ParquetError)>,
}

/// Tracks the batching state for a single namespace.
pub struct NamespaceBatchState<D> {
    /// Time interval after which to flush the batch
    pub flush_interval: Duration,
    /// Size threshold after which to flush the batch
    pub flush_size: u64,
    /// Map from (topic, partition) to the partition batcher
    pub partitions: HashMap<(TopicName, Option<ScalarValue>), PartitionBatcher<BatchReply<D>>>,
    /// The current size of the batch
    pub current_size: u64,
    /// The timer key for the flush timer
    pub timer_key: delay_queue::Key,
}

impl<D> NamespaceBatchState<D> {
    /// Creates a new namespace batch with the given flush parameters.
    pub fn new(flush_interval: Duration, flush_size: u64, timer_key: delay_queue::Key) -> Self {
        Self {
            flush_interval,
            flush_size,
            partitions: HashMap::new(),
            current_size: 0,
            timer_key,
        }
    }
}

/// Configuration options for the batcher.
pub struct BatcherOptions {
    pub channel_size: usize,
}

impl Default for BatcherOptions {
    fn default() -> Self {
        Self { channel_size: 512 }
    }
}

/// Batches data for multiple partitions, grouped by namespace.
pub struct Batcher<D> {
    output_sender: Sender<NamespaceBatch<D>>,
    input_receiver: Receiver<Batch<D>>,
}

impl<D> Batcher<D> {
    /// Creates a new batcher with the given output channel and options.
    /// Returns the batcher instance and a sender for sending work to it.
    pub fn new(
        output_sender: Sender<NamespaceBatch<D>>,
        options: BatcherOptions,
    ) -> (Self, Sender<Batch<D>>) {
        let (input_sender, input_receiver) = channel(options.channel_size);
        let batcher = Self {
            output_sender,
            input_receiver,
        };
        (batcher, input_sender)
    }

    /// Creates a new batcher with the given output channel and default options.
    /// Returns the batcher instance and a sender for sending work to it.
    pub fn with_default_options(
        output_sender: Sender<NamespaceBatch<D>>,
    ) -> (Self, Sender<Batch<D>>) {
        Self::new(output_sender, BatcherOptions::default())
    }

    /// Runs the batcher, processing incoming batches until cancelled.
    pub async fn run(mut self, ct: CancellationToken) -> BatcherResult<()> {
        let mut state = InnerBatcher::default();
        let mut timers = DelayQueue::<NamespaceName>::new();

        loop {
            println!("timer empty? {:?}", timers.is_empty());
            tokio::select! {
                _ = ct.cancelled() => {
                    break;
                }
                expired = timers.next(), if !timers.is_empty() => {
                    println!("expired {:?}", expired);
                    let Some(entry) = expired else {
                        continue;
                    };

                    let Some(removed_batch) = state.expire_namespace(entry.into_inner()) else {
                        continue;
                    };

                    if let Some(namespace_batch) = removed_batch.batch {
                        let _ = self.output_sender.send(namespace_batch).await;
                    };


                    for (replies, err) in removed_batch.errors {
                        let err = Arc::new(err);
                        for reply in replies {
                            let _ = reply.send(Err(BatcherError::ParquetWriter { inner: err.clone() }.into()));
                        }
                    }
                }
                msg = self.input_receiver.recv() => {
                    let Some(batch) = msg else {
                        break;
                    };

                    if batch.topic.partition_key.is_none() && batch.partition.is_some() {
                        let error = BatcherError::Validation {
                            message: "topic does not specify a partition key but batch contains partition data"
                        };
                        let _ = batch.reply.send(Err(error.into()));
                        continue;
                    }

                    if batch.topic.name.parent() != &batch.namespace.name {
                        let error = BatcherError::Validation {
                            message: "topic namespace does not match provided namespace"
                        };
                        let _ = batch.reply.send(Err(error.into()));
                        continue;
                    }

                    match state.write_batch(batch, &mut timers) {
                        Err((reply, err)) => {
                            let err = BatcherError::ParquetWriter { inner: Arc::new(err) };
                            let _ = reply.send(Err(err.into()));
                            continue;
                        }
                        Ok(None) => {},
                        Ok(Some(removed_batch)) => {
                            timers.remove(&removed_batch.timer_key);

                            if let Some(namespace_batch) = removed_batch.batch {
                                let _ = self.output_sender.send(namespace_batch).await;
                            }

                            for (replies, err) in removed_batch.errors {
                                let err = Arc::new(err);
                                for reply in replies {
                                    let _ = reply.send(Err(BatcherError::ParquetWriter { inner: err.clone() }.into()));
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

struct InnerBatcher<D> {
    batches: HashMap<NamespaceName, NamespaceBatchState<D>>,
}

impl<D> Default for InnerBatcher<D> {
    fn default() -> Self {
        Self {
            batches: HashMap::default(),
        }
    }
}

impl<D> InnerBatcher<D> {
    fn write_batch(
        &mut self,
        batch: Batch<D>,
        delay_queue: &mut DelayQueue<NamespaceName>,
    ) -> std::result::Result<Option<RemovedBatch<D>>, (BatchReply<D>, ParquetError)> {
        let namespace = batch.namespace.name.clone();

        let batch_state = self
            .batches
            .entry(batch.namespace.name.clone())
            .or_insert_with(|| {
                let flush_interval = batch.namespace.flush_interval;
                let timer_key = delay_queue.insert(namespace.clone(), flush_interval);
                println!("pushed timer key {:?} {:?}", timer_key, flush_interval);
                NamespaceBatchState::new(
                    flush_interval,
                    batch.namespace.flush_size.as_u64(),
                    timer_key,
                )
            });

        let partition_batcher = match batch_state
            .partitions
            .entry((batch.topic.name.clone(), batch.partition))
        {
            Entry::Occupied(inner) => inner.into_mut(),
            Entry::Vacant(inner) => match PartitionBatcher::new(batch.topic.schema()) {
                Ok(batcher) => inner.insert(batcher),
                Err(err) => {
                    return Err((batch.reply, err));
                }
            },
        };

        let written = partition_batcher.write_batch(&batch.records, batch.reply)?;
        batch_state.current_size += written as u64;

        if batch_state.current_size >= batch_state.flush_size {
            let Some(state) = self.batches.remove(&namespace) else {
                return Ok(None);
            };

            return Ok(state.remove_namespace_batch(namespace).into());
        }

        Ok(None)
    }

    fn expire_namespace(&mut self, namespace: NamespaceName) -> Option<RemovedBatch<D>> {
        let state = self.batches.remove(&namespace)?;

        state.remove_namespace_batch(namespace).into()
    }
}

impl<D> NamespaceBatchState<D> {
    pub fn remove_namespace_batch(self, namespace: NamespaceName) -> RemovedBatch<D> {
        let mut batches = Vec::with_capacity(self.partitions.len());
        let mut errors = Vec::new();

        for (key, partition_batcher) in self.partitions.into_iter() {
            let (topic_name, partition) = key;
            match partition_batcher.finish() {
                Ok(batch) => batches.push((topic_name, partition, batch)),
                Err((replies, err)) => errors.push((replies, err)),
            }
        }

        let batch = if batches.is_empty() {
            None
        } else {
            Some(NamespaceBatch { namespace, batches })
        };

        RemovedBatch {
            batch,
            timer_key: self.timer_key,
            errors,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{fields, generate_test_batch};
    use bytesize::ByteSize;
    use datafusion::scalar::ScalarValue;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio::time::timeout;
    use wings_metadata_core::admin::{
        Namespace, NamespaceName, SecretName, TenantName, Topic, TopicName,
    };

    fn create_test_namespace() -> Namespace {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        Namespace {
            name: namespace_name,
            flush_interval: Duration::from_millis(50),
            flush_size: ByteSize(1000),
            default_object_store_config: SecretName::new("test").unwrap(),
            frozen_object_store_config: None,
        }
    }

    fn create_test_topic() -> Topic {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let topic_name = TopicName::new("test-topic", namespace_name).unwrap();
        Topic {
            name: topic_name,
            fields: fields(),
            partition_key: None,
        }
    }

    #[tokio::test]
    async fn test_batcher_happy_path() {
        let (output_sender, mut output_receiver) = mpsc::channel(10);
        let (batcher, input_sender) = Batcher::<()>::with_default_options(output_sender);

        let ct = CancellationToken::new();

        // Start batcher in background
        let batcher_handle = tokio::spawn({
            let ct = ct.clone();
            async move { batcher.run(ct).await }
        });

        // Create test batch
        let namespace = create_test_namespace().into();
        let topic = create_test_topic().into();
        let records = generate_test_batch(5);
        let (reply_sender, _reply_receiver) = tokio::sync::oneshot::channel();

        let batch = Batch {
            namespace,
            topic,
            partition: None,
            records,
            reply: reply_sender,
        };

        // Send batch
        input_sender.send(batch).await.unwrap();

        // Verify we get a successful output after timeout
        let reply_result = timeout(Duration::from_millis(500), output_receiver.recv()).await;
        assert!(reply_result.is_ok());

        let reply = reply_result.unwrap().unwrap();
        assert_eq!(reply.namespace.id(), "test-namespace");
        assert_eq!(reply.batches.len(), 1);

        // Clean up
        ct.cancel();
        let _ = batcher_handle.await;
    }

    #[tokio::test]
    async fn test_batcher_partition_validation_error() {
        let (output_sender, _output_receiver) = mpsc::channel(10);
        let (batcher, input_sender) = Batcher::<()>::with_default_options(output_sender);

        let ct = CancellationToken::new();

        // Start batcher in background
        let batcher_handle = tokio::spawn({
            let ct = ct.clone();
            async move { batcher.run(ct).await }
        });

        // Create test batch with partition but topic has no partition key
        let namespace = create_test_namespace().into();
        let topic = create_test_topic().into(); // partition_key is None
        let records = generate_test_batch(5);
        let (reply_sender, reply_receiver) = tokio::sync::oneshot::channel();

        let batch = Batch {
            namespace,
            topic,
            partition: Some(ScalarValue::Utf8(Some("invalid".to_string()))), // This should cause error
            records,
            reply: reply_sender,
        };

        // Send batch
        input_sender.send(batch).await.unwrap();

        // Verify we get an error reply
        let reply_result = timeout(Duration::from_millis(100), reply_receiver).await;
        assert!(reply_result.is_ok());
        let reply = reply_result.unwrap().unwrap();
        assert!(reply.is_err());

        let error = reply.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("topic does not specify a partition key")
        );

        // Clean up
        ct.cancel();
        let _ = batcher_handle.await;
    }

    #[tokio::test]
    async fn test_batcher_namespace_validation_error() {
        let (output_sender, _output_receiver) = mpsc::channel(10);
        let (batcher, input_sender) = Batcher::<()>::with_default_options(output_sender);

        let ct = CancellationToken::new();

        // Start batcher in background
        let batcher_handle = tokio::spawn({
            let ct = ct.clone();
            async move { batcher.run(ct).await }
        });

        // Create test batch with mismatched namespace
        let namespace = create_test_namespace().into();

        // Create a topic with different namespace
        let different_tenant = TenantName::new("different-tenant").unwrap();
        let different_namespace =
            NamespaceName::new("different-namespace", different_tenant).unwrap();
        let different_topic_name = TopicName::new("test-topic", different_namespace).unwrap();
        let mut topic = create_test_topic();
        topic.name = different_topic_name;

        let records = generate_test_batch(5);
        let (reply_sender, reply_receiver) = tokio::sync::oneshot::channel();

        let batch = Batch {
            namespace,
            topic: topic.into(),
            partition: None,
            records,
            reply: reply_sender,
        };

        // Send batch
        input_sender.send(batch).await.unwrap();

        // Verify we get an error reply
        let reply_result = timeout(Duration::from_millis(100), reply_receiver).await;
        assert!(reply_result.is_ok());
        let reply = reply_result.unwrap().unwrap();
        assert!(reply.is_err());

        let error = reply.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("topic namespace does not match provided namespace")
        );

        // Clean up
        ct.cancel();
        let _ = batcher_handle.await;
    }

    #[tokio::test]
    async fn test_batcher_multiple_batches() {
        let (output_sender, mut output_receiver) = mpsc::channel(10);
        let (batcher, input_sender) = Batcher::<()>::with_default_options(output_sender);

        let ct = CancellationToken::new();

        // Start batcher in background
        let batcher_handle = tokio::spawn({
            let ct = ct.clone();
            async move { batcher.run(ct).await }
        });

        // Send multiple batches
        for i in 0..3 {
            let namespace = create_test_namespace().into();
            let topic = create_test_topic().into();
            let records = generate_test_batch(i + 1);
            let (reply_sender, _reply_receiver) = tokio::sync::oneshot::channel();

            let batch = Batch {
                namespace,
                topic,
                partition: None,
                records,
                reply: reply_sender,
            };

            input_sender.send(batch).await.unwrap();
        }

        // Verify we get a successful output after timeout
        let reply_result = timeout(Duration::from_millis(500), output_receiver.recv()).await;
        assert!(reply_result.is_ok());

        let reply = reply_result.unwrap().unwrap();
        assert_eq!(reply.namespace.id(), "test-namespace");
        // Only one partition
        assert_eq!(reply.batches.len(), 1);
        let partition = reply.batches.first().unwrap();
        assert_eq!(partition.0.id(), "test-topic");
        assert!(partition.1.is_none());
        assert_eq!(partition.2.metadata.len(), 3);

        // Clean up
        ct.cancel();
        let _ = batcher_handle.await;
    }

    #[tokio::test]
    async fn test_batcher_cancellation() {
        let (output_sender, _output_receiver) = mpsc::channel(10);
        let (batcher, _input_sender) = Batcher::<()>::with_default_options(output_sender);

        let ct = CancellationToken::new();

        // Start batcher in background
        let batcher_handle = tokio::spawn({
            let ct = ct.clone();
            async move { batcher.run(ct).await }
        });

        // Cancel immediately
        ct.cancel();

        // Verify batcher exits gracefully
        let result = timeout(Duration::from_millis(100), batcher_handle).await;
        assert!(result.is_ok());
        let batcher_result = result.unwrap().unwrap();
        assert!(batcher_result.is_ok());
    }

    #[tokio::test]
    async fn test_batcher_channel_closed() {
        let (output_sender, _output_receiver) = mpsc::channel(10);
        let (batcher, input_sender) = Batcher::<()>::with_default_options(output_sender);

        let ct = CancellationToken::new();

        // Start batcher in background
        let batcher_handle = tokio::spawn({
            let ct = ct.clone();
            async move { batcher.run(ct).await }
        });

        // Close input channel
        drop(input_sender);

        // Verify batcher exits gracefully
        let result = timeout(Duration::from_millis(100), batcher_handle).await;
        assert!(result.is_ok());
        let batcher_result = result.unwrap().unwrap();
        assert!(batcher_result.is_ok());
    }

    #[tokio::test]
    async fn test_batcher_with_partitioned_topic() {
        let (output_sender, mut output_receiver) = mpsc::channel(10);
        let (batcher, input_sender) = Batcher::<()>::with_default_options(output_sender);

        let ct = CancellationToken::new();

        // Start batcher in background
        let batcher_handle = tokio::spawn({
            let ct = ct.clone();
            async move { batcher.run(ct).await }
        });

        // Create topic with partition key
        let namespace = create_test_namespace().into();
        let mut topic = create_test_topic();
        topic.partition_key = Some(0); // First field as partition key

        let records = generate_test_batch(5);
        let (reply_sender, _reply_receiver) = tokio::sync::oneshot::channel();

        let batch = Batch {
            namespace,
            topic: topic.into(),
            partition: Some(ScalarValue::Int32(Some(123))),
            records,
            reply: reply_sender,
        };

        // Send batch
        input_sender.send(batch).await.unwrap();

        // Verify we get a successful output after timeout
        let reply_result = timeout(Duration::from_millis(500), output_receiver.recv()).await;
        assert!(reply_result.is_ok());

        let reply = reply_result.unwrap().unwrap();
        assert_eq!(reply.namespace.id(), "test-namespace");
        assert_eq!(reply.batches.len(), 1);
        let partition = reply.batches.first().unwrap();
        assert_eq!(partition.0.id(), "test-topic");
        assert!(partition.1.is_some());
        assert_eq!(partition.2.metadata.len(), 1);

        // Clean up
        ct.cancel();
        let _ = batcher_handle.await;
    }
}
