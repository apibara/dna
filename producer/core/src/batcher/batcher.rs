use std::collections::{HashMap, hash_map::Entry};
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use datafusion::scalar::ScalarValue;
use error_stack::ResultExt;
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
    namespace: NamespaceName,
    batches: Vec<(
        TopicName,
        Option<ScalarValue>,
        PartitionBatch<BatchReply<D>>,
    )>,
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
            tokio::select! {
                _ = ct.cancelled() => {
                    break;
                }
                expired = timers.next(), if !timers.is_empty() => {
                    let Some(entry) = expired else {
                        continue;
                    };

                    // TODO: we need to handle the case where the `expire_namespace` call fails.
                    // In that case, we need to iterate over all reply channels and send an error.
                    if let Ok(Some(namespace_batch)) = state.expire_namespace(entry.into_inner()) {
                        let _ = self.output_sender.send(namespace_batch);
                    };
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
                            let err = Err(err)
                                .change_context(BatcherError::ParquetWriter)
                                .attach_printable("failed to add data to Parquet batcher");
                            let _ = reply.send(err);
                            continue;
                        }
                        Ok(None) => {},
                        Ok(Some((namespace_batch, timer_key))) => {
                            timers.remove(&timer_key);
                            let _ = self.output_sender.send(namespace_batch);
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
    ) -> std::result::Result<
        Option<(NamespaceBatch<D>, delay_queue::Key)>,
        (BatchReply<D>, ParquetError),
    > {
        let namespace = batch.namespace.name.clone();

        let batch_state = self
            .batches
            .entry(batch.namespace.name.clone())
            .or_insert_with(|| {
                let flush_interval = batch.namespace.flush_interval;
                let timer_key = delay_queue.insert(namespace.clone(), flush_interval);
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
                Err(err) => return Err((batch.reply, err)),
            },
        };

        let written = partition_batcher.write_batch(&batch.records, batch.reply)?;
        batch_state.current_size += written as u64;

        if batch_state.current_size >= batch_state.flush_size {
            let Some(state) = self.batches.remove(&namespace) else {
                return Ok(None);
            };
            // TODO: fix error handling
            let (batch, timer_key) = state.into_namespace_batch(namespace).unwrap();
            return Ok(Some((batch, timer_key)));
        }

        Ok(None)
    }

    fn expire_namespace(
        &mut self,
        namespace: NamespaceName,
    ) -> BatcherResult<Option<NamespaceBatch<D>>> {
        let Some(state) = self.batches.remove(&namespace) else {
            return Ok(None);
        };

        state
            .into_namespace_batch(namespace)
            .map(|(batch, _)| Some(batch))
    }
}

impl<D> NamespaceBatchState<D> {
    pub fn into_namespace_batch(
        self,
        namespace: NamespaceName,
    ) -> BatcherResult<(NamespaceBatch<D>, delay_queue::Key)> {
        let mut batches = Vec::with_capacity(self.partitions.len());
        for (key, partition_batcher) in self.partitions.into_iter() {
            let (topic_name, partition) = key;
            let batch_data = partition_batcher
                .finish()
                .attach_printable_lazy(|| format!("topic_name: {topic_name}"))?;
            batches.push((topic_name, partition, batch_data));
        }

        Ok((NamespaceBatch { namespace, batches }, self.timer_key))
    }
}
