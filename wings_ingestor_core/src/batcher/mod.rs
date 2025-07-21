use std::collections::{HashMap, hash_map::Entry};

use tokio_util::time::{DelayQueue, delay_queue};
use wings_metadata_core::{
    admin::{NamespaceName, NamespaceRef, TopicName},
    partition::PartitionValue,
};

use crate::{
    batch::{Batch, WriteReplySender},
    types::{NamespaceFolio, ReplyWithError},
};

use self::partition::PartitionFolioWriter;

mod partition;

#[derive(Default)]
pub struct NamespaceFolioWriter {
    namespaces: HashMap<NamespaceName, NamespaceFolioState>,
}

struct NamespaceFolioState {
    /// The namespace
    pub namespace: NamespaceRef,
    /// Size threshold after which to flush the folio
    pub flush_size: u64,
    /// Map from (topic, partition) to the partition folio writer
    pub partitions: HashMap<(TopicName, Option<PartitionValue>), PartitionFolioWriter>,
    /// The current size of the folio
    pub current_size: u64,
    /// The timer key for the flush timer
    pub timer_key: delay_queue::Key,
}

impl NamespaceFolioWriter {
    /// Writes a batch to the namespace folio, returning the flushed folio if it's full.
    pub fn write_batch(
        &mut self,
        batch: Batch,
        reply: WriteReplySender,
        delay_queue: &mut DelayQueue<NamespaceName>,
    ) -> std::result::Result<Option<(NamespaceFolio, Vec<ReplyWithError>)>, ReplyWithError> {
        let namespace = batch.namespace.name.clone();

        let folio_state = self
            .namespaces
            .entry(batch.namespace.name.clone())
            .or_insert_with(|| {
                let flush_interval = batch.namespace.flush_interval;
                let timer_key = delay_queue.insert(namespace.clone(), flush_interval);
                NamespaceFolioState::new(batch.namespace, timer_key)
            });

        let folio_writer = match folio_state
            .partitions
            .entry((batch.topic.name.clone(), batch.partition.clone()))
        {
            Entry::Occupied(inner) => inner.into_mut(),
            Entry::Vacant(inner) => {
                match PartitionFolioWriter::new(
                    batch.topic.name.clone(),
                    batch.partition,
                    batch.topic.schema_without_partition_column(),
                ) {
                    Ok(writer) => inner.insert(writer),
                    Err(error) => return Err(ReplyWithError { reply, error }),
                }
            }
        };

        let written = folio_writer.write_batch(&batch.records, reply)?;
        folio_state.current_size += written as u64;

        if folio_state.current_size >= folio_state.flush_size {
            let Some(state) = self.namespaces.remove(&namespace) else {
                return Ok(None);
            };

            return Ok(state.finish().into());
        }

        Ok(None)
    }

    /// Expires a namespace, flushing any pending data.
    pub fn expire_namespace(
        &mut self,
        namespace: NamespaceName,
    ) -> Option<(NamespaceFolio, Vec<ReplyWithError>)> {
        let state = self.namespaces.remove(&namespace)?;

        state.finish().into()
    }
}

impl NamespaceFolioState {
    /// Creates a new namespace folio writer with the given flush parameters.
    pub fn new(namespace: NamespaceRef, timer_key: delay_queue::Key) -> Self {
        let flush_size = namespace.flush_size.as_u64();
        Self {
            namespace,
            flush_size,
            partitions: HashMap::new(),
            current_size: 0,
            timer_key,
        }
    }

    /// Flushes the namespace folio, ready for the next step.
    pub fn finish(self) -> (NamespaceFolio, Vec<ReplyWithError>) {
        let mut partitions = Vec::with_capacity(self.partitions.len());
        let mut errors = Vec::new();

        for (key, partition_writer) in self.partitions.into_iter() {
            let (topic_name, partition) = key;
            match partition_writer.finish(topic_name, partition) {
                Ok(folio) => partitions.push(folio),
                Err(err) => errors.extend(err.into_iter()),
            }
        }

        let folio = NamespaceFolio {
            namespace: self.namespace,
            timer_key: self.timer_key,
            partitions,
        };
        (folio, errors)
    }
}
