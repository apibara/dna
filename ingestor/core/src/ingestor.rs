use std::sync::Arc;

use error_stack::{ResultExt, report};
use futures_util::{StreamExt, stream::FuturesUnordered};
use tokio::sync::{mpsc, oneshot};
use tokio_util::{sync::CancellationToken, time::DelayQueue};
use wings_metadata_core::offset_registry::OffsetRegistry;
use wings_object_store::ObjectStoreFactory;

use crate::{
    batch::{Batch, WriteInfo, WriteReplySender},
    batcher::NamespaceFolioWriter,
    error::{IngestorError, IngestorResult},
    types::{
        CommittedNamespaceFolioMetadata, CommittedPartitionFolioMetadata, NamespaceFolio,
        ReplyWithError,
    },
    uploader::FolioUploader,
};

pub struct BatchIngestor {
    tx: mpsc::Sender<BatchWithReply>,
    rx: mpsc::Receiver<BatchWithReply>,
    uploader: FolioUploader,
    offset_registry: Arc<dyn OffsetRegistry>,
}

#[derive(Clone)]
pub struct BatchIngestorClient {
    tx: mpsc::Sender<BatchWithReply>,
}

pub struct BatchWithReply {
    pub batch: Batch,
    pub reply: WriteReplySender,
}

pub async fn run_background_ingestor(
    ingestor: BatchIngestor,
    ct: CancellationToken,
) -> IngestorResult<()> {
    ingestor.run(ct).await
}

impl BatchIngestor {
    pub fn new(
        object_store_factory: Arc<dyn ObjectStoreFactory>,
        offset_registry: Arc<dyn OffsetRegistry>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(100);
        let uploader = FolioUploader::new_ulid(object_store_factory);

        Self {
            tx,
            rx,
            uploader,
            offset_registry,
        }
    }

    pub fn client(&self) -> BatchIngestorClient {
        BatchIngestorClient {
            tx: self.tx.clone(),
        }
    }

    async fn run(mut self, ct: CancellationToken) -> IngestorResult<()> {
        let _ct_guard = ct.child_token().drop_guard();
        let mut folio_timer = DelayQueue::new();
        let mut folio_writer = NamespaceFolioWriter::default();
        let folio_uploader = self.uploader;
        let committer = self.offset_registry;
        let mut upload_tasks = FuturesUnordered::new();

        loop {
            tokio::select! {
                _ = ct.cancelled() => {
                    break;
                }
                expired = folio_timer.next(), if !folio_timer.is_empty() => {
                    let Some(entry) = expired else {
                        continue;
                    };

                    let Some((folio, errors)) = folio_writer.expire_namespace(entry.into_inner()) else {
                        continue;
                    };

                    // Try to remove any duplicate timer keys.
                    folio_timer.try_remove(&folio.timer_key);

                    for error in errors {
                        error.send();
                    }

                    upload_tasks.push(upload_and_commit_folio(folio_uploader.clone(), committer.clone(), folio));
                }
                batch_with_reply = self.rx.recv() => {
                    let Some(BatchWithReply { batch, reply }) = batch_with_reply else {
                        break;
                    };

                    if let Err(validation_error) = batch.validate() {
                        let _ = reply.send(Err(validation_error));
                        continue;
                    }

                    match folio_writer.write_batch(batch, reply, &mut folio_timer) {
                        Ok(None) => {},
                        Ok(Some((folio, errors))) => {
                            folio_timer.remove(&folio.timer_key);

                            for error in errors {
                                error.send();
                            }

                            upload_tasks.push(upload_and_commit_folio(folio_uploader.clone(), committer.clone(), folio));
                        }
                        Err(error) => {
                            error.send();
                        }
                    }
                }
                task = upload_tasks.next(), if !upload_tasks.is_empty() => {
                    match task {
                        None => break,
                        Some(Ok(committed_namespace)) => {
                            reply_with_committed_offset(committed_namespace);
                        }
                        Some(Err(errors)) => {
                            for error in errors {
                                error.send();
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

impl BatchIngestorClient {
    pub async fn write(&self, batch: Batch) -> IngestorResult<WriteInfo> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send(BatchWithReply { batch, reply: tx })
            .await
            .or_else(|err| {
                Err(err).change_context(IngestorError::Internal("channel closed".to_string()))
            })?;

        rx.await.or_else(|err| {
            Err(err).change_context(IngestorError::Internal("channel closed".to_string()))
        })?
    }
}

async fn upload_and_commit_folio(
    uploader: FolioUploader,
    offset_registry: Arc<dyn OffsetRegistry>,
    folio: NamespaceFolio,
) -> std::result::Result<CommittedNamespaceFolioMetadata, Vec<ReplyWithError>> {
    let uploaded = uploader.upload_folio(folio).await?;

    let mut batches_to_commit = Vec::new();
    let mut batch_context = Vec::new();
    for partition in uploaded.partitions.into_iter() {
        let (batch, context) = partition.into_batch_to_commit();
        batch_context.push((
            batch.topic_name.clone(),
            batch.partition_value.clone(),
            context,
        ));
        batches_to_commit.push(batch);
    }

    let commits = match offset_registry
        .commit_folio(
            uploaded.namespace.name.clone(),
            uploaded.file_ref,
            &batches_to_commit,
        )
        .await
    {
        Ok(commits) => commits,
        Err(err) => {
            let error = IngestorError::Internal(format!("failed to commit folio: {err}"));

            let replies = batch_context
                .into_iter()
                .flat_map(|p| {
                    p.2.into_iter().map(|b| ReplyWithError {
                        reply: b.reply,
                        error: report!(error.clone()),
                    })
                })
                .collect::<Vec<_>>();

            return Err(replies);
        }
    };

    let committed_partitions = commits
        .into_iter()
        .zip(batch_context.into_iter())
        .map(|(committed, (topic_name, partition_value, batches))| {
            CommittedPartitionFolioMetadata {
                topic_name,
                partition_value,
                start_offset: committed.start_offset,
                end_offset: committed.end_offset,
                batches,
            }
        })
        .collect::<Vec<_>>();

    Ok(CommittedNamespaceFolioMetadata {
        namespace: uploaded.namespace,
        partitions: committed_partitions,
    })
}

fn reply_with_committed_offset(committed_namespace: CommittedNamespaceFolioMetadata) {
    for partition in committed_namespace.partitions.into_iter() {
        let mut current_offset = partition.start_offset;
        for batch in partition.batches.into_iter() {
            let start_offset = current_offset;
            let end_offset = current_offset + (batch.num_messages as u64) - 1;
            let _ = batch.reply.send(Ok(WriteInfo {
                start_offset,
                end_offset,
            }));
            current_offset = end_offset + 1;
        }
        assert_eq!(partition.end_offset, current_offset - 1);
    }
}
