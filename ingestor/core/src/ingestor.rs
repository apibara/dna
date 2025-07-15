use std::sync::Arc;

use error_stack::ResultExt;
use futures_util::{StreamExt, stream::FuturesUnordered};
use tokio::sync::{mpsc, oneshot};
use tokio_util::{sync::CancellationToken, time::DelayQueue};
use wings_metadata_core::offset_registry::OffsetRegistry;
use wings_object_store::ObjectStoreFactory;

use crate::{
    batch::{Batch, WriteInfo, WriteReplySender},
    batcher::NamespaceFolioWriter,
    error::{IngestorError, IngestorResult},
    uploader::{FolioToUpload, FolioUploader},
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
        let folio_uploader = Arc::new(self.uploader);
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

                    let Some(folio) = folio_writer.expire_namespace(entry.into_inner()) else {
                        continue;
                    };

                    // Try to remove any duplicate timer keys.
                    folio_timer.try_remove(&folio.timer_key);
                    let folio_to_upload = folio.reply_with_errors_and_continue();
                    upload_tasks.push(upload_and_commit_folio(folio_uploader.clone(), committer.clone(), folio_to_upload));
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
                        Ok(Some(folio)) => {
                            folio_timer.remove(&folio.timer_key);
                            let folio_to_upload = folio.reply_with_errors_and_continue();
                            upload_tasks.push(upload_and_commit_folio(folio_uploader.clone(), committer.clone(), folio_to_upload));
                        }
                        Err((reply, error)) => {
                            let _ = reply.send(Err(error));
                        }
                    }
                }
                task = upload_tasks.next(), if !upload_tasks.is_empty() => {
                    let Some(_) = task else {
                        break;
                    };
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
            .or_else(|err| Err(err).change_context(IngestorError::ChannelClosed))?;

        rx.await
            .or_else(|err| Err(err).change_context(IngestorError::ChannelClosed))?
    }
}

async fn upload_and_commit_folio(
    uploader: Arc<FolioUploader>,
    offset_registry: Arc<dyn OffsetRegistry>,
    folio: FolioToUpload,
) {
    let uploaded = uploader.upload_folio(folio).await.unwrap();
    // Create file
    // Upload file
    let committed = offset_registry
        .commit_folio(uploaded.namespace, uploaded.file_ref, &uploaded.batches)
        .await
        .change_context(IngestorError::Commit)
        .unwrap();
    // Commit file
    println!("Committed {:?}", committed);
}
