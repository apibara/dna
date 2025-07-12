use error_stack::ResultExt;
use futures_util::StreamExt;
use tokio::sync::{mpsc, oneshot};
use tokio_util::{sync::CancellationToken, time::DelayQueue};

use crate::{
    batch::{Batch, WriteInfo, WriteReplySender},
    batcher::NamespaceFolioWriter,
    error::{IngestorError, IngestorResult},
    uploader::FolioToUpload,
};

pub struct BatchIngestor {
    tx: mpsc::Sender<BatchWithReply>,
    rx: mpsc::Receiver<BatchWithReply>,
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
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(100);

        Self { tx, rx }
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

                    // TODO: why does `remove` panic? The key should be there.
                    folio_timer.try_remove(&folio.timer_key);
                    let folio_to_upload = folio.reply_with_errors_and_continue();
                    stub_upload_and_commit(folio_to_upload);
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
                            stub_upload_and_commit(folio_to_upload);
                        }
                        Err((reply, error)) => {
                            let _ = reply.send(Err(error));
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
            .or_else(|err| Err(err).change_context(IngestorError::ChannelClosed))?;

        rx.await
            .or_else(|err| Err(err).change_context(IngestorError::ChannelClosed))?
    }
}

fn stub_upload_and_commit(folio: FolioToUpload) {
    println!("Upload and commit {}", folio.namespace);

    for partition in folio.partitions.into_iter() {
        let data_size = bytesize::ByteSize(partition.folio.data.len() as _);
        println!(
            "Commit {} {:?} {}",
            partition.topic, partition.partition, data_size
        );
        for batch in partition.folio.batches.into_iter() {
            let _ = batch.reply.send(Ok(WriteInfo {
                start_offset: 0,
                end_offset: batch.batch_size as _,
            }));
        }
    }
}
