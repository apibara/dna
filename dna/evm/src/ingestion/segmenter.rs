use apibara_dna_common::{
    ingestion::{Snapshot, SnapshotChange},
    segment::SegmentOptions,
    storage::{LocalStorageBackend, StorageBackend},
};
use futures_util::Stream;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tokio_util::sync::CancellationToken;

use super::downloader::BlockEvent;

pub struct SegmenterService<
    S: StorageBackend + Send + Sync + 'static,
    I: Stream<Item = BlockEvent> + Unpin + Send + Sync + 'static,
> {
    local_storage: LocalStorageBackend,
    storage: S,
    ingestion_events: I,
}

impl<S, I> SegmenterService<S, I>
where
    S: StorageBackend + Send + Sync + 'static,
    I: Stream<Item = BlockEvent> + Unpin + Send + Sync + 'static,
{
    pub fn new(local_storage: LocalStorageBackend, storage: S, ingestion_events: I) -> Self {
        Self {
            local_storage,
            storage,
            ingestion_events,
        }
    }

    pub fn start(mut self, ct: CancellationToken) -> impl Stream<Item = SnapshotChange> {
        let (tx, rx) = mpsc::channel(128);

        tokio::spawn(async move {
            tx.send(SnapshotChange::Started(Snapshot {
                revision: 0,
                first_block_number: 0,
                segment_options: SegmentOptions::default(),
                group_count: 0,
            }))
            .await
            .unwrap();

            while let Some(xx) = self.ingestion_events.next().await {
                println!("{:?}", xx);
            }
        });

        ReceiverStream::new(rx)
    }
}
