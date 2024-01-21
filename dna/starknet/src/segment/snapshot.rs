use apibara_dna_common::{
    error::{DnaError, Result},
    segment::SegmentOptions,
    storage::{FormattedSize, StorageBackend},
};
use error_stack::ResultExt;
use flatbuffers::FlatBufferBuilder;
use tokio::io::AsyncWriteExt;
use tracing::info;

use super::{store, SegmentGroupEvent};

pub struct Snapshot<'a, S: StorageBackend> {
    builder: FlatBufferBuilder<'a>,
    storage: S,
    options: SegmentOptions,
    first_block_number: u64,
    revision: u64,
    group_count: usize,
}

impl<'a, S> Snapshot<'a, S>
where
    S: StorageBackend,
{
    pub fn new(first_block_number: u64, storage: S, options: SegmentOptions) -> Self {
        Self {
            builder: FlatBufferBuilder::new(),
            storage,
            options,
            first_block_number,
            revision: 0,
            group_count: 0,
        }
    }

    pub async fn handle_segment_group_event(&mut self, event: SegmentGroupEvent) -> Result<()> {
        let _summary = match event {
            SegmentGroupEvent::None => {
                return Ok(());
            }
            SegmentGroupEvent::Flushed(summary) => summary,
        };

        self.group_count += 1;

        let mut snapshot = store::SnapshotBuilder::new(&mut self.builder);
        snapshot.add_revision(self.revision + 1);
        snapshot.add_first_block_number(self.first_block_number);
        snapshot.add_segment_size(self.options.segment_size as u32);
        snapshot.add_group_size(self.options.group_size as u32);
        snapshot.add_group_count(self.group_count as u32);
        let snapshot = snapshot.finish();

        self.builder.finish(snapshot, None);

        let data = self.builder.finished_data();
        info!(
            revision = self.revision + 1,
            snapshot_size = %FormattedSize(data.len()),
            "writing snapshot"
        );

        let mut writer = self.storage.put("", "snapshot").await?;
        writer
            .write_all(data)
            .await
            .change_context(DnaError::Io)
            .attach_printable("failed to write snapshot")?;
        writer.shutdown().await.change_context(DnaError::Io)?;

        self.revision += 1;
        self.builder.reset();

        Ok(())
    }

    pub fn group_count(&self) -> usize {
        self.group_count
    }
}
