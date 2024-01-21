use apibara_dna_common::{
    error::{DnaError, Result},
    segment::{SegmentExt, SegmentOptions},
    storage::StorageBackend,
};
use error_stack::ResultExt;
use flatbuffers::FlatBufferBuilder;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tracing::info;

use crate::segment::{index::FlatBufferBuilderFieldElementBitmapExt, store};

use super::{builder::SegmentEvent, index::SegmentIndex};

/// A builder for a segment group.
///
/// A segment group is a collection of pointers to segments
/// and indices to quickly access data in them.
pub struct SegmentGroupBuilder<'a, S: StorageBackend> {
    storage: S,
    options: SegmentOptions,
    builder: FlatBufferBuilder<'a>,
    index: SegmentIndex,
    first_block_number: u64,
    segment_count: usize,
}

pub struct SegmentGroupSummary {
    pub first_block_number: u64,
}

pub enum SegmentGroupEvent {
    None,
    Flushed(SegmentGroupSummary),
}

impl<'a, S> SegmentGroupBuilder<'a, S>
where
    S: StorageBackend,
{
    pub fn new(storage: S, options: SegmentOptions) -> Self {
        Self {
            storage,
            options,
            index: SegmentIndex::default(),
            builder: FlatBufferBuilder::new(),
            first_block_number: 0,
            segment_count: 0,
        }
    }

    pub async fn handle_segment_event(&mut self, event: SegmentEvent) -> Result<SegmentGroupEvent> {
        let summary = match event {
            SegmentEvent::None => return Ok(SegmentGroupEvent::None),
            SegmentEvent::Flushed(summary) => summary,
        };

        assert_eq!(summary.size, self.options.segment_size);

        let segment_start = summary.first_block_number.segment_start(&self.options);
        info!(segment_start, "flushed segment");

        if self.segment_count == 0 {
            self.first_block_number = segment_start;
        }
        self.segment_count += 1;
        self.index.join(&summary.index);

        if self.segment_count < self.options.group_size {
            return Ok(SegmentGroupEvent::None);
        }

        let first_block_number = self.first_block_number;
        let group_name = first_block_number.format_segment_group_name(&self.options);

        let mut writer = self.storage.put("group", group_name).await?;
        self.write_segment_group(&mut writer).await?;

        let summary = SegmentGroupSummary { first_block_number };
        Ok(SegmentGroupEvent::Flushed(summary))
    }

    async fn write_segment_group<W: AsyncWrite + Unpin>(&mut self, writer: &mut W) -> Result<()> {
        info!("flushing segment group");

        let event_by_address = self
            .builder
            .create_field_element_bitmap(&self.index.event_by_address)?;

        let event_by_key = self
            .builder
            .create_field_element_bitmap(&self.index.event_by_key)?;

        let mut group = store::SegmentGroupBuilder::new(&mut self.builder);
        group.add_first_block_number(self.first_block_number);
        group.add_segment_size(self.options.segment_size as u32);
        group.add_segment_count(self.segment_count as u32);
        group.add_event_by_address(event_by_address);
        group.add_event_by_key(event_by_key);

        let group = group.finish();
        self.builder.finish(group, None);

        writer
            .write_all(self.builder.finished_data())
            .await
            .change_context(DnaError::Io)
            .attach_printable("failed to write segment group")?;
        writer.shutdown().await.change_context(DnaError::Io)?;

        self.segment_count = 0;
        self.first_block_number = 0;
        self.builder.reset();

        Ok(())
    }
}
