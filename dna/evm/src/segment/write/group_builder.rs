use apibara_dna_common::{
    error::{DnaError, Result},
    storage::StorageBackend,
};
use error_stack::ResultExt;
use flatbuffers::FlatBufferBuilder;
use tokio::io::AsyncWriteExt;

use crate::segment::store;

use super::{index::FlatBufferBuilderSegmentIndexExt, SegmentIndex};

pub struct SegmentGroupBuilder<'a> {
    builder: FlatBufferBuilder<'a>,
    first_block_number: Option<u64>,
    index: SegmentIndex,
    segment_count: usize,
}

impl<'a> Default for SegmentGroupBuilder<'a> {
    fn default() -> Self {
        Self {
            builder: FlatBufferBuilder::new(),
            first_block_number: None,
            segment_count: 0,
            index: SegmentIndex::default(),
        }
    }
}

impl<'a> SegmentGroupBuilder<'a> {
    pub fn add_segment(&mut self, first_block_number: u64, _count: usize) {
        if self.first_block_number.is_none() {
            self.first_block_number = Some(first_block_number);
        }
        self.segment_count += 1;
    }

    pub fn add_index(&mut self, other: &SegmentIndex) {
        self.index.join(other);
    }

    pub async fn write<S: StorageBackend>(
        &mut self,
        group_name: &str,
        storage: &mut S,
    ) -> Result<()> {
        let log_by_address = self
            .builder
            .create_address_bitmap(&self.index.log_by_address)?;
        let log_by_topic = self.builder.create_topic_bitmap(&self.index.log_by_topic)?;

        let mut group = store::SegmentGroupBuilder::new(&mut self.builder);

        if let Some(first_block_number) = self.first_block_number {
            group.add_first_block_number(first_block_number);
        }
        group.add_log_by_address(log_by_address);
        group.add_log_by_topic(log_by_topic);

        let group = group.finish();
        self.builder.finish(group, None);

        let mut writer = storage.put("group", group_name).await?;
        writer
            .write_all(self.builder.finished_data())
            .await
            .change_context(DnaError::Io)?;
        writer.shutdown().await.change_context(DnaError::Io)?;
        Ok(())
    }

    pub fn reset(&mut self) {
        self.index = SegmentIndex::default();
        self.segment_count = 0;
        self.first_block_number = None;
        self.builder.reset();
    }
}
