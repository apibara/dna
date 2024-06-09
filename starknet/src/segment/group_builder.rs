use apibara_dna_common::{
    error::{DnaError, Result},
    storage::StorageBackend,
};
use error_stack::ResultExt;
use tokio::io::AsyncWriteExt;

use crate::segment::store;

use super::index::Index;

#[derive(Default)]
pub struct SegmentGroupBuilder {
    first_block_number: Option<u64>,
    segment_count: usize,
    index: Index,
}

impl SegmentGroupBuilder {
    pub fn reset(&mut self) {
        self.first_block_number = None;
        self.segment_count = 0;
        self.index = Index::default();
    }

    pub fn add_segment(&mut self, first_block_number: u64, index: Index) {
        if self.first_block_number.is_none() {
            self.first_block_number = Some(first_block_number);
        }
        self.segment_count += 1;
        self.index.merge(index);
    }

    pub async fn write<S: StorageBackend>(
        &mut self,
        group_name: &str,
        storage: &mut S,
    ) -> Result<()> {
        let index = store::Index::try_from(std::mem::take(&mut self.index))
            .change_context(DnaError::Fatal)
            .attach_printable("failed to convert segment group index")?;
        let segment_group = store::SegmentGroup { index };

        let mut writer = storage.put("group", group_name).await?;
        let bytes = rkyv::to_bytes::<_, 0>(&segment_group).change_context(DnaError::Io)?;
        writer
            .write_all(&bytes)
            .await
            .change_context(DnaError::Io)?;
        writer.shutdown().await.change_context(DnaError::Io)?;

        Ok(())
    }
}
