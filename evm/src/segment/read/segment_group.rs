use std::io::Cursor;

use apibara_dna_common::{
    error::{DnaError, Result},
    segment::SegmentOptions,
    storage::StorageBackend,
};
use error_stack::ResultExt;

use crate::segment::store;

pub struct SegmentGroupReader<S: StorageBackend> {
    storage: S,
    segment_options: SegmentOptions,
    buffer: Vec<u8>,
}

impl<S> SegmentGroupReader<S>
where
    S: StorageBackend,
    <S as StorageBackend>::Reader: Unpin,
{
    pub fn new(storage: S, segment_options: SegmentOptions, buffer_size: usize) -> Self {
        let buffer = vec![0; buffer_size];
        Self {
            storage,
            segment_options,
            buffer,
        }
    }

    pub async fn read(&mut self, segment_group_start: u64) -> Result<store::SegmentGroup<'_>> {
        let segment_group_name = self
            .segment_options
            .format_segment_group_name(segment_group_start);

        let mut reader = self.storage.get("group", &segment_group_name).await?;

        let len = {
            let mut cursor = Cursor::new(&mut self.buffer[..]);
            tokio::io::copy(&mut reader, &mut cursor)
                .await
                .change_context(DnaError::Io)? as usize
        };

        let group = flatbuffers::root::<store::SegmentGroup>(&self.buffer[..len])
            .change_context(DnaError::Fatal)
            .attach_printable_lazy(|| {
                format!("failed to read segment group {}", segment_group_name)
            })?;

        Ok(group)
    }
}
