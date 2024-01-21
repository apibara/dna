use apibara_dna_common::error::Result;
use apibara_dna_common::segment::{SegmentExt, SegmentOptions};
use apibara_dna_common::storage::StorageBackend;
use tracing::{debug, instrument};

use crate::ingestion::FullBlock;

pub use super::event::EventSegmentBuilder;
pub use super::header::BlockHeaderSegmentBuilder;

/// Turn a bunch of blocks into a segment.
pub struct SegmentBuilder<'a, S: StorageBackend> {
    storage: S,
    options: SegmentOptions,
    current_block_number: u64,
    header: BlockHeaderSegmentBuilder<'a>,
    event: EventSegmentBuilder<'a>,
}

pub struct SegmentSummary {
    pub first_block_number: u64,
    pub size: usize,
}

pub enum SegmentEvent {
    None,
    Flushed(SegmentSummary),
}

impl<'a, S> SegmentBuilder<'a, S>
where
    S: StorageBackend,
{
    /// Create a new segment builder.
    pub fn new(storage: S, options: SegmentOptions) -> Self {
        Self {
            storage,
            options,
            current_block_number: 0,
            header: BlockHeaderSegmentBuilder::new(),
            event: EventSegmentBuilder::new(),
        }
    }

    #[instrument(skip(self, block), err(Debug))]
    pub async fn ingest_block(
        &mut self,
        block_number: u64,
        block: FullBlock,
    ) -> Result<SegmentEvent> {
        self.current_block_number = block_number;

        let current_segment = self.current_block_number.segment_start(&self.options);

        self.header.add_block_header(&block.block)?;
        self.event.add_block_events(block_number, &block.receipts)?;

        let next_segment = (block_number + 1).segment_start(&self.options);

        if current_segment == next_segment {
            return Ok(SegmentEvent::None);
        }

        debug!(
            current_segment = current_segment,
            "segment boundary reached"
        );

        let segment_name = self.current_block_number.format_segment_name(&self.options);

        let mut headers_writer = self
            .storage
            .put(format!("segment/{segment_name}"), "headers")
            .await?;
        self.header.write_segment(&mut headers_writer).await?;

        let mut events_writer = self
            .storage
            .put(format!("segment/{segment_name}"), "events")
            .await?;
        self.event.write_segment(&mut events_writer).await?;

        let segment_size = 1 + self.current_block_number - current_segment;
        let summary = SegmentSummary {
            first_block_number: current_segment,
            size: segment_size as usize,
        };

        Ok(SegmentEvent::Flushed(summary))
    }
}
