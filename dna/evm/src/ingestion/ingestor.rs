use apibara_dna_common::{
    error::{DnaError, Result},
    segment::{SegmentOptions, SnapshotBuilder},
    storage::StorageBackend,
};
use error_stack::ResultExt;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::segment::{SegmentBuilder, SegmentGroupBuilder};

use super::{FinalizedBlockIngestor, IngestionEvent, IngestorOptions, RpcProvider};

pub struct Ingestor<S: StorageBackend + Send + Sync + 'static> {
    segment_options: SegmentOptions,
    provider: RpcProvider,
    storage: S,
    options: IngestorOptions,
}

impl<S> Ingestor<S>
where
    S: StorageBackend + Send + Sync + 'static,
    <S as StorageBackend>::Reader: Unpin,
{
    pub fn new(provider: RpcProvider, storage: S) -> Self {
        let segment_options = SegmentOptions::default();
        Self {
            segment_options,
            provider,
            storage,
            options: IngestorOptions::default(),
        }
    }

    pub fn with_segment_options(mut self, segment_options: SegmentOptions) -> Self {
        self.segment_options = segment_options;
        self
    }

    pub fn with_ingestor_options(mut self, options: IngestorOptions) -> Self {
        self.options = options;
        self
    }

    pub async fn start(
        mut self,
        starting_block_number_override: Option<u64>,
        ct: CancellationToken,
    ) -> Result<()> {
        let mut segment_builder = SegmentBuilder::new();
        let mut segment_group_builder = SegmentGroupBuilder::new();

        let mut snapshot_builder = SnapshotBuilder::from_storage(&mut self.storage)
            .await?
            .unwrap_or_else(|| {
                SnapshotBuilder::new(
                    starting_block_number_override.unwrap_or(0),
                    self.segment_options.clone(),
                )
            });

        let segment_options = snapshot_builder.state().segment_options.clone();

        // TODO: implement this by recomputing group size if the user overrides the starting block number.
        if starting_block_number_override.is_some() && snapshot_builder.state().group_count > 0 {
            return Err(DnaError::Configuration).attach_printable(
                "cannot override starting block number after ingestion has started",
            );
        }

        let starting_block_number = snapshot_builder.state().starting_block();

        info!(
            revision = snapshot_builder.state().revision,
            segment_options = ?segment_options,
            starting_block_number, "starting ingestion"
        );

        let mut ingestor =
            FinalizedBlockIngestor::new(self.provider, starting_block_number, self.options);

        let mut segment_size = 0;
        let mut group_size = 0;

        loop {
            if ct.is_cancelled() {
                return Ok(());
            }

            let max_blocks = 1;
            match ingestor
                .ingest_next_segment(&mut segment_builder, max_blocks)
                .await?
            {
                IngestionEvent::Completed {
                    last_ingested_block,
                } => {
                    info!(block_number = last_ingested_block, "finished ingestion");
                    break;
                }
                IngestionEvent::Segment {
                    count,
                    first_block_number,
                    last_block_number,
                } => {
                    segment_size += count;
                    info!(
                        first_block_number = first_block_number,
                        last_block_number = last_block_number,
                        count = count,
                        segment_size = segment_size,
                        "ingested segments"
                    );

                    segment_group_builder.add_segment(first_block_number, count);

                    if segment_size >= segment_options.segment_size {
                        let segment_name = segment_options.format_segment_name(last_block_number);
                        segment_builder
                            .write(&format!("segment/{segment_name}"), &mut self.storage)
                            .await?;
                        let index = segment_builder.take_index();
                        segment_group_builder.add_index(&index);

                        segment_size = 0;
                        segment_builder.reset();
                        group_size += 1;

                        info!(segment_name, "wrote segment");
                    }

                    if group_size >= segment_options.group_size {
                        let group_name =
                            segment_options.format_segment_group_name(last_block_number);
                        group_size = 0;
                        segment_group_builder
                            .write(&group_name, &mut self.storage)
                            .await?;
                        segment_group_builder.reset();
                        info!(group_name, "wrote group index");
                        let new_revision =
                            snapshot_builder.write_revision(&mut self.storage).await?;
                        snapshot_builder.reset();
                        info!(revision = new_revision, "wrote snapshot");
                    }
                }
            }
        }

        Ok(())
    }
}