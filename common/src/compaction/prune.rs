use error_stack::{Result, ResultExt};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::{
    block_store::BlockStoreWriter, chain_view::ChainView, ingestion::IngestionStateClient,
};

use super::{metrics::CompactionMetrics, CompactionError};

pub struct PruneService {
    segment_size: usize,
    chain_view: ChainView,
    block_store_writer: BlockStoreWriter,
    state_client: IngestionStateClient,
    metrics: CompactionMetrics,
}

impl PruneService {
    pub fn new(
        segment_size: usize,
        chain_view: ChainView,
        block_store_writer: BlockStoreWriter,
        state_client: IngestionStateClient,
        metrics: CompactionMetrics,
    ) -> Self {
        Self {
            segment_size,
            chain_view,
            block_store_writer,
            state_client,
            metrics,
        }
    }

    pub async fn start(mut self, ct: CancellationToken) -> Result<(), CompactionError> {
        loop {
            if ct.is_cancelled() {
                return Ok(());
            }

            info!("compaction: prune tick");

            self.prune_loop(&ct).await?;

            info!("compaction: pruned up to the grouped segment");

            let Some(_) = ct
                .run_until_cancelled(self.chain_view.segmented_changed())
                .await
            else {
                return Ok(());
            };
        }
    }

    async fn prune_loop(&mut self, ct: &CancellationToken) -> Result<(), CompactionError> {
        let Some(grouped) = self
            .state_client
            .get_grouped()
            .await
            .change_context(CompactionError)?
        else {
            debug!("no grouped block, skipping pruning");
            return Ok(());
        };

        let starting_block = if let Some(pruned_block) = self
            .state_client
            .get_pruned()
            .await
            .change_context(CompactionError)?
        {
            pruned_block
        } else {
            let genesis = self
                .chain_view
                .get_starting_cursor()
                .await
                .change_context(CompactionError)?;
            genesis.number
        };

        debug!(starting_block, grouped, "prune loop starting");

        let mut current_pruned_block = starting_block;

        while current_pruned_block < grouped {
            for _ in 0..self.segment_size {
                if ct.is_cancelled() {
                    return Ok(());
                }

                if current_pruned_block > grouped {
                    break;
                }

                debug!(block = current_pruned_block, "pruning block");

                self.block_store_writer
                    .delete_block_with_prefix(current_pruned_block)
                    .await
                    .change_context(CompactionError)
                    .attach_printable("failed to delete block")
                    .attach_printable_lazy(|| format!("block number: {current_pruned_block}"))?;

                current_pruned_block += 1;
            }

            let last_pruned_block = current_pruned_block - 1;

            debug!(block = last_pruned_block, "updating pruned block in state");

            self.state_client
                .put_pruned(last_pruned_block)
                .await
                .change_context(CompactionError)?;

            self.metrics.pruned.record(last_pruned_block, &[]);
        }

        Ok(())
    }
}
