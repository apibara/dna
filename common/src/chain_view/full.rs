use error_stack::{Result, ResultExt};
use tracing::debug;

use crate::chain::CanonicalChainSegment;
use crate::chain_store::ChainStore;
use crate::Cursor;

use super::ChainViewError;

pub enum CanonicalCursor {
    BeforeAvailable(Cursor),
    AfterAvailable(Cursor),
    Canonical(Cursor),
}

pub struct FullCanonicalChain {
    store: ChainStore,
    pub(crate) starting_block: u64,
    chain_segment_size: usize,
    recent: CanonicalChainSegment,
}

impl FullCanonicalChain {
    pub async fn initialize(
        store: ChainStore,
        starting_block: u64,
        chain_segment_size: usize,
    ) -> Result<Self, ChainViewError> {
        let recent = store
            .get_recent(None)
            .await
            .change_context(ChainViewError)
            .attach_printable("failed to get recent canonical chain segment")?
            .ok_or(ChainViewError)
            .attach_printable("recent canonical chain segment not found")?;

        Ok(Self {
            store,
            starting_block,
            chain_segment_size,
            recent,
        })
    }

    pub async fn get_head(&self) -> Result<Cursor, ChainViewError> {
        Ok(self.recent.info.last_block.clone())
    }

    pub async fn get_canonical(
        &self,
        block_number: u64,
    ) -> Result<CanonicalCursor, ChainViewError> {
        if block_number > self.recent.info.last_block.number {
            return Ok(CanonicalCursor::AfterAvailable(
                self.recent.info.last_block.clone(),
            ));
        }

        if block_number < self.starting_block {
            let first_available = self.get_canonical_impl(self.starting_block).await?;
            return Ok(CanonicalCursor::BeforeAvailable(first_available));
        }

        let cursor = self.get_canonical_impl(block_number).await?;
        Ok(CanonicalCursor::Canonical(cursor))
    }

    pub async fn refresh_recent(&mut self) -> Result<(), ChainViewError> {
        debug!("refreshing recent canonical chain segment");

        let Ok(Some(recent)) = self.store.get_recent(None).await else {
            return Ok(());
        };

        self.recent = recent;

        Ok(())
    }

    pub async fn get_canonical_impl(&self, block_number: u64) -> Result<Cursor, ChainViewError> {
        // It's a recent block. No need to fetch the chain segment.
        if self.recent.info.first_block.number <= block_number {
            let cursor = self
                .recent
                .canonical(block_number)
                .change_context(ChainViewError)
                .attach_printable("failed to get canonical block for recent chain segment")?;
            return Ok(cursor);
        }

        let chain_segment_start =
            chain_segment_start(block_number, self.starting_block, self.chain_segment_size);

        let segment = self
            .store
            .get(chain_segment_start)
            .await
            .change_context(ChainViewError)
            .attach_printable("failed to get chain segment")?
            .ok_or(ChainViewError)
            .attach_printable("chain segment not found")?;

        let cursor = segment
            .canonical(block_number)
            .change_context(ChainViewError)
            .attach_printable("failed to get canonical block for recent chain segment")?;

        Ok(cursor)
    }
}

fn chain_segment_start(block_number: u64, starting_block: u64, chain_segment_size: usize) -> u64 {
    let chain_segment_size = chain_segment_size as u64;
    (block_number - starting_block) / chain_segment_size * chain_segment_size + starting_block
}
