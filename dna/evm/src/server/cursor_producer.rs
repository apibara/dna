use std::sync::Arc;

use apibara_dna_common::{
    core::Cursor,
    error::{DnaError, Result},
    ingestion::{Snapshot, SnapshotChange},
    segment::SegmentOptions,
};
use error_stack::ResultExt;
use futures_util::{Stream, StreamExt};
use tokio::{
    pin,
    sync::{RwLock, RwLockReadGuard, RwLockWriteGuard},
};
use tokio_util::sync::CancellationToken;

pub struct CursorProducerService<C>
where
    C: Stream<Item = SnapshotChange> + Unpin + Send + Sync + 'static,
{
    snapshot_changes: C,
}

#[derive(Debug, Clone)]
pub enum BlockNumberOrCursor {
    Number(u64),
    Cursor(Cursor),
}

/// The next block that can be streamed.
#[derive(Debug, Clone)]
pub enum NextBlock {
    /// Stream an entire segment group.
    SegmentGroup(u64, SegmentOptions),
    /// Stream a single segment group.
    Segment(u64, SegmentOptions),
    /// Stream a single block.
    Block(Cursor),
    /// A chain reorganization invalidated the current state.
    Invalidate,
    /// The cursor producer is not ready yet.
    NotReady,
    /// Reached the head of the chain.
    HeadReached,
}

#[derive(Clone)]
pub struct CursorProducer {
    inner: SharedState,
}

struct CursorState {
    pub snapshot: Snapshot,
    pub finalized: Option<Cursor>,
    pub extra_cursors: Vec<Cursor>,
}

#[derive(Clone, Default)]
struct SharedState(Arc<RwLock<Option<CursorState>>>);

impl<C> CursorProducerService<C>
where
    C: Stream<Item = SnapshotChange> + Unpin + Send + Sync + 'static,
{
    pub fn new(snapshot_changes: C) -> Self {
        Self { snapshot_changes }
    }

    pub fn start(self, ct: CancellationToken) -> CursorProducer {
        let state = SharedState::default();

        tokio::spawn(update_shared_state_from_stream(
            state.clone(),
            self.snapshot_changes,
            ct,
        ));

        CursorProducer { inner: state }
    }
}

impl CursorState {
    pub fn most_recent_available_block(&self) -> BlockNumberOrCursor {
        if let Some(last_cursor) = self.extra_cursors.last() {
            return last_cursor.clone().into();
        }

        if let Some(finalized) = &self.finalized {
            return finalized.clone().into();
        }

        let starting = u64::max(self.snapshot.starting_block_number(), 1) - 1;
        starting.into()
    }
}

impl CursorProducer {
    pub async fn is_block_available(&self, block: &BlockNumberOrCursor) -> bool {
        let state = self.inner.read().await;
        let Some(cursor_state) = state.as_ref() else {
            return false;
        };

        // Remember that cursors are always relative to the block _before_
        // the first block that will be streamed.
        let block_number = match block {
            BlockNumberOrCursor::Number(number) => *number,
            BlockNumberOrCursor::Cursor(cursor) => cursor.number + 1,
        };

        if block_number < cursor_state.snapshot.ingestion.first_block_number {
            return false;
        }

        block_number < cursor_state.snapshot.starting_block_number()
    }

    pub async fn most_recent_available_block(&self) -> Option<BlockNumberOrCursor> {
        let state = self.inner.read().await;
        state.as_ref().map(|s| s.most_recent_available_block())
    }

    pub async fn is_block_finalized(&self, cursor: &Cursor) -> bool {
        let state = self.inner.read().await;
        state
            .as_ref()
            .and_then(|state| state.finalized.as_ref())
            .map(|finalized| finalized.number >= cursor.number)
            .unwrap_or(false)
    }

    pub async fn next_block(&self, current: &BlockNumberOrCursor) -> Result<NextBlock> {
        let state_guard = self.inner.read().await;

        let Some(state) = state_guard.as_ref() else {
            return Ok(NextBlock::NotReady);
        };

        let next_block_number = current.number() + 1;

        let segment_options = &state.snapshot.segment_options;

        if next_block_number < state.snapshot.ingestion.first_block_number {
            return Err(DnaError::Fatal)
                .attach_printable("requested block is before the first block");
        }

        if next_block_number < state.snapshot.first_non_grouped_block_number() {
            let next_starting_group = segment_options.segment_group_start(next_block_number);
            return Ok(NextBlock::SegmentGroup(
                next_starting_group,
                segment_options.clone(),
            ));
        }

        if next_block_number < state.snapshot.starting_block_number() {
            let next_segment_start = segment_options.segment_start(next_block_number);
            return Ok(NextBlock::Segment(
                next_segment_start,
                segment_options.clone(),
            ));
        }

        if let Some(next_cursor) = state
            .extra_cursors
            .iter()
            .find(|c| c.number == next_block_number)
        {
            Ok(NextBlock::Block(next_cursor.clone()))
        } else {
            Ok(NextBlock::HeadReached)
        }
    }

    pub async fn segment_options(&self) -> Option<SegmentOptions> {
        let state = self.inner.read().await;

        state.as_ref().map(|s| s.snapshot.segment_options.clone())
    }
}

async fn update_shared_state_from_stream(
    shared_state: SharedState,
    snapshot_changes: impl Stream<Item = SnapshotChange> + Unpin + Send + Sync + 'static,
    ct: CancellationToken,
) -> Result<()> {
    let snapshot_changes = snapshot_changes.fuse().take_until(ct.cancelled());
    pin!(snapshot_changes);

    while let Some(change) = snapshot_changes.next().await {
        match change {
            SnapshotChange::Started(mut snapshot) => {
                let mut state = shared_state.write().await;
                // Reset state to the initial state.
                // This should almost never happen.
                snapshot.revision = 0;
                let new_state = CursorState {
                    snapshot,
                    finalized: None,
                    extra_cursors: vec![],
                };

                *state = Some(new_state);
            }
            SnapshotChange::StateChanged {
                new_state,
                finalized,
            } => {
                let mut state = shared_state.write().await;

                let latest_state = state
                    .as_mut()
                    .ok_or(DnaError::Fatal)
                    .attach_printable("received state changed but no snapshot started")?;

                latest_state.snapshot.ingestion = new_state;
                latest_state.finalized = Some(finalized);
            }
            SnapshotChange::BlockIngested(ingested) => {
                let mut state = shared_state.write().await;

                let latest_state = state
                    .as_mut()
                    .ok_or(DnaError::Fatal)
                    .attach_printable("received state changed but no snapshot started")?;

                latest_state.extra_cursors.push(ingested.cursor);
            }
        }
    }

    Ok(())
}

impl SharedState {
    async fn read(&self) -> RwLockReadGuard<'_, Option<CursorState>> {
        self.0.read().await
    }

    async fn write(&self) -> RwLockWriteGuard<'_, Option<CursorState>> {
        self.0.write().await
    }
}

impl From<u64> for BlockNumberOrCursor {
    fn from(value: u64) -> Self {
        BlockNumberOrCursor::Number(value)
    }
}

impl From<Cursor> for BlockNumberOrCursor {
    fn from(value: Cursor) -> Self {
        BlockNumberOrCursor::Cursor(value)
    }
}

impl BlockNumberOrCursor {
    pub fn number(&self) -> u64 {
        match self {
            BlockNumberOrCursor::Number(number) => *number,
            BlockNumberOrCursor::Cursor(cursor) => cursor.number,
        }
    }
}
