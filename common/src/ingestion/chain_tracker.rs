use std::collections::{BTreeMap, VecDeque};

use async_trait::async_trait;
use error_stack::{Result, ResultExt};
use futures_util::Stream;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn};

use crate::core::Cursor;

use super::snapshot_manager::SnapshotReader;

#[derive(Debug, Clone)]
pub enum ChainChange {
    /// First message in the stream, with the starting state.
    Initialize { head: Cursor, finalized: Cursor },
    /// A new head has been detected.
    NewHead(Cursor),
    /// A new finalized block has been detected.
    NewFinalized(Cursor),
    /// The chain reorganized.
    Invalidate,
}

#[derive(Debug, Clone)]
pub enum ChainChangeV2_ChangeMe_Before_Release {
    /// First message in the stream, with the starting state.
    Initialize { head: Cursor, finalized: Cursor },
    /// A new head has been detected.
    NewHead(Cursor),
    /// A new finalized block has been detected.
    NewFinalized(Cursor),
    /// Ingest the provided block.
    Ingest(Cursor),
    /// The chain reorganized.
    Invalidate {
        new_head: Cursor,
        removed: Vec<Cursor>,
    },
}

#[async_trait]
pub trait CursorProvider {
    type Error: error_stack::Context;
    type CursorStream: Stream<Item = Cursor>;

    /// Subscribe to changes to the current head.
    async fn subscribe_head(&self) -> Result<Self::CursorStream, Self::Error>;

    /// Subscribe to changes to the current finalized block.
    async fn subscribe_finalized(&self) -> Result<Self::CursorStream, Self::Error>;

    /// Returns the cursor of the parent of the provided cursor.
    async fn get_parent_cursor(&self, cursor: &Cursor) -> Result<Cursor, Self::Error>;
}

#[derive(Debug)]
pub enum BlockIngestionDriverError {
    InvalidState,
    CursorProvider,
    Snapshot,
    StreamClosed,
}

#[derive(Debug)]
pub struct BlockIngestionDriverOptions {
    pub channel_size: usize,
}

/// This object is used to generate [Cursor] that need to be ingested.
pub struct BlockIngestionDriver<P, SR>
where
    P: CursorProvider + Send + Sync + 'static,
    SR: SnapshotReader + Send + Sync + 'static,
{
    options: BlockIngestionDriverOptions,
    cursor_provider: P,
    snapshot_reader: SR,
    previous: Option<Cursor>,
    queued_messages: VecDeque<ChainChangeV2_ChangeMe_Before_Release>,
    head: Cursor,
    finalized: Cursor,
    canonical: BTreeMap<u64, Cursor>,
}

impl<P, SR> BlockIngestionDriver<P, SR>
where
    P: CursorProvider + Send + Sync + 'static,
    P::CursorStream: Send + Unpin,
    SR: SnapshotReader + Send + Sync + 'static,
{
    pub fn new(
        cursor_provider: P,
        snapshot_reader: SR,
        options: BlockIngestionDriverOptions,
    ) -> Self {
        Self {
            options,
            cursor_provider,
            snapshot_reader,
            previous: None,
            queued_messages: VecDeque::default(),
            head: Cursor::new_finalized(0),
            finalized: Cursor::new_finalized(0),
            canonical: BTreeMap::new(),
        }
    }

    pub fn start(
        self,
        ct: CancellationToken,
    ) -> (
        ReceiverStream<ChainChangeV2_ChangeMe_Before_Release>,
        JoinHandle<Result<(), BlockIngestionDriverError>>,
    ) {
        let (tx, rx) = mpsc::channel(self.options.channel_size);
        let handle = tokio::spawn(self.do_loop(tx, ct));
        (ReceiverStream::new(rx), handle)
    }

    async fn do_loop(
        mut self,
        tx: mpsc::Sender<ChainChangeV2_ChangeMe_Before_Release>,
        ct: CancellationToken,
    ) -> Result<(), BlockIngestionDriverError> {
        info!("starting block ingestion driver");

        let mut head_stream = self
            .cursor_provider
            .subscribe_head()
            .await
            .change_context(BlockIngestionDriverError::CursorProvider)
            .attach_printable("failed to subscribe to head changes")?;

        let mut finalized_stream = self
            .cursor_provider
            .subscribe_finalized()
            .await
            .change_context(BlockIngestionDriverError::CursorProvider)
            .attach_printable("failed to subscribe to finalized changes")?;

        self.head = tokio::select! {
            head = head_stream.next() => head,
            _ = ct.cancelled() => return Ok(()),
        }
        .ok_or(BlockIngestionDriverError::CursorProvider)
        .attach_printable("head stream closed")?;

        self.finalized = tokio::select! {
            finalized = finalized_stream.next() => finalized,
            _ = ct.cancelled() => return Ok(()),
        }
        .ok_or(BlockIngestionDriverError::CursorProvider)
        .attach_printable("finalized stream closed")?;

        info!(head = %self.head, finalized = %self.finalized, "received initial head and finalized blocks");

        let starting_snapshot = self
            .snapshot_reader
            .read()
            .await
            .change_context(BlockIngestionDriverError::Snapshot)
            .attach_printable("failed to read snapshot")?;

        debug!(?starting_snapshot, "read starting snapshot");

        self.previous = if let Some(snapshot) = starting_snapshot {
            let ingestion = &snapshot.ingestion;
            let first_block_to_ingest = ingestion.first_block_number
                + (ingestion.group_count as u64 * snapshot.segment_options.segment_group_blocks());
            // Notice that we track the last ingested block.
            // Also notice that we check that group_count > 0 above.
            Some(Cursor::new_finalized(first_block_to_ingest - 1))
        } else {
            None
        };

        self.initialize_canonical_chain().await?;

        let permit = tx
            .reserve()
            .await
            .change_context(BlockIngestionDriverError::StreamClosed)?;
        permit.send(ChainChangeV2_ChangeMe_Before_Release::Initialize {
            head: self.head.clone(),
            finalized: self.finalized.clone(),
        });

        loop {
            self.check_head_invariant()?;

            tokio::select! {
                // Give higher priority to cursor changes to avoid sending cursors that will
                // be invalidated immediately after.
                biased;
                _ = ct.cancelled() => break,

                Some(head) = head_stream.next() => {
                    self.update_head(head).await?;
                }
                Some(finalized) = finalized_stream.next() => {
                    self.update_finalized(finalized)?;
                }
                permit = tx.reserve(), if self.has_something_to_send() => {
                    let permit = permit.change_context(BlockIngestionDriverError::StreamClosed)?;
                    if let Some(message) = self.queued_messages.pop_front() {
                        permit.send(message);
                    } else {
                        self.send_next_cursor(permit)?;
                    }
                }
                else => {
                    return Err(BlockIngestionDriverError::CursorProvider)
                        .attach_printable("head or finalized stream closed");
                }
            }
        }

        Ok(())
    }

    fn check_head_invariant(&self) -> Result<(), BlockIngestionDriverError> {
        if self.head.number < self.finalized.number {
            return Err(BlockIngestionDriverError::InvalidState)
                .attach_printable("head is behind finalized");
        }

        Ok(())
    }

    fn has_something_to_send(&self) -> bool {
        self.has_cursor_to_send() || self.has_queued_message_to_send()
    }

    fn has_cursor_to_send(&self) -> bool {
        self.previous
            .as_ref()
            .map(|c| c.number < self.head.number)
            .unwrap_or(true)
    }

    fn has_queued_message_to_send(&self) -> bool {
        !self.queued_messages.is_empty()
    }

    #[tracing::instrument(skip(self), err(Debug))]
    async fn update_head(&mut self, new_head: Cursor) -> Result<(), BlockIngestionDriverError> {
        trace!(?new_head, "updating head");

        // Check if the head appends cleanly to the previous one
        // This is the most common case and it doesn't require any special handling.
        let new_head_parent = self
            .cursor_provider
            .get_parent_cursor(&new_head)
            .await
            .change_context(BlockIngestionDriverError::CursorProvider)
            .attach_printable("failed to get parent cursor")?;

        trace!(?new_head_parent, "new head parent");

        if new_head_parent == self.head {
            trace!("clean chain append");
            self.canonical.insert(new_head.number, new_head.clone());
            self.head = new_head;
            self.queued_messages
                .push_back(ChainChangeV2_ChangeMe_Before_Release::NewHead(
                    self.head.clone(),
                ));

            return Ok(());
        }

        // These cursors have been invalidated.
        let mut invalidated_cursors = Vec::new();

        // Check that the new head is not behind the current head.
        // If that happens, we need to reorganize the chain. This can happen on
        // chains like Starknet with a centralized sequencer.
        //
        // We handle this case by shrinking the old chain until we reach the same
        // height as the new chain. Then we can handle it like any other reorg.
        //
        // TESTING STRATEGY (with Anvil):
        //
        //      /--o---Z
        // o---X---o---o---Y
        //     ^ s(0x0)
        //
        // - Start anvil with fast (1s) block time.
        //     Fast block times are needed to produce blocks in between polls.
        // - Start a cursor provider with slow polling rate.
        // - Wait for block X and take snapshot 0x0.
        // - Wait for block Y > X. Wait time: 3 * poll_rate.
        // - Restore snapshot 0x0.
        // - Wait for block X < Z < Y. Check the reorg is detected.
        if new_head.number <= self.head.number {
            trace!(?new_head, ?self.head, "shrinking invalidated chain");
            let mut number = self.head.number;
            while number >= new_head.number {
                let Some(cursor) = self.canonical.remove(&number) else {
                    return Err(BlockIngestionDriverError::InvalidState)
                        .attach_printable("missing block in canonical chain")
                        .attach_printable_lazy(|| format!("block number: {number}"))?;
                };
                invalidated_cursors.push(cursor);
                number -= 1;
            }
        }

        // Walk backwards from the new head until we find a block that belongs to
        // the canonical chain.
        //
        // - Case 1: if the block is the current head, we can just update the head.
        // - Case 2: if the block is not the current head, we have a reorganization.
        //
        // CASE 1 - TESTING STRATEGY (with Anvil):
        //
        // - Start anvil with fast (1s) block time.
        //     Fast block times are needed to produce blocks in between polls.
        // - Start a cursor provider with slow polling rate.
        // - Cursors should be produced in "bursts", but without any reorg.
        //
        // CASE 2 - TESTING STRATEGY (with Anvil):
        //
        //      /--o---o---o---o---o---Z
        // o---X---o---o---Y
        //     ^ s(0x0)
        //
        // - Start anvil with fast (1s) block time.
        //     Fast block times are needed to produce blocks in between polls.
        // - Start a cursor provider with slow polling rate.
        // - Wait for block X and take snapshot 0x0.
        // - Wait for block Y to be detected.
        // - Quickly restore snapshot 0x0 and mine a longer chain (e.g. `anvil_mine([0x20])`).
        // - Wait for block Z to be detected. Check the reorg is detected.

        let mut current = new_head.clone();
        // These cursors are not invalidated yet. That depends whether we had a reorg or not.
        let mut inspected_cursors = Vec::new();

        let common_ancestor = loop {
            if current.number <= self.finalized.number {
                return Err(BlockIngestionDriverError::InvalidState)
                    .attach_printable("reorg is behind finalized")
                    .attach_printable_lazy(|| format!("finalized: {}", self.finalized))
                    .attach_printable_lazy(|| format!("new head: {}", new_head))
                    .attach_printable_lazy(|| format!("head: {}", self.head))?;
            }

            let parent = self
                .cursor_provider
                .get_parent_cursor(&current)
                .await
                .change_context(BlockIngestionDriverError::CursorProvider)
                .attach_printable("failed to get parent cursor")?;

            trace!(?current, ?parent, "walking chain back");

            inspected_cursors.push(current.clone());

            if let Some(canonical_parent) = self.canonical.get(&parent.number) {
                if canonical_parent == &parent {
                    break parent;
                } else {
                    // Cursor will exist since we checked it before.
                    let cursor = self
                        .canonical
                        .remove(&parent.number)
                        .expect("canonical cursor");
                    invalidated_cursors.push(cursor);
                }
            }

            current = parent;
        };

        // No cursor has been invalidated. It means that the new head belongs to the same
        // chain and it was just too far ahead.
        if invalidated_cursors.is_empty() {
            for cursor in inspected_cursors.into_iter() {
                self.canonical.insert(cursor.number, cursor);
            }

            self.head = new_head;

            self.queued_messages
                .push_back(ChainChangeV2_ChangeMe_Before_Release::NewHead(
                    self.head.clone(),
                ));

            return Ok(());
        }

        for cursor in inspected_cursors.into_iter() {
            // All cursors should have been removed.
            if self.canonical.insert(cursor.number, cursor).is_some() {
                return Err(BlockIngestionDriverError::InvalidState)
                    .attach_printable("cursor already in canonical chain");
            }
        }

        // Update head.
        self.head = new_head;

        // If we pushed any cursor in the old canonical chain, we need to adjust
        // the previous cursor to the new common ancestor so that the downstream
        // components can continue from there.
        if let Some(previous) = &self.previous {
            if previous.number > common_ancestor.number {
                // Warn downstream components of the reorganization.
                // Only do this if we send any cursor that has been invalidated.
                let removed_cursors = invalidated_cursors
                    .into_iter()
                    .filter(|c| c.number <= previous.number)
                    .rev()
                    .collect();

                self.previous = Some(common_ancestor.clone());

                self.queued_messages
                    .push_back(ChainChangeV2_ChangeMe_Before_Release::Invalidate {
                        new_head: common_ancestor.clone(),
                        removed: removed_cursors,
                    });
            }
        }

        // Send them the new chain head.
        self.queued_messages
            .push_back(ChainChangeV2_ChangeMe_Before_Release::NewHead(
                self.head.clone(),
            ));

        Ok(())
    }

    #[tracing::instrument(skip(self), err(Debug))]
    fn update_finalized(&mut self, finalized: Cursor) -> Result<(), BlockIngestionDriverError> {
        if finalized.number < self.finalized.number {
            return Err(BlockIngestionDriverError::InvalidState)
                .attach_printable("finalized is behind previous finalized")?;
        }

        // Remove cursors that are not needed anymore.
        self.canonical.retain(|n, _| n >= &finalized.number);
        self.finalized = finalized;

        self.queued_messages
            .push_back(ChainChangeV2_ChangeMe_Before_Release::NewFinalized(
                self.finalized.clone(),
            ));

        Ok(())
    }

    fn send_next_cursor(
        &mut self,
        tx: mpsc::Permit<'_, ChainChangeV2_ChangeMe_Before_Release>,
    ) -> Result<(), BlockIngestionDriverError> {
        let cursor_to_send = {
            if let Some(previous) = &self.previous {
                if previous.number < self.finalized.number {
                    Cursor::new_finalized(previous.number + 1)
                } else if previous.number < self.head.number {
                    let next_number = previous.number + 1;
                    self.canonical
                        .get(&next_number)
                        .cloned()
                        .ok_or(BlockIngestionDriverError::InvalidState)
                        .attach_printable("missing block in canonical chain")
                        .attach_printable_lazy(|| format!("block number: {next_number}"))?
                } else {
                    // This should have not happened.
                    warn!("Inside send_next_cursor with nothing to do.");
                    return Ok(());
                }
            } else {
                Cursor::new_finalized(0)
            }
        };

        tx.send(ChainChangeV2_ChangeMe_Before_Release::Ingest(
            cursor_to_send.clone(),
        ));

        self.previous = Some(cursor_to_send);

        Ok(())
    }

    #[tracing::instrument(skip_all, err(Debug))]
    async fn initialize_canonical_chain(&mut self) -> Result<(), BlockIngestionDriverError> {
        let mut current = self.head.clone();

        self.canonical.insert(current.number, current.clone());

        while current.number > self.finalized.number {
            let parent = self
                .cursor_provider
                .get_parent_cursor(&current)
                .await
                .change_context(BlockIngestionDriverError::CursorProvider)
                .attach_printable("failed to get parent cursor")?;

            self.canonical.insert(parent.number, parent.clone());
            current = parent;
        }

        Ok(())
    }
}

impl ChainChangeV2_ChangeMe_Before_Release {
    pub fn as_new_head(&self) -> Option<&Cursor> {
        match self {
            ChainChangeV2_ChangeMe_Before_Release::NewHead(cursor) => Some(cursor),
            _ => None,
        }
    }

    pub fn as_new_finalized(&self) -> Option<&Cursor> {
        match self {
            ChainChangeV2_ChangeMe_Before_Release::NewFinalized(cursor) => Some(cursor),
            _ => None,
        }
    }

    pub fn as_ingest(&self) -> Option<&Cursor> {
        match self {
            ChainChangeV2_ChangeMe_Before_Release::Ingest(cursor) => Some(cursor),
            _ => None,
        }
    }

    pub fn as_invalidated(&self) -> Option<(&Cursor, &[Cursor])> {
        match self {
            ChainChangeV2_ChangeMe_Before_Release::Invalidate { new_head, removed } => {
                Some((new_head, removed))
            }
            _ => None,
        }
    }
}

impl Default for BlockIngestionDriverOptions {
    fn default() -> Self {
        Self { channel_size: 1024 }
    }
}

impl error_stack::Context for BlockIngestionDriverError {}

impl std::fmt::Display for BlockIngestionDriverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use BlockIngestionDriverError::*;

        match self {
            InvalidState => write!(f, "Invalid internal state. This is a bug."),
            CursorProvider => write!(f, "Cursor provider error"),
            Snapshot => write!(f, "Snapshot error"),
            StreamClosed => write!(f, "Output stream closed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use tokio::sync::{mpsc, Mutex};
    use tokio_stream::wrappers::ReceiverStream;

    use crate::{
        core::testing::new_test_cursor,
        ingestion::{IngestionState, Snapshot, SnapshotError},
        segment::SegmentOptions,
    };

    use super::*;

    struct CursorProviderTx {
        pub parent: Arc<Mutex<HashMap<Cursor, Cursor>>>,
        pub head_tx: mpsc::Sender<Cursor>,
        pub finalized_tx: mpsc::Sender<Cursor>,
    }

    struct TestCursorProvider {
        pub parent: Arc<Mutex<HashMap<Cursor, Cursor>>>,
        pub head_rx: Mutex<Option<mpsc::Receiver<Cursor>>>,
        pub finalized_rx: Mutex<Option<mpsc::Receiver<Cursor>>>,
    }

    struct TestSnapshotReader {
        snapshot: Snapshot,
    }

    #[derive(Debug)]
    struct TestError;

    ///       Segment:  1_000 blocks.
    /// Segment group: 10_000 blocks.
    const SEGMENT_OPTIONS: SegmentOptions = SegmentOptions {
        segment_size: 1_000,
        group_size: 10,
    };

    /// Snapshot:
    /// - Revision: 0 (not important).
    /// - Segment options: as above.
    /// - State:
    ///   + First block number: 1_000 (spicy things up).
    ///   + Group count: 7 (70_000 blocks in total).
    ///   + Extra segment count: 9 (9_000 additional blocks).
    fn new_snapshot_reader() -> TestSnapshotReader {
        TestSnapshotReader {
            snapshot: Snapshot {
                revision: 0,
                segment_options: SEGMENT_OPTIONS,
                ingestion: IngestionState {
                    first_block_number: 1_000,
                    group_count: 7,
                    extra_segment_count: 9,
                },
            },
        }
    }

    fn new_cursor_provider() -> (TestCursorProvider, CursorProviderTx) {
        let (head_tx, head_rx) = mpsc::channel(10);
        let (finalized_tx, finalized_rx) = mpsc::channel(10);

        let parent = Arc::new(Mutex::new(HashMap::new()));

        let provider = TestCursorProvider {
            parent: parent.clone(),
            finalized_rx: Mutex::new(finalized_rx.into()),
            head_rx: Mutex::new(head_rx.into()),
        };

        let tx = CursorProviderTx {
            parent,
            head_tx,
            finalized_tx,
        };

        (provider, tx)
    }

    /// Test that the first cursor produced is Cursor(71_000).
    ///
    /// For this test, the cursor provider will return blocks that are far ahead of the snapshot.
    #[tokio::test]
    async fn test_starts_at_beginning_of_segment_group() {
        let snapshot_reader = new_snapshot_reader();
        let (cursor_provider, cursor_provider_tx) = new_cursor_provider();
        let ct = CancellationToken::default();

        for i in 0..10 {
            cursor_provider.parent.lock().await.insert(
                new_test_cursor(1_000_000 + i + 1, 0),
                new_test_cursor(1_000_000 + i, 0),
            );
        }

        cursor_provider_tx
            .head_tx
            .send(new_test_cursor(1_000_010, 0))
            .await
            .unwrap();
        cursor_provider_tx
            .finalized_tx
            .send(new_test_cursor(1_000_000, 0))
            .await
            .unwrap();

        let driver =
            BlockIngestionDriver::new(cursor_provider, snapshot_reader, Default::default());

        let (mut cursor_stream, handle) = driver.start(ct.clone());

        let initialize = cursor_stream.next().await.unwrap();
        assert!(matches!(
            initialize,
            ChainChangeV2_ChangeMe_Before_Release::Initialize { .. }
        ));

        for i in 0..1000 {
            let cursor = cursor_stream.next().await.unwrap();
            let cursor = cursor.as_ingest().unwrap();
            assert_eq!(*cursor, Cursor::new_finalized(71_000 + i));
        }

        ct.cancel();
        handle.await.unwrap().unwrap();
    }

    /// Test that the cursors produced contain the block hash if the cursor is not finalized.
    #[tokio::test]
    async fn test_cursors_include_hash_for_non_finalized_cursors() {
        let snapshot_reader = new_snapshot_reader();
        let (cursor_provider, cursor_provider_tx) = new_cursor_provider();
        let ct = CancellationToken::default();

        for i in 0..50 {
            cursor_provider.parent.lock().await.insert(
                new_test_cursor(71_050 + i + 1, 0),
                new_test_cursor(71_050 + i, 0),
            );
        }

        cursor_provider_tx
            .head_tx
            .send(new_test_cursor(71_100, 0))
            .await
            .unwrap();
        cursor_provider_tx
            .finalized_tx
            .send(new_test_cursor(71_050, 0))
            .await
            .unwrap();

        let driver =
            BlockIngestionDriver::new(cursor_provider, snapshot_reader, Default::default());

        let (mut cursor_stream, handle) = driver.start(ct.clone());

        let initialize = cursor_stream.next().await.unwrap();
        assert!(matches!(
            initialize,
            ChainChangeV2_ChangeMe_Before_Release::Initialize { .. }
        ));

        // These cursors are finalized.
        for i in 0..=50 {
            let cursor = cursor_stream.next().await.unwrap();
            let cursor = cursor.as_ingest().unwrap();
            assert_eq!(*cursor, Cursor::new_finalized(71_000 + i));
        }

        // Here they are not and so they include the hash.
        for i in 1..=50 {
            let cursor = cursor_stream.next().await.unwrap();
            let cursor = cursor.as_ingest().unwrap();
            assert_eq!(cursor.number, 71_050 + i);
            assert!(!cursor.hash.is_empty());
        }

        ct.cancel();
        handle.await.unwrap().unwrap();
    }

    /// Test that the internal state is updated correctly when the cursors change.
    #[tokio::test]
    async fn test_state_is_updated() {
        let snapshot_reader = new_snapshot_reader();
        let (cursor_provider, cursor_provider_tx) = new_cursor_provider();
        let ct = CancellationToken::default();

        for i in 0..100 {
            cursor_provider.parent.lock().await.insert(
                new_test_cursor(71_050 + i + 1, 0),
                new_test_cursor(71_050 + i, 0),
            );
        }

        cursor_provider_tx
            .head_tx
            .send(new_test_cursor(71_100, 0))
            .await
            .unwrap();
        cursor_provider_tx
            .finalized_tx
            .send(new_test_cursor(71_050, 0))
            .await
            .unwrap();

        // Change the channel size to 1 to control when messages are sent.
        let driver = BlockIngestionDriver::new(
            cursor_provider,
            snapshot_reader,
            BlockIngestionDriverOptions { channel_size: 1 },
        );

        let (mut cursor_stream, handle) = driver.start(ct.clone());

        let initialize = cursor_stream.next().await.unwrap();
        assert!(matches!(
            initialize,
            ChainChangeV2_ChangeMe_Before_Release::Initialize { .. }
        ));

        // These cursors are finalized.
        for i in 0..=50 {
            let cursor = cursor_stream.next().await.unwrap();
            let cursor = cursor.as_ingest().unwrap();
            assert_eq!(*cursor, Cursor::new_finalized(71_000 + i));
        }

        // Here they are not and so they include the hash.
        for i in 1..=5 {
            let cursor = cursor_stream.next().await.unwrap();
            let cursor = cursor.as_ingest().unwrap();
            assert_eq!(cursor.number, 71_050 + i);
            assert!(!cursor.hash.is_empty());
        }

        // Now the head moves forward by 10 blocks.
        cursor_provider_tx
            .head_tx
            .send(new_test_cursor(71_110, 0))
            .await
            .unwrap();

        let message = cursor_stream.next().await.unwrap();
        let message = message.as_new_head().unwrap();
        assert_eq!(message.number, 71_110);

        // Since the finalized block is still the same, we still have hashes.
        for i in 1..=5 {
            let cursor = cursor_stream.next().await.unwrap();
            let cursor = cursor.as_ingest().unwrap();
            assert_eq!(cursor.number, 71_055 + i);
            assert!(!cursor.hash.is_empty());
        }

        // Now the finalized cursor moves
        cursor_provider_tx
            .finalized_tx
            .send(new_test_cursor(71_100, 0))
            .await
            .unwrap();

        let message = cursor_stream.next().await.unwrap();
        let message = message.as_new_finalized().unwrap();
        assert_eq!(message.number, 71_100);

        // These cursors are now finalized
        for i in 1..=40 {
            let cursor = cursor_stream.next().await.unwrap();
            let cursor = cursor.as_ingest().unwrap();
            assert_eq!(*cursor, Cursor::new_finalized(71_060 + i));
        }

        ct.cancel();
        handle.await.unwrap().unwrap();
    }

    /// Test a chain reorganization that doesn't involve an ingested block.
    ///
    /// The new chain is SHORTER.
    ///
    /// The stream SHOULD NOT include a reorg message.
    #[tokio::test]
    async fn test_shrinking_reorg_after_current_cursor() {
        let snapshot_reader = new_snapshot_reader();
        let (cursor_provider, cursor_provider_tx) = new_cursor_provider();
        let ct = CancellationToken::default();

        for i in 0..10 {
            cursor_provider.parent.lock().await.insert(
                new_test_cursor(1_000_000 + i + 1, 0),
                new_test_cursor(1_000_000 + i, 0),
            );
        }

        cursor_provider_tx
            .head_tx
            .send(new_test_cursor(1_000_010, 0))
            .await
            .unwrap();
        cursor_provider_tx
            .finalized_tx
            .send(new_test_cursor(1_000_000, 0))
            .await
            .unwrap();

        let driver = BlockIngestionDriver::new(
            cursor_provider,
            snapshot_reader,
            BlockIngestionDriverOptions { channel_size: 1 },
        );

        let (mut cursor_stream, handle) = driver.start(ct.clone());

        let initialize = cursor_stream.next().await.unwrap();
        assert!(matches!(
            initialize,
            ChainChangeV2_ChangeMe_Before_Release::Initialize { .. }
        ));

        for i in 0..1000 {
            let cursor = cursor_stream.next().await.unwrap();
            let cursor = cursor.as_ingest().unwrap();
            assert_eq!(*cursor, Cursor::new_finalized(71_000 + i));
        }

        cursor_provider_tx
            .parent
            .lock()
            .await
            .insert(new_test_cursor(1_000_001, 1), new_test_cursor(1_000_000, 0));

        // Now the head moves back.
        cursor_provider_tx
            .head_tx
            .send(new_test_cursor(1_000_001, 1))
            .await
            .unwrap();

        let message = cursor_stream.next().await.unwrap();
        let message = message.as_new_head().unwrap();
        assert_eq!(*message, new_test_cursor(1_000_001, 1));

        for i in 0..1000 {
            let cursor = cursor_stream.next().await.unwrap();
            let cursor = cursor.as_ingest().unwrap();
            assert_eq!(*cursor, Cursor::new_finalized(72_000 + i));
        }

        ct.cancel();
        handle.await.unwrap().unwrap();
    }

    /// Test a chain reorganization that does involve an ingested block.
    ///
    /// The new chain is SHORTER.
    ///
    /// The stream SHOULD include a reorg message.
    #[tokio::test]
    async fn test_shrinking_reorg_before_current_cursor() {
        let snapshot_reader = new_snapshot_reader();
        let (cursor_provider, cursor_provider_tx) = new_cursor_provider();
        let ct = CancellationToken::default();

        for i in 0..1000 {
            cursor_provider.parent.lock().await.insert(
                new_test_cursor(71_100 + i + 1, 0),
                new_test_cursor(71_100 + i, 0),
            );
        }

        cursor_provider_tx
            .head_tx
            .send(new_test_cursor(72_000, 0))
            .await
            .unwrap();
        cursor_provider_tx
            .finalized_tx
            .send(new_test_cursor(71_100, 0))
            .await
            .unwrap();

        let driver = BlockIngestionDriver::new(
            cursor_provider,
            snapshot_reader,
            BlockIngestionDriverOptions { channel_size: 1 },
        );

        let (mut cursor_stream, handle) = driver.start(ct.clone());

        let initialize = cursor_stream.next().await.unwrap();
        assert!(matches!(
            initialize,
            ChainChangeV2_ChangeMe_Before_Release::Initialize { .. }
        ));

        for i in 0..=100 {
            let cursor = cursor_stream.next().await.unwrap();
            let cursor = cursor.as_ingest().unwrap();
            assert_eq!(*cursor, Cursor::new_finalized(71_000 + i));
        }

        // Consume some non finalized cursors.
        for i in 1..=35 {
            let cursor = cursor_stream.next().await.unwrap();
            let cursor = cursor.as_ingest().unwrap();
            assert_eq!(*cursor, new_test_cursor(71_100 + i, 0));
        }

        // Simulate a reorg where the head moves to 71_130 then to 71_140.

        // Link chains at 71_129.
        cursor_provider_tx
            .parent
            .lock()
            .await
            .insert(new_test_cursor(71_130, 1), new_test_cursor(71_129, 0));

        for i in 71_130..71_140 {
            cursor_provider_tx
                .parent
                .lock()
                .await
                .insert(new_test_cursor(i + 1, 1), new_test_cursor(i, 1));
        }

        // Now the head moves back.
        cursor_provider_tx
            .head_tx
            .send(new_test_cursor(71_140, 1))
            .await
            .unwrap();

        let message = cursor_stream.next().await.unwrap();
        let (new_head, removed_cursors) = message.as_invalidated().unwrap();

        assert_eq!(*new_head, new_test_cursor(71_129, 0));
        assert_eq!(
            removed_cursors,
            &[
                new_test_cursor(71_130, 0),
                new_test_cursor(71_131, 0),
                new_test_cursor(71_132, 0),
                new_test_cursor(71_133, 0),
                new_test_cursor(71_134, 0),
                new_test_cursor(71_135, 0),
            ]
        );

        let message = cursor_stream.next().await.unwrap();
        let new_head = message.as_new_head().unwrap();
        assert_eq!(*new_head, new_test_cursor(71_140, 1));

        // Ingest the new chain.
        for i in 0..=10 {
            let cursor = cursor_stream.next().await.unwrap();
            let cursor = cursor.as_ingest().unwrap();
            assert_eq!(*cursor, new_test_cursor(71_130 + i, 1));
        }

        ct.cancel();
        handle.await.unwrap().unwrap();
    }

    /// Test a chain reorganization that doesn't involve an ingested block.
    ///
    /// The new chain is LONGER.
    ///
    /// The stream SHOULD NOT include a reorg message.
    #[tokio::test]
    async fn test_reorg_before_current_cursor() {
        let snapshot_reader = new_snapshot_reader();
        let (cursor_provider, cursor_provider_tx) = new_cursor_provider();
        let ct = CancellationToken::default();

        for i in 0..10 {
            cursor_provider.parent.lock().await.insert(
                new_test_cursor(1_000_000 + i + 1, 0),
                new_test_cursor(1_000_000 + i, 0),
            );
        }

        cursor_provider_tx
            .head_tx
            .send(new_test_cursor(1_000_010, 0))
            .await
            .unwrap();
        cursor_provider_tx
            .finalized_tx
            .send(new_test_cursor(1_000_000, 0))
            .await
            .unwrap();

        let driver = BlockIngestionDriver::new(
            cursor_provider,
            snapshot_reader,
            BlockIngestionDriverOptions { channel_size: 1 },
        );

        let (mut cursor_stream, handle) = driver.start(ct.clone());

        let initialize = cursor_stream.next().await.unwrap();
        assert!(matches!(
            initialize,
            ChainChangeV2_ChangeMe_Before_Release::Initialize { .. }
        ));

        for i in 0..1000 {
            let cursor = cursor_stream.next().await.unwrap();
            let cursor = cursor.as_ingest().unwrap();
            assert_eq!(*cursor, Cursor::new_finalized(71_000 + i));
        }

        // Link new chain with old chain at 1_000_001 -> 1_000_000.
        cursor_provider_tx
            .parent
            .lock()
            .await
            .insert(new_test_cursor(1_000_001, 1), new_test_cursor(1_000_000, 0));

        for i in 1_000_001..=1_000_100 {
            cursor_provider_tx
                .parent
                .lock()
                .await
                .insert(new_test_cursor(i + 1, 1), new_test_cursor(i, 1));
        }

        // Now the head moves forward by a lot.
        cursor_provider_tx
            .head_tx
            .send(new_test_cursor(1_000_100, 1))
            .await
            .unwrap();

        let message = cursor_stream.next().await.unwrap();
        let message = message.as_new_head().unwrap();
        assert_eq!(*message, new_test_cursor(1_000_100, 1));

        // Ingestion continues as usual.
        for i in 0..1000 {
            let cursor = cursor_stream.next().await.unwrap();
            let cursor = cursor.as_ingest().unwrap();
            assert_eq!(*cursor, Cursor::new_finalized(72_000 + i));
        }

        ct.cancel();
        handle.await.unwrap().unwrap();
    }

    /// Test a chain reorganization that does involve an ingested block.
    ///
    /// The new chain is LONGER.
    ///
    /// The stream SHOULD include a reorg message.
    #[tokio::test]
    async fn test_reorg_after_current_cursor() {
        let snapshot_reader = new_snapshot_reader();
        let (cursor_provider, cursor_provider_tx) = new_cursor_provider();
        let ct = CancellationToken::default();

        for i in 0..1000 {
            cursor_provider.parent.lock().await.insert(
                new_test_cursor(71_100 + i + 1, 0),
                new_test_cursor(71_100 + i, 0),
            );
        }

        cursor_provider_tx
            .head_tx
            .send(new_test_cursor(71_140, 0))
            .await
            .unwrap();
        cursor_provider_tx
            .finalized_tx
            .send(new_test_cursor(71_100, 0))
            .await
            .unwrap();

        let driver = BlockIngestionDriver::new(
            cursor_provider,
            snapshot_reader,
            BlockIngestionDriverOptions { channel_size: 1 },
        );

        let (mut cursor_stream, handle) = driver.start(ct.clone());

        let initialize = cursor_stream.next().await.unwrap();
        assert!(matches!(
            initialize,
            ChainChangeV2_ChangeMe_Before_Release::Initialize { .. }
        ));

        for i in 0..=100 {
            let cursor = cursor_stream.next().await.unwrap();
            let cursor = cursor.as_ingest().unwrap();
            assert_eq!(*cursor, Cursor::new_finalized(71_000 + i));
        }

        // Consume some non finalized cursors.
        for i in 1..=35 {
            let cursor = cursor_stream.next().await.unwrap();
            let cursor = cursor.as_ingest().unwrap();
            assert_eq!(*cursor, new_test_cursor(71_100 + i, 0));
        }

        // Simulate a reorg where the head moves to 71_130 then to 71_150.

        // Link chains at 71_129.
        cursor_provider_tx
            .parent
            .lock()
            .await
            .insert(new_test_cursor(71_130, 1), new_test_cursor(71_129, 0));

        for i in 71_130..71_150 {
            cursor_provider_tx
                .parent
                .lock()
                .await
                .insert(new_test_cursor(i + 1, 1), new_test_cursor(i, 1));
        }

        // Now the head moves forward to 71_150.
        cursor_provider_tx
            .head_tx
            .send(new_test_cursor(71_150, 1))
            .await
            .unwrap();

        let message = cursor_stream.next().await.unwrap();
        let (new_head, removed_cursors) = message.as_invalidated().unwrap();

        assert_eq!(*new_head, new_test_cursor(71_129, 0));
        assert_eq!(
            removed_cursors,
            &[
                new_test_cursor(71_130, 0),
                new_test_cursor(71_131, 0),
                new_test_cursor(71_132, 0),
                new_test_cursor(71_133, 0),
                new_test_cursor(71_134, 0),
                new_test_cursor(71_135, 0),
            ]
        );

        let message = cursor_stream.next().await.unwrap();
        let new_head = message.as_new_head().unwrap();
        assert_eq!(*new_head, new_test_cursor(71_150, 1));

        // Ingest the new chain.
        for i in 0..=20 {
            let cursor = cursor_stream.next().await.unwrap();
            let cursor = cursor.as_ingest().unwrap();
            assert_eq!(*cursor, new_test_cursor(71_130 + i, 1));
        }

        ct.cancel();
        handle.await.unwrap().unwrap();
    }

    impl error_stack::Context for TestError {}

    impl std::fmt::Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Test error")
        }
    }

    #[async_trait::async_trait]
    impl SnapshotReader for TestSnapshotReader {
        async fn read(&self) -> Result<Option<Snapshot>, SnapshotError> {
            Ok(self.snapshot.clone().into())
        }
    }

    #[async_trait::async_trait]
    impl CursorProvider for TestCursorProvider {
        type Error = TestError;
        type CursorStream = ReceiverStream<Cursor>;

        async fn subscribe_head(&self) -> Result<Self::CursorStream, Self::Error> {
            let mut head_rx = self.head_rx.lock().await;
            let head_rx = std::mem::replace(&mut *head_rx, None).expect("already subscribed");
            Ok(ReceiverStream::new(head_rx))
        }

        async fn subscribe_finalized(&self) -> Result<Self::CursorStream, Self::Error> {
            let mut finalized_rx = self.finalized_rx.lock().await;
            let finalized_rx =
                std::mem::replace(&mut *finalized_rx, None).expect("already subscribed");
            Ok(ReceiverStream::new(finalized_rx))
        }

        async fn get_parent_cursor(&self, cursor: &Cursor) -> Result<Cursor, Self::Error> {
            let parent = self.parent.lock().await;
            Ok(parent.get(cursor).cloned().expect("parent not found"))
        }
    }
}
