use std::collections::{BTreeMap, VecDeque};

use async_trait::async_trait;
use error_stack::{Result, ResultExt};
use futures_util::Stream;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tokio_util::sync::CancellationToken;
use tracing::warn;

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

        let starting_snapshot = self
            .snapshot_reader
            .read()
            .await
            .change_context(BlockIngestionDriverError::Snapshot)
            .attach_printable("failed to read snapshot")?;

        self.previous = if starting_snapshot.ingestion.group_count == 0 {
            None
        } else {
            let ingestion = &starting_snapshot.ingestion;
            let first_block_to_ingest = ingestion.first_block_number
                + (ingestion.group_count as u64
                    * starting_snapshot.segment_options.segment_group_blocks());
            // Notice that we track the last ingested block.
            // Also notice that we check that group_count > 0 above.
            Some(Cursor::new_finalized(first_block_to_ingest - 1))
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

    async fn update_head(&mut self, head: Cursor) -> Result<(), BlockIngestionDriverError> {
        self.head = head;

        self.queued_messages
            .push_back(ChainChangeV2_ChangeMe_Before_Release::NewHead(
                self.head.clone(),
            ));

        Ok(())
    }

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
        write!(f, "Block ingestion driver error")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tokio::sync::{mpsc, Mutex};
    use tokio_stream::wrappers::ReceiverStream;

    use crate::{
        core::testing::new_test_cursor,
        ingestion::{IngestionState, Snapshot, SnapshotError},
        segment::SegmentOptions,
    };

    use super::*;

    struct CursorProviderTx {
        pub head_tx: mpsc::Sender<Cursor>,
        pub finalized_tx: mpsc::Sender<Cursor>,
    }

    struct TestCursorProvider {
        pub parent: HashMap<Cursor, Cursor>,
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

        let provider = TestCursorProvider {
            parent: HashMap::new(),
            finalized_rx: Mutex::new(finalized_rx.into()),
            head_rx: Mutex::new(head_rx.into()),
        };

        let tx = CursorProviderTx {
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
        let (mut cursor_provider, cursor_provider_tx) = new_cursor_provider();
        let ct = CancellationToken::default();

        for i in 0..10 {
            cursor_provider.parent.insert(
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
        let (mut cursor_provider, cursor_provider_tx) = new_cursor_provider();
        let ct = CancellationToken::default();

        for i in 0..50 {
            cursor_provider.parent.insert(
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
        let (mut cursor_provider, cursor_provider_tx) = new_cursor_provider();
        let ct = CancellationToken::default();

        for i in 0..100 {
            cursor_provider.parent.insert(
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

    impl error_stack::Context for TestError {}

    impl std::fmt::Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Test error")
        }
    }

    #[async_trait::async_trait]
    impl SnapshotReader for TestSnapshotReader {
        async fn read(&self) -> Result<Snapshot, SnapshotError> {
            Ok(self.snapshot.clone())
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
            Ok(self.parent.get(cursor).cloned().expect("parent not found"))
        }
    }
}
