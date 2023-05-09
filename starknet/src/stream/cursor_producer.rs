use std::{
    pin::Pin,
    sync::Arc,
    task::{self, Poll, Waker},
};

use apibara_core::{node::v1alpha2::DataFinality, starknet::v1alpha2};
use apibara_node::{
    async_trait,
    stream::{
        BatchCursor, CursorProducer, IngestionMessage, IngestionResponse, StreamConfiguration,
        StreamError,
    },
};
use futures::{stream::FusedStream, Stream};

use crate::{core::GlobalBlockId, db::StorageReader};

/// A [CursorProducer] that produces sequential cursors.
pub struct SequentialCursorProducer<R: StorageReader + Send + Sync + 'static> {
    configured: bool,
    current: Option<GlobalBlockId>,
    data_finality: DataFinality,
    ingestion_state: Option<IngestionState>,
    storage: Arc<R>,
    waker: Option<Waker>,
}

#[derive(Default, Debug)]
struct IngestionState {
    finalized: Option<GlobalBlockId>,
    accepted: Option<GlobalBlockId>,
    pending: Option<GlobalBlockId>,
}

impl<R> SequentialCursorProducer<R>
where
    R: StorageReader + Send + Sync + 'static,
{
    pub fn new(storage: Arc<R>) -> Self {
        SequentialCursorProducer {
            configured: false,
            current: None,
            data_finality: DataFinality::DataStatusAccepted,
            storage,
            ingestion_state: None,
            waker: None,
        }
    }

    pub fn next_cursor(&mut self) -> Result<Option<GlobalBlockId>, R::Error> {
        let next_block_number = self.current.map(|c| c.number() + 1).unwrap_or(0);

        if !self.should_produce_cursor(next_block_number)? {
            return Ok(None);
        }

        let next_cursor = self.storage.canonical_block_id(next_block_number)?;

        // only update cursor if we have a new block
        if let Some(next_cursor) = next_cursor {
            self.current = Some(next_cursor);
        }

        Ok(next_cursor)
    }

    fn should_produce_cursor(&mut self, next_block_number: u64) -> Result<bool, R::Error> {
        if !self.configured {
            return Ok(false);
        }

        let finality = self.data_finality;
        let state = self.get_ingestion_state()?;

        // if client request pending, produce a cursor unless we already reached the head.
        if finality == DataFinality::DataStatusPending {
            if let Some(pending) = state.pending {
                return Ok(next_block_number <= pending.number());
            }
        }

        // produce cursor only if 1) have a finalized cursor 2) next cursor is before that
        if finality == DataFinality::DataStatusFinalized {
            if let Some(finalized) = state.finalized {
                return Ok(next_block_number <= finalized.number());
            }
        }

        let accepted_finality = finality == DataFinality::DataStatusAccepted
            || finality == DataFinality::DataStatusPending;

        if accepted_finality {
            if let Some(accepted) = state.accepted {
                return Ok(next_block_number <= accepted.number());
            }
        }

        // nothing matched. need more data to make a decision
        Ok(false)
    }

    fn get_ingestion_state(&mut self) -> Result<&mut IngestionState, R::Error> {
        // Read new state only if we don't have one yet.
        // Initialize with default value otherwise to make the borrow checker happy.
        let new_state = if self.ingestion_state.is_some() {
            IngestionState::default()
        } else {
            let accepted = self.storage.highest_accepted_block()?;
            let finalized = self.storage.highest_finalized_block()?;
            IngestionState {
                accepted,
                finalized,
                pending: None,
            }
        };

        Ok(self.ingestion_state.get_or_insert(new_state))
    }
}

fn lowest_cursor(a: GlobalBlockId, b: GlobalBlockId) -> GlobalBlockId {
    if a.number() < b.number() {
        a
    } else {
        b
    }
}

#[async_trait]
impl<R> CursorProducer for SequentialCursorProducer<R>
where
    R: StorageReader + Send + Sync + 'static,
{
    type Cursor = GlobalBlockId;
    type Filter = v1alpha2::Filter;

    fn reconfigure(
        &mut self,
        configuration: &StreamConfiguration<Self::Cursor, Self::Filter>,
    ) -> Result<(), StreamError> {
        self.configured = true;
        self.current = configuration.starting_cursor;
        self.data_finality = configuration.finality;
        Ok(())
    }

    async fn handle_ingestion_message(
        &mut self,
        message: &IngestionMessage<Self::Cursor>,
    ) -> Result<IngestionResponse<Self::Cursor>, StreamError> {
        let mut state = self.get_ingestion_state().map_err(StreamError::internal)?;
        let response = match message {
            IngestionMessage::Pending(cursor) => {
                state.pending = Some(*cursor);
                IngestionResponse::Ok
            }
            IngestionMessage::Accepted(cursor) => {
                state.finalized = None;
                state.accepted = Some(*cursor);
                IngestionResponse::Ok
            }
            IngestionMessage::Finalized(cursor) => {
                state.finalized = Some(*cursor);
                IngestionResponse::Ok
            }
            IngestionMessage::Invalidate(cursor) => {
                state.pending = None;
                state.accepted = state.accepted.map(|c| lowest_cursor(c, *cursor));
                state.finalized = state.finalized.map(|c| lowest_cursor(c, *cursor));
                // if the current cursor is after the new head, then data was invalidated.
                let is_invalidated = self
                    .current
                    .map(|c| c.number() > cursor.number())
                    .unwrap_or(false);

                self.current = self.current.map(|c| lowest_cursor(c, *cursor));

                if is_invalidated {
                    IngestionResponse::Invalidate(*cursor)
                } else {
                    IngestionResponse::Ok
                }
            }
        };

        // wake up the stream if it was waiting for a new block
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }

        Ok(response)
    }
}

impl<R> Stream for SequentialCursorProducer<R>
where
    R: StorageReader + Send + Sync + 'static,
{
    type Item = Result<BatchCursor<GlobalBlockId>, StreamError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Option<Self::Item>> {
        /*
        match self.next_cursor() {
            Err(err) => {
                let err = StreamError::internal(err);
                Poll::Ready(Some(Err(err)))
            }
            Ok(None) => {
                // no new block yet, store waker and wake after a new ingestion message
                self.waker = Some(cx.waker().clone());
                Poll::Pending
            }
            Ok(Some(cursor)) => Poll::Ready(Some(Ok(cursor))),
        }
        */
        todo!()
    }
}

impl<R> FusedStream for SequentialCursorProducer<R>
where
    R: StorageReader + Send + Sync + 'static,
{
    fn is_terminated(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use apibara_core::{node::v1alpha2::DataFinality, starknet::v1alpha2::Filter};
    use apibara_node::stream::{CursorProducer, IngestionMessage, StreamConfiguration};
    use futures::{FutureExt, StreamExt, TryStreamExt};

    use crate::{
        core::{BlockHash, GlobalBlockId},
        db::{MockStorageReader, StorageReader},
    };

    use super::SequentialCursorProducer;

    fn new_block_hash(n: u64, c: u8) -> BlockHash {
        let mut b = [0; 32];
        b[24..].copy_from_slice(&n.to_be_bytes());
        b[0] = c;
        BlockHash::from_slice(&b).unwrap()
    }

    fn new_block_id(num: u64) -> GlobalBlockId {
        let hash = new_block_hash(num, 0);
        GlobalBlockId::new(num, hash)
    }

    fn new_configuration(
        starting_cursor: Option<GlobalBlockId>,
        finality: DataFinality,
    ) -> StreamConfiguration<GlobalBlockId, Filter> {
        StreamConfiguration {
            batch_size: 1,
            stream_id: 0,
            finality,
            starting_cursor,
            filter: Filter::default(),
        }
    }

    fn new_producer<R>(
        cursor: Option<GlobalBlockId>,
        finality: DataFinality,
        storage: Arc<R>,
    ) -> SequentialCursorProducer<R>
    where
        R: StorageReader + Send + Sync + 'static,
    {
        let mut producer = SequentialCursorProducer::new(storage);
        producer
            .reconfigure(&new_configuration(cursor, finality))
            .unwrap();
        producer
    }

    #[tokio::test]
    async fn test_produce_full_batch() {
        let mut storage = MockStorageReader::new();
        storage
            .expect_canonical_block_id()
            .returning(|i| Ok(Some(new_block_id(i))));
        storage
            .expect_highest_accepted_block()
            .returning(|| Ok(Some(new_block_id(100))));
        storage
            .expect_highest_finalized_block()
            .returning(|| Ok(Some(new_block_id(90))));
        let mut producer = new_producer(None, DataFinality::DataStatusAccepted, Arc::new(storage));

        let cursors: Vec<_> = producer.take(10).try_collect().await.unwrap();
        assert_eq!(cursors.len(), 10);
        for (i, cursor) in cursors.iter().enumerate() {
            assert_eq!(cursor.number(), i as u64);
        }
    }

    #[tokio::test]
    async fn test_reach_head_of_chain() {
        let mut storage = MockStorageReader::new();
        storage
            .expect_canonical_block_id()
            .returning(|i| Ok(Some(new_block_id(i))));
        storage
            .expect_highest_accepted_block()
            .returning(|| Ok(Some(new_block_id(4))));
        storage
            .expect_highest_finalized_block()
            .returning(|| Ok(Some(new_block_id(1))));

        let mut producer = new_producer(None, DataFinality::DataStatusAccepted, Arc::new(storage));

        for i in 0..5 {
            let cursor = producer.try_next().await.unwrap().unwrap();
            assert_eq!(cursor.number(), i as u64);
        }

        let cursor = producer.try_next().now_or_never();
        assert!(cursor.is_none());
    }

    #[tokio::test]
    async fn test_start_at_given_cursor() {
        let mut storage = MockStorageReader::new();
        storage
            .expect_canonical_block_id()
            .returning(|i| Ok(Some(new_block_id(i))));
        storage
            .expect_highest_accepted_block()
            .returning(|| Ok(Some(new_block_id(200))));
        storage
            .expect_highest_finalized_block()
            .returning(|| Ok(Some(new_block_id(190))));

        let starting_cursor = new_block_id(103);
        let producer = new_producer(
            Some(starting_cursor),
            DataFinality::DataStatusAccepted,
            Arc::new(storage),
        );

        let cursors: Vec<_> = producer.take(10).try_collect().await.unwrap();
        assert_eq!(cursors.len(), 10);
        for (i, cursor) in cursors.iter().enumerate() {
            assert_eq!(cursor.number(), 104 + i as u64);
        }
    }

    #[tokio::test]
    async fn test_handle_finalized_message_finalized_finality() {
        let mut storage = MockStorageReader::new();
        storage
            .expect_canonical_block_id()
            .returning(|i| Ok(Some(new_block_id(i))));
        storage
            .expect_highest_accepted_block()
            .returning(|| Ok(Some(new_block_id(4))));
        storage
            .expect_highest_finalized_block()
            .returning(|| Ok(Some(new_block_id(1))));

        let mut producer = new_producer(None, DataFinality::DataStatusFinalized, Arc::new(storage));

        for i in 0..2 {
            let cursor = producer.try_next().await.unwrap().unwrap();
            assert_eq!(cursor.number(), i as u64);
        }

        let cursor = producer.try_next().now_or_never();
        assert!(cursor.is_none());

        producer
            .handle_ingestion_message(&IngestionMessage::Finalized(new_block_id(3)))
            .await
            .unwrap();

        for i in 2..4 {
            let cursor = producer.try_next().await.unwrap().unwrap();
            assert_eq!(cursor.number(), i as u64);
        }

        let cursor = producer.try_next().now_or_never();
        assert!(cursor.is_none());
    }

    #[tokio::test]
    async fn test_handle_accepted_message() {
        let mut storage = MockStorageReader::new();
        storage
            .expect_canonical_block_id()
            .returning(|i| Ok(Some(new_block_id(i))));
        storage
            .expect_highest_accepted_block()
            .returning(|| Ok(Some(new_block_id(4))));
        storage
            .expect_highest_finalized_block()
            .returning(|| Ok(Some(new_block_id(1))));

        let mut producer = new_producer(None, DataFinality::DataStatusAccepted, Arc::new(storage));

        for i in 0..5 {
            let cursor = producer.try_next().await.unwrap().unwrap();
            assert_eq!(cursor.number(), i as u64);
        }

        let cursor = producer.try_next().now_or_never();
        assert!(cursor.is_none());

        producer
            .handle_ingestion_message(&IngestionMessage::Accepted(new_block_id(6)))
            .await
            .unwrap();

        for i in 5..7 {
            let cursor = producer.try_next().await.unwrap().unwrap();
            assert_eq!(cursor.number(), i as u64);
        }

        let cursor = producer.try_next().now_or_never();
        assert!(cursor.is_none());
    }

    #[tokio::test]
    async fn test_handle_accepted_message_finalized_finality() {
        let mut storage = MockStorageReader::new();
        storage
            .expect_canonical_block_id()
            .returning(|i| Ok(Some(new_block_id(i))));
        storage
            .expect_highest_accepted_block()
            .returning(|| Ok(Some(new_block_id(4))));
        storage
            .expect_highest_finalized_block()
            .returning(|| Ok(Some(new_block_id(1))));

        let mut producer = new_producer(None, DataFinality::DataStatusFinalized, Arc::new(storage));

        for i in 0..2 {
            let cursor = producer.try_next().await.unwrap().unwrap();
            assert_eq!(cursor.number(), i as u64);
        }

        let cursor = producer.try_next().now_or_never();
        assert!(cursor.is_none());

        producer
            .handle_ingestion_message(&IngestionMessage::Accepted(new_block_id(6)))
            .await
            .unwrap();

        let cursor = producer.try_next().now_or_never();
        assert!(cursor.is_none());
    }

    #[tokio::test]
    async fn test_handle_pending_message() {
        let mut storage = MockStorageReader::new();
        storage
            .expect_canonical_block_id()
            .returning(|i| Ok(Some(new_block_id(i))));
        storage
            .expect_highest_accepted_block()
            .returning(|| Ok(Some(new_block_id(4))));
        storage
            .expect_highest_finalized_block()
            .returning(|| Ok(Some(new_block_id(1))));

        let mut producer = new_producer(None, DataFinality::DataStatusPending, Arc::new(storage));

        for i in 0..5 {
            let cursor = producer.try_next().await.unwrap().unwrap();
            assert_eq!(cursor.number(), i as u64);
        }

        let cursor = producer.try_next().now_or_never();
        assert!(cursor.is_none());

        producer
            .handle_ingestion_message(&IngestionMessage::Pending(new_block_id(5)))
            .await
            .unwrap();

        let cursor = producer
            .try_next()
            .now_or_never()
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(cursor.number(), 5);
    }

    #[tokio::test]
    async fn test_handle_pending_message_accepted_finality() {
        let mut storage = MockStorageReader::new();
        storage
            .expect_canonical_block_id()
            .returning(|i| Ok(Some(new_block_id(i))));
        storage
            .expect_highest_accepted_block()
            .returning(|| Ok(Some(new_block_id(4))));
        storage
            .expect_highest_finalized_block()
            .returning(|| Ok(Some(new_block_id(1))));

        let mut producer = new_producer(None, DataFinality::DataStatusAccepted, Arc::new(storage));

        for i in 0..5 {
            let cursor = producer.try_next().await.unwrap().unwrap();
            assert_eq!(cursor.number(), i as u64);
        }

        let cursor = producer.try_next().now_or_never();
        assert!(cursor.is_none());

        producer
            .handle_ingestion_message(&IngestionMessage::Pending(new_block_id(5)))
            .await
            .unwrap();

        let cursor = producer.try_next().now_or_never();
        assert!(cursor.is_none());
    }

    #[tokio::test]
    async fn test_handle_invalidate_message_after_current() {
        let mut storage = MockStorageReader::new();
        storage
            .expect_canonical_block_id()
            .returning(|i| Ok(Some(new_block_id(i))));
        storage
            .expect_highest_accepted_block()
            .returning(|| Ok(Some(new_block_id(4))));
        storage
            .expect_highest_finalized_block()
            .returning(|| Ok(Some(new_block_id(1))));

        let mut producer = new_producer(None, DataFinality::DataStatusAccepted, Arc::new(storage));

        for i in 0..1 {
            let cursor = producer.try_next().await.unwrap().unwrap();
            assert_eq!(cursor.number(), i as u64);
        }

        producer
            .handle_ingestion_message(&IngestionMessage::Invalidate(new_block_id(3)))
            .await
            .unwrap();

        for i in 1..4 {
            let cursor = producer.try_next().await.unwrap().unwrap();
            assert_eq!(cursor.number(), i as u64);
        }

        let cursor = producer.try_next().now_or_never();
        assert!(cursor.is_none());
    }

    #[tokio::test]
    async fn test_handle_invalidate_message_before_current_accepted() {
        let mut storage = MockStorageReader::new();
        storage
            .expect_canonical_block_id()
            .returning(|i| Ok(Some(new_block_id(i))));
        storage
            .expect_highest_accepted_block()
            .returning(|| Ok(Some(new_block_id(4))));
        storage
            .expect_highest_finalized_block()
            .returning(|| Ok(Some(new_block_id(1))));

        let mut producer = new_producer(None, DataFinality::DataStatusAccepted, Arc::new(storage));

        for i in 0..3 {
            let cursor = producer.try_next().await.unwrap().unwrap();
            assert_eq!(cursor.number(), i as u64);
        }

        producer
            .handle_ingestion_message(&IngestionMessage::Invalidate(new_block_id(2)))
            .await
            .unwrap();

        let cursor = producer.try_next().now_or_never();
        assert!(cursor.is_none());

        producer
            .handle_ingestion_message(&IngestionMessage::Accepted(new_block_id(4)))
            .await
            .unwrap();

        for i in 3..5 {
            let cursor = producer.try_next().await.unwrap().unwrap();
            assert_eq!(cursor.number(), i as u64);
        }

        let cursor = producer.try_next().now_or_never();
        assert!(cursor.is_none());
    }

    #[tokio::test]
    async fn test_handle_invalidate_message_before_current_finalized() {
        let mut storage = MockStorageReader::new();
        storage
            .expect_canonical_block_id()
            .returning(|i| Ok(Some(new_block_id(i))));
        storage
            .expect_highest_accepted_block()
            .returning(|| Ok(Some(new_block_id(8))));
        storage
            .expect_highest_finalized_block()
            .returning(|| Ok(Some(new_block_id(4))));

        let mut producer = new_producer(None, DataFinality::DataStatusFinalized, Arc::new(storage));

        for i in 0..4 {
            let cursor = producer.try_next().await.unwrap().unwrap();
            assert_eq!(cursor.number(), i as u64);
        }

        producer
            .handle_ingestion_message(&IngestionMessage::Invalidate(new_block_id(2)))
            .await
            .unwrap();

        let cursor = producer.try_next().now_or_never();
        assert!(cursor.is_none());

        producer
            .handle_ingestion_message(&IngestionMessage::Accepted(new_block_id(6)))
            .await
            .unwrap();
        producer
            .handle_ingestion_message(&IngestionMessage::Finalized(new_block_id(3)))
            .await
            .unwrap();

        for i in 3..4 {
            let cursor = producer.try_next().await.unwrap().unwrap();
            assert_eq!(cursor.number(), i as u64);
        }

        let cursor = producer.try_next().now_or_never();
        assert!(cursor.is_none());
    }
}
