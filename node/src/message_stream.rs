//! Stream messages with backfilling.
//!
//! This object is used to create a continuous stream that
//! seamlessly switches between sending historical messages to
//! sending live messages.
//! It can also start streaming from a sequence number not yet
//! produced: in this case it waits until the live stream reaches
//! the target sequence number.
//!
//! ```txt
//!         ┌───────────┐
//!         │           │
//! Live────►           ├───► Backfilled
//!         │           │     Stream
//!         └─────▲─────┘
//!               │
//!               │
//!            Message
//!            Storage
//! ```

use std::{collections::VecDeque, marker::PhantomData, pin::Pin, task::Poll};

use apibara_core::stream::{MessageData, RawMessageData, Sequence, StreamMessage};
use futures::Stream;
use pin_project::pin_project;
use tokio_util::sync::CancellationToken;

use crate::message_storage::MessageStorage;

pub type LiveStreamItem<M> = std::result::Result<StreamMessage<M>, Box<dyn std::error::Error>>;

#[pin_project]
pub struct BackfilledMessageStream<M, S, L>
where
    M: MessageData,
    S: MessageStorage<M>,
    L: Stream<Item = LiveStreamItem<M>>,
{
    storage: S,
    #[pin]
    live: L,
    // state is in its own struct to play nicely with pin.
    state: State<M>,
    ct: CancellationToken,
    _phantom: PhantomData<M>,
}

#[derive(Debug, thiserror::Error)]
pub enum BackfilledMessageStreamError {
    #[error("invalid live message sequence number")]
    InvalidLiveSequence { expected: u64, actual: u64 },
    #[error("message with sequence {sequence} not found")]
    MessageNotFound { sequence: u64 },
    #[error("error retrieving data from message storage")]
    Storage(Box<dyn std::error::Error>),
    #[error("error retrieving data from live stream")]
    LiveStream(Box<dyn std::error::Error>),
}

pub type Result<T> = std::result::Result<T, BackfilledMessageStreamError>;

#[derive(Debug)]
struct State<M: MessageData> {
    current: Sequence,
    latest: Sequence,
    buffer: VecDeque<(Sequence, RawMessageData<M>)>,
}

impl<M, S, L> BackfilledMessageStream<M, S, L>
where
    M: MessageData,
    S: MessageStorage<M>,
    L: Stream<Item = LiveStreamItem<M>>,
{
    /// Creates a new `MessageStreamer`.
    ///
    /// Start streaming from the `current` message (inclusive), using `latest` as
    /// hint about the most recently stored message.
    /// Messages that are not `live` are streamed from the `storage`.
    pub fn new(
        current: Sequence,
        latest: Sequence,
        storage: S,
        live: L,
        ct: CancellationToken,
    ) -> Self {
        BackfilledMessageStream {
            storage,
            live,
            state: State::new(current, latest),
            ct,
            _phantom: PhantomData::default(),
        }
    }
}

impl<M, S, L> Stream for BackfilledMessageStream<M, S, L>
where
    M: MessageData,
    S: MessageStorage<M>,
    L: Stream<Item = LiveStreamItem<M>>,
{
    type Item = Result<StreamMessage<M>>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // always check cancellation
        if self.ct.is_cancelled() {
            return Poll::Ready(None);
        }

        // when receiving a `StreamMessage::Data` message the stream can perform
        // three possible actions, depending on the stream state:
        //
        // current < latest:
        //   live stream: used to keep latest updated
        //   storage: used to read backfilled data and send it to stream
        // current == latest:
        //   live stream: used to keep state updated and send data to stream
        //   storage: not used
        // current > latest:
        //   live stream: used to keep track of state, but data is not sent
        //   storage: not used
        //
        // when receiving a `StreamMessage::Invalidate` message the stream
        // can perform:
        //
        // current < invalidate:
        //   update latest from invalidate
        // current >= invalidate:
        //   update current and latest from invalidate
        //   send invalidate message to stream

        let current = self.state.current;
        let latest = self.state.latest;

        let mut this = self.project();

        let live_message = {
            match Pin::new(&mut this.live).poll_next(cx) {
                Poll::Pending => {
                    // return pending and wake when live stream is ready
                    if current > latest {
                        return Poll::Pending;
                    }
                    None
                }
                Poll::Ready(None) => {
                    // live stream closed, try to keep sending backfilled messages
                    // but if it's live simply close this stream too.
                    if current >= latest {
                        return Poll::Ready(None);
                    }
                    None
                }
                Poll::Ready(Some(message)) => Some(message),
            }
        };

        if let Some(message) = live_message {
            match message {
                Err(err) => {
                    return Poll::Ready(Some(Err(BackfilledMessageStreamError::LiveStream(err))))
                }
                Ok(StreamMessage::Invalidate { sequence }) => {
                    // clear buffer just in case
                    this.state.clear_buffer();
                    this.state.update_latest(sequence);

                    // all messages after `sequence` (inclusive) are now invalidated.
                    // forward invalidate message
                    if current >= sequence {
                        this.state.update_current(sequence);
                        let message = StreamMessage::Invalidate { sequence };
                        return Poll::Ready(Some(Ok(message)));
                    }

                    // buffer data was invalidated and new state updated
                    // let's stop here and start again
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Ok(StreamMessage::Data { sequence, data }) => {
                    this.state.update_latest(sequence);

                    // just send the message to the stream if it's the current one
                    if current == sequence {
                        this.state.update_current(sequence);
                        this.state.increment_current();
                        let message = StreamMessage::Data { sequence, data };
                        return Poll::Ready(Some(Ok(message)));
                    }

                    // no point in adding messages that won't be sent
                    if current < sequence {
                        // add message to buffer
                        this.state.add_live_message(sequence, data);
                    }
                }
                Ok(StreamMessage::Pending { sequence, data }) => {
                    if sequence == current {
                        let message = StreamMessage::Pending { sequence, data };
                        return Poll::Ready(Some(Ok(message)));
                    }
                }
            }
        }

        // stream is not interested in any messages we can send, so just
        // restart and wait for more live data
        if current > latest {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        // prioritize sending from buffer
        if this.state.buffer_has_sequence(this.state.current()) {
            match this.state.pop_buffer() {
                None => {
                    let sequence = this.state.current().as_u64();
                    return Poll::Ready(Some(Err(BackfilledMessageStreamError::MessageNotFound {
                        sequence,
                    })));
                }
                Some((sequence, data)) => {
                    this.state.increment_current();
                    let message = StreamMessage::Data { sequence, data };
                    return Poll::Ready(Some(Ok(message)));
                }
            }
        }

        // as last resort, send backfilled messages from storage
        match this.storage.get(this.state.current()) {
            Err(err) => {
                let err = BackfilledMessageStreamError::Storage(Box::new(err));
                Poll::Ready(Some(Err(err)))
            }
            Ok(None) => {
                let sequence = this.state.current().as_u64();
                Poll::Ready(Some(Err(BackfilledMessageStreamError::MessageNotFound {
                    sequence,
                })))
            }
            Ok(Some(message)) => {
                let sequence = *this.state.current();
                this.state.increment_current();
                let message = StreamMessage::Data {
                    sequence,
                    data: message,
                };
                Poll::Ready(Some(Ok(message)))
            }
        }
    }
}

impl<M: MessageData> State<M> {
    fn new(current: Sequence, latest: Sequence) -> Self {
        State {
            current,
            latest,
            buffer: VecDeque::default(),
        }
    }

    fn current(&self) -> &Sequence {
        &self.current
    }

    fn increment_current(&mut self) {
        self.current = Sequence::from_u64(self.current.as_u64() + 1);
    }

    fn update_latest(&mut self, sequence: Sequence) {
        self.latest = sequence;
    }

    fn update_current(&mut self, sequence: Sequence) {
        self.current = sequence;
    }

    fn add_live_message(&mut self, sequence: Sequence, message: RawMessageData<M>) {
        self.buffer.push_back((sequence, message));

        // trim buffer size to always be ~50 elements
        while self.buffer.len() > 50 {
            self.buffer.pop_front();
        }
    }

    fn clear_buffer(&mut self) {
        self.buffer.clear();
    }

    fn buffer_has_sequence(&self, sequence: &Sequence) -> bool {
        match self.buffer.front() {
            None => false,
            Some((seq, _)) => seq <= sequence,
        }
    }

    fn pop_buffer(&mut self) -> Option<(Sequence, RawMessageData<M>)> {
        self.buffer.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    use apibara_core::stream::{RawMessageData, Sequence, StreamMessage};
    use futures::StreamExt;
    use prost::Message;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;
    use tokio_util::sync::CancellationToken;

    use crate::message_storage::MessageStorage;

    use super::BackfilledMessageStream;

    #[derive(Clone, prost::Message)]
    pub struct TestMessage {
        #[prost(uint64, tag = "1")]
        pub sequence: u64,
    }

    impl TestMessage {
        pub fn new(sequence: u64) -> TestMessage {
            TestMessage { sequence }
        }

        pub fn new_raw(sequence: u64) -> RawMessageData<TestMessage> {
            let data = Self::new(sequence).encode_to_vec();
            RawMessageData::from_vec(data)
        }
    }

    #[derive(Debug, Default)]
    pub struct TestMessageStorage {
        messages: HashMap<Sequence, RawMessageData<TestMessage>>,
    }

    #[derive(Debug, thiserror::Error)]
    pub enum TestMessageStorageError {}

    impl TestMessageStorage {
        pub fn insert(&mut self, sequence: &Sequence, message: &TestMessage) {
            let message = RawMessageData::from_vec(message.encode_to_vec());
            self.insert_raw(sequence, message);
        }

        pub fn insert_raw(&mut self, sequence: &Sequence, message: RawMessageData<TestMessage>) {
            self.messages.insert(*sequence, message);
        }
    }

    impl MessageStorage<TestMessage> for Arc<Mutex<TestMessageStorage>> {
        type Error = TestMessageStorageError;

        fn get(
            &self,
            sequence: &Sequence,
        ) -> Result<Option<RawMessageData<TestMessage>>, Self::Error> {
            Ok(self.lock().unwrap().messages.get(sequence).cloned())
        }
    }

    #[tokio::test]
    pub async fn test_transition_between_backfilled_and_live() {
        let storage = Arc::new(Mutex::new(TestMessageStorage::default()));

        for sequence in 0..10 {
            let message = TestMessage::new(sequence);
            storage
                .lock()
                .unwrap()
                .insert(&Sequence::from_u64(sequence), &message);
        }

        let (live_tx, live_rx) = mpsc::channel(256);
        let live_stream = ReceiverStream::new(live_rx);
        let ct = CancellationToken::new();

        let mut stream = BackfilledMessageStream::new(
            Sequence::from_u64(0),
            Sequence::from_u64(9),
            storage.clone(),
            live_stream,
            ct,
        );

        live_tx
            .send(Ok(StreamMessage::new_data(
                Sequence::from_u64(10),
                TestMessage::new_raw(10),
            )))
            .await
            .unwrap();

        // first 10 messages come from storage
        for sequence in 0..10 {
            let message = stream.next().await.unwrap().unwrap();
            assert_eq!(message.sequence().as_u64(), sequence);
        }

        // 11th message from live stream
        let message = stream.next().await.unwrap().unwrap();
        assert_eq!(message.sequence().as_u64(), 10);

        // simulate node adding messages to storage (for persistence) while
        // publishing to live stream
        for sequence in 11..100 {
            let message = TestMessage::new_raw(sequence);
            let sequence = Sequence::from_u64(sequence);
            storage
                .lock()
                .unwrap()
                .insert_raw(&sequence, message.clone());
            let message = StreamMessage::new_data(sequence, message);
            live_tx.send(Ok(message)).await.unwrap();
        }

        for sequence in 11..100 {
            let message = stream.next().await.unwrap().unwrap();
            assert_eq!(message.sequence().as_u64(), sequence);
        }
    }

    #[tokio::test]
    pub async fn test_start_at_future_sequence() {
        let storage = Arc::new(Mutex::new(TestMessageStorage::default()));

        let (live_tx, live_rx) = mpsc::channel(256);
        let live_stream = ReceiverStream::new(live_rx);
        let ct = CancellationToken::new();

        let mut stream = BackfilledMessageStream::new(
            Sequence::from_u64(15),
            Sequence::from_u64(9),
            storage.clone(),
            live_stream,
            ct,
        );

        for sequence in 10..20 {
            let message = TestMessage::new_raw(sequence);
            let sequence = Sequence::from_u64(sequence);
            let message = StreamMessage::new_data(sequence, message);
            live_tx.send(Ok(message)).await.unwrap();
        }

        for sequence in 15..20 {
            let message = stream.next().await.unwrap().unwrap();
            assert_eq!(message.sequence().as_u64(), sequence);
        }
    }

    #[tokio::test]
    pub async fn test_invalidate_data_after_current() {
        let storage = Arc::new(Mutex::new(TestMessageStorage::default()));

        let (live_tx, live_rx) = mpsc::channel(256);
        let live_stream = ReceiverStream::new(live_rx);
        let ct = CancellationToken::new();

        let mut stream = BackfilledMessageStream::new(
            Sequence::from_u64(0),
            Sequence::from_u64(9),
            storage.clone(),
            live_stream,
            ct,
        );

        // add some messages to storage
        for sequence in 0..5 {
            let message = TestMessage::new(sequence);
            let sequence = Sequence::from_u64(sequence);
            storage.lock().unwrap().insert(&sequence, &message);
        }

        // live stream some messages
        for sequence in 5..10 {
            let message = TestMessage::new_raw(sequence);
            let sequence = Sequence::from_u64(sequence);
            storage
                .lock()
                .unwrap()
                .insert_raw(&sequence, message.clone());
            let message = StreamMessage::new_data(sequence, message);
            live_tx.send(Ok(message)).await.unwrap();
        }

        // invalidate all messages with sequence >= 8
        let sequence = Sequence::from_u64(8);
        let message = StreamMessage::new_invalidate(sequence);
        live_tx.send(Ok(message)).await.unwrap();

        // then send some more messages
        for sequence in 8..12 {
            let message = TestMessage::new_raw(sequence);
            let sequence = Sequence::from_u64(sequence);
            storage
                .lock()
                .unwrap()
                .insert_raw(&sequence, message.clone());
            let message = StreamMessage::new_data(sequence, message);
            live_tx.send(Ok(message)).await.unwrap();
        }

        // notice there is no invalidate message because it happened for a
        // message sequence that was never streamed
        for sequence in 0..12 {
            let message = stream.next().await.unwrap().unwrap();
            assert_eq!(message.sequence().as_u64(), sequence);
            assert!(message.is_data());
        }
    }

    #[tokio::test]
    pub async fn test_invalidate_before_current() {
        let storage = Arc::new(Mutex::new(TestMessageStorage::default()));

        let (live_tx, live_rx) = mpsc::channel(256);
        let live_stream = ReceiverStream::new(live_rx);
        let ct = CancellationToken::new();

        let mut stream = BackfilledMessageStream::new(
            Sequence::from_u64(0),
            Sequence::from_u64(9),
            storage.clone(),
            live_stream,
            ct,
        );

        // add some messages to storage
        for sequence in 0..5 {
            let message = TestMessage::new(sequence);
            let sequence = Sequence::from_u64(sequence);
            storage.lock().unwrap().insert(&sequence, &message);
        }

        // live stream some messages
        for sequence in 5..10 {
            let message = TestMessage::new_raw(sequence);
            let sequence = Sequence::from_u64(sequence);
            storage
                .lock()
                .unwrap()
                .insert_raw(&sequence, message.clone());
            let message = StreamMessage::new_data(sequence, message);
            live_tx.send(Ok(message)).await.unwrap();
        }

        // now stream messages up to 9
        for sequence in 0..10 {
            let message = stream.next().await.unwrap().unwrap();
            assert_eq!(message.sequence().as_u64(), sequence);
            assert!(message.is_data());
        }

        // invalidate all messages with sequence >= 8
        let sequence = Sequence::from_u64(8);
        let message = StreamMessage::new_invalidate(sequence);
        live_tx.send(Ok(message)).await.unwrap();

        // then send some more messages
        for sequence in 8..12 {
            let message = TestMessage::new_raw(sequence);
            let sequence = Sequence::from_u64(sequence);
            storage
                .lock()
                .unwrap()
                .insert_raw(&sequence, message.clone());
            let message = StreamMessage::new_data(sequence, message);
            live_tx.send(Ok(message)).await.unwrap();
        }

        // received invalidate message
        let message = stream.next().await.unwrap().unwrap();
        assert_eq!(message.sequence().as_u64(), 8);
        assert!(message.is_invalidate());

        // resume messages
        for sequence in 8..12 {
            let message = stream.next().await.unwrap().unwrap();
            assert_eq!(message.sequence().as_u64(), sequence);
            assert!(message.is_data());
        }
    }
}
