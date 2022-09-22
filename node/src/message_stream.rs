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

use apibara_core::stream::Sequence;
use futures::Stream;
use pin_project::pin_project;
use tokio_util::sync::CancellationToken;

use crate::{message::Message, message_storage::MessageStorage};

#[pin_project]
pub struct BackfilledMessageStream<M, S, L>
where
    M: Message,
    S: MessageStorage<M>,
    L: Stream<Item = M>,
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
    Storage(#[from] Box<dyn std::error::Error>),
}

pub type Result<T> = std::result::Result<T, BackfilledMessageStreamError>;

#[derive(Debug)]
struct State<M: Message> {
    current: Sequence,
    latest: Sequence,
    buffer: VecDeque<M>,
}

impl<M, S, L> BackfilledMessageStream<M, S, L>
where
    M: Message,
    S: MessageStorage<M>,
    L: Stream<Item = M>,
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

impl<M: Message> State<M> {
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

    fn update_state_with_live_message(&mut self, message: &M) -> Result<()> {
        let sequence = message.sequence();
        self.latest = sequence;
        Ok(())
    }

    fn update_state_with_sent_message(&mut self, message: &M) -> Result<()> {
        let sequence = message.sequence();
        self.current = sequence;
        Ok(())
    }

    fn add_live_message(&mut self, message: M) {
        self.buffer.push_back(message);

        // trim buffer size to always be ~50 elements
        while self.buffer.len() > 50 {
            self.buffer.pop_front();
        }
    }

    fn buffer_has_sequence(&self, sequence: &Sequence) -> bool {
        match self.buffer.front() {
            None => false,
            Some(m) => &m.sequence() <= sequence,
        }
    }

    fn pop_buffer(&mut self) -> Option<M> {
        self.buffer.pop_front()
    }
}

impl<M, S, L> Stream for BackfilledMessageStream<M, S, L>
where
    M: Message,
    S: MessageStorage<M>,
    L: Stream<Item = M>,
{
    type Item = Result<M>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // always check cancellation
        if self.ct.is_cancelled() {
            return Poll::Ready(None);
        }

        // there are three possible actions, depending on the stream state:
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
            match this.state.update_state_with_live_message(&message) {
                Ok(_) => {}
                Err(err) => return Poll::Ready(Some(Err(err))),
            }

            // just send the message to the stream if it's the current one
            if current == message.sequence() {
                match this.state.update_state_with_sent_message(&message) {
                    Ok(_) => {}
                    Err(err) => return Poll::Ready(Some(Err(err))),
                }
                this.state.increment_current();
                return Poll::Ready(Some(Ok(message)));
            }

            // no point in adding messages that won't be sent
            if current < message.sequence() {
                // add message to buffer
                this.state.add_live_message(message);
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
                Some(message) => {
                    this.state.increment_current();
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
                this.state.increment_current();
                Poll::Ready(Some(Ok(message)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    use apibara_core::stream::Sequence;
    use futures::StreamExt;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;
    use tokio_util::sync::CancellationToken;

    use crate::{message::Message, message_storage::MessageStorage};

    use super::BackfilledMessageStream;

    #[derive(Clone, prost::Message)]
    pub struct TestMessage {
        #[prost(uint64, tag = "1")]
        pub sequence: u64,
    }

    impl Message for TestMessage {
        fn sequence(&self) -> Sequence {
            Sequence::from_u64(self.sequence)
        }
    }

    impl TestMessage {
        pub fn new(sequence: u64) -> TestMessage {
            TestMessage { sequence }
        }
    }

    #[derive(Debug, Default)]
    pub struct TestMessageStorage {
        messages: HashMap<Sequence, TestMessage>,
    }

    #[derive(Debug, thiserror::Error)]
    pub enum TestMessageStorageError {}

    impl MessageStorage<TestMessage> for Arc<Mutex<TestMessageStorage>> {
        type Error = TestMessageStorageError;

        fn insert(&mut self, message: &TestMessage) -> Result<(), Self::Error> {
            self.lock()
                .unwrap()
                .messages
                .insert(message.sequence(), message.clone());
            Ok(())
        }

        fn invalidate(&mut self, _sequence: &Sequence) -> Result<usize, Self::Error> {
            unimplemented!()
        }

        fn get(&self, sequence: &Sequence) -> Result<Option<TestMessage>, Self::Error> {
            Ok(self.lock().unwrap().messages.get(sequence).cloned())
        }
    }

    #[tokio::test]
    pub async fn test_transition_between_backfilled_and_live() {
        let mut storage = Arc::new(Mutex::new(TestMessageStorage::default()));

        for sequence in 0..10 {
            let message = TestMessage::new(sequence);
            storage.insert(&message).unwrap();
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

        live_tx.send(TestMessage::new(10)).await.unwrap();

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
            let message = TestMessage::new(sequence);
            storage.insert(&message).unwrap();
            live_tx.send(message).await.unwrap();
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
            let message = TestMessage::new(sequence);
            live_tx.send(message).await.unwrap();
        }

        for sequence in 15..20 {
            let message = stream.next().await.unwrap().unwrap();
            assert_eq!(message.sequence().as_u64(), sequence);
        }
    }
}
