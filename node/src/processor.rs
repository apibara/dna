//! Input stream processor.

use std::{
    pin::Pin,
    sync::Arc,
    task::{self, Poll},
    time::Duration,
};

use apibara_core::{
    application::pb as app_pb,
    node::v1alpha1::pb as node_pb,
    stream::{MessageData, RawMessageData, Sequence, StreamId, StreamMessage},
};
use futures::Stream;
use libmdbx::{Environment, EnvironmentKind, Error as MdbxError};
use pin_project::pin_project;
use prost::Message;
use tokio::sync::{broadcast, Mutex, MutexGuard};
use tokio_stream::{wrappers::BroadcastStream, StreamExt};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::{
    application::Application,
    input::{InputMixer, InputMixerError, MixedInputStreamError},
    message_storage::{MdbxMessageStorage, MdbxMessageStorageError},
    message_stream::{BackfilledMessageStream, BackfilledMessageStreamError},
    sequencer::{Sequencer, SequencerError},
};

/// A service that allow streaming of messages.
pub trait MessageProducer: Send + Sync + 'static {
    type Message: MessageData;
    type Error: std::error::Error + Send + Sync + 'static;
    type Stream: Stream<
            Item = std::result::Result<StreamMessage<Self::Message>, BackfilledMessageStreamError>,
        > + Send;

    fn stream_from_sequence(
        &self,
        starting_sequence: &Sequence,
        pending_interval: Option<Duration>,
        ct: CancellationToken,
    ) -> std::result::Result<Self::Stream, Self::Error>;
}

pub type ProcessorMessage<M> = StreamMessage<M>;

/// Service to processor messages from the input stream.
pub struct Processor<A: Application + Send, E: EnvironmentKind> {
    application: Mutex<A>,
    db: Arc<Environment<E>>,
    sequencer: Sequencer<E>,
    storage: Arc<MdbxMessageStorage<E, A::Message>>,
    message_tx: broadcast::Sender<ProcessorMessage<A::Message>>,
    _message_rx: broadcast::Receiver<ProcessorMessage<A::Message>>,
}

#[derive(Debug, thiserror::Error)]
pub enum ProcessorError {
    #[error("database operation error")]
    Database(#[from] MdbxError),
    #[error("sequencer error")]
    Sequencer(#[from] SequencerError),
    #[error("message storage error")]
    Storage(#[from] MdbxMessageStorageError),
    #[error("error configuring input mixer")]
    InputMixer(#[from] InputMixerError),
    #[error("error streaming from input")]
    InputStream(#[from] MixedInputStreamError),
    #[error("application error")]
    Application(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("error broadcasting message")]
    Broadcast,
    // TODO: add back error information
    #[error("error streaming messages")]
    BackfilledMessageStream,
    #[error("error locking application")]
    ApplicationLock,
    #[error("message contains no data")]
    EmptyMessage,
    #[error("input stream was closed")]
    InputStreamClosed,
    #[error("input stream timed out")]
    InputStreamTimeout,
}

pub type Result<T> = std::result::Result<T, ProcessorError>;

const MESSAGE_CHANNEL_SIZE: usize = 1024;

/// Used to give a simpler type parameter to [BackfilledMessageStream].
#[pin_project]
pub struct LiveStream<M: MessageData> {
    #[pin]
    inner: BroadcastStream<StreamMessage<M>>,
}

impl<A, E> Processor<A, E>
where
    A: Application + Send,
    E: EnvironmentKind,
{
    /// Creates a new stream processor with the given application.
    pub fn new(db: Arc<Environment<E>>, application: A) -> Result<Self> {
        let sequencer = Sequencer::new(db.clone())?;
        let storage = MdbxMessageStorage::new(db.clone())?;

        let (message_tx, message_rx) = broadcast::channel(MESSAGE_CHANNEL_SIZE);

        Ok(Processor {
            application: Mutex::new(application),
            db,
            sequencer,
            storage: Arc::new(storage),
            message_tx,
            _message_rx: message_rx,
        })
    }

    /// Locks the application mutex and returns its value.
    pub async fn application(&self) -> MutexGuard<'_, A> {
        self.application.lock().await
    }

    /// Starts the input stream processor.
    ///
    /// The processor will start indexing messages from its inputs,
    /// generating a new sequence of output messages.
    pub async fn start(&self, inputs: &[app_pb::InputStream], ct: CancellationToken) -> Result<()> {
        let input_mixer = self.init_input_mixer(inputs).await?;
        let mut input_stream = input_mixer.stream_messages().await?;

        // timeout is the heartbeat interval (30 secs) + a reasonable interval (15 secs)
        let receive_message_timeout = Duration::from_secs(45);
        // enter main loop. keep streaming messages from the inputs and pass them to the application.
        loop {
            if ct.is_cancelled() {
                return Ok(());
            }

            match tokio::time::timeout(receive_message_timeout, input_stream.next()).await {
                Err(_) => {
                    error!("stream input timeout");
                    return Err(ProcessorError::InputStreamTimeout);
                }
                Ok(None) => {
                    error!("stream input closed");
                    return Err(ProcessorError::InputStreamClosed);
                }
                Ok(Some(maybe_message)) => {
                    let (input_id, message) = maybe_message?;
                    self.handle_input_message(input_id, message).await?;
                }
            }
        }
    }

    /// Subscribe to new output messages.
    fn live_stream(&self) -> LiveStream<A::Message> {
        let receiver = self.message_tx.subscribe();
        let inner = BroadcastStream::new(receiver);
        LiveStream { inner }
    }

    async fn init_input_mixer(&self, inputs: &[app_pb::InputStream]) -> Result<InputMixer> {
        // check whether inputs already started streaming, if that's the case start from where it left.
        let mut inputs_with_starting_sequence = Vec::default();
        for input in inputs {
            let stream_id = StreamId::from_u64(input.id);
            let starting_input_sequence = self
                .sequencer
                .input_sequence(&stream_id)?
                .map(|seq| seq.successor())
                .unwrap_or_else(|| Sequence::from_u64(input.starting_sequence));

            info!(stream_id = ?stream_id, url = %input.url, starting_sequence = %starting_input_sequence.as_u64(), "loading input stream config");
            inputs_with_starting_sequence.push(app_pb::InputStream {
                id: stream_id.as_u64(),
                starting_sequence: starting_input_sequence.as_u64(),
                url: input.url.clone(),
            });
        }
        let input_mixer = InputMixer::new_from_pb(inputs_with_starting_sequence);
        Ok(input_mixer)
    }

    async fn handle_input_message(
        &self,
        input_id: u64,
        message: node_pb::StreamMessagesResponse,
    ) -> Result<()> {
        match message.message {
            None => {
                error!(input_id = %input_id, "input message contains no message");
                Err(ProcessorError::InputStreamClosed)
            }
            Some(node_pb::stream_messages_response::Message::Data(data)) => {
                self.handle_data(input_id, data).await
            }
            Some(node_pb::stream_messages_response::Message::Invalidate(invalidate)) => {
                self.handle_invalidate(input_id, invalidate).await
            }
            Some(node_pb::stream_messages_response::Message::Heartbeat(_)) => Ok(()),
            Some(node_pb::stream_messages_response::Message::Pending(_)) => Ok(()),
        }
    }

    async fn handle_data(&self, input_id: u64, data: node_pb::Data) -> Result<()> {
        let input_id = StreamId::from_u64(input_id);
        let sequence = Sequence::from_u64(data.sequence);
        // TODO: should it check the type url?
        let raw_data = data.data.ok_or(ProcessorError::EmptyMessage)?.value;
        let mut application = self.application.lock().await;
        let messages = application
            .receive_data(&input_id, &sequence, &raw_data)
            .await
            .map_err(|err| ProcessorError::Application(Box::new(err)))?;

        let txn = self.db.begin_rw_txn()?;

        // store messages and assign them a sequence number
        let output_sequence =
            self.sequencer
                .register_with_txn(&input_id, &sequence, messages.len(), &txn)?;

        for (index, sequence) in output_sequence.enumerate() {
            self.storage
                .insert_with_txn(&sequence, &messages[index], &txn)?;
            let raw_data = RawMessageData::from_vec(messages[index].encode_to_vec());
            let message = StreamMessage::new_data(sequence, raw_data);
            self.message_tx
                .send(message)
                .map_err(|_| ProcessorError::Broadcast)?;
        }

        txn.commit()?;

        Ok(())
    }

    async fn handle_invalidate(
        &self,
        _input_id: u64,
        _invalidate: node_pb::Invalidate,
    ) -> Result<()> {
        todo!()
    }
}

impl<A, E> MessageProducer for Processor<A, E>
where
    A: Application,
    E: EnvironmentKind,
{
    type Message = A::Message;
    type Error = ProcessorError;
    type Stream = BackfilledMessageStream<
        Self::Message,
        Arc<MdbxMessageStorage<E, A::Message>>,
        LiveStream<A::Message>,
    >;

    fn stream_from_sequence(
        &self,
        starting_sequence: &Sequence,
        pending_interval: Option<Duration>,
        ct: CancellationToken,
    ) -> std::result::Result<Self::Stream, Self::Error> {
        info!(start = ?starting_sequence, "start stream");
        let latest = self.sequencer.next_output_sequence_start()?;
        let latest = if latest.is_zero() {
            latest
        } else {
            latest.predecessor()
        };
        let live = self.live_stream();
        let stream = BackfilledMessageStream::new(
            *starting_sequence,
            latest,
            self.storage.clone(),
            live,
            pending_interval,
            ct,
        );
        Ok(stream)
    }
}

impl<M: MessageData + 'static> Stream for LiveStream<M> {
    type Item = std::result::Result<StreamMessage<M>, Box<dyn std::error::Error>>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Option<Self::Item>> {
        let mut this = self.project();
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(message)) => {
                let res = message.map_err(|err| Box::new(err) as Box<dyn std::error::Error>);
                Poll::Ready(Some(res))
            }
        }
    }
}
