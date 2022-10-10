//! Input stream processor.

use std::{marker::PhantomData, pin::Pin, sync::Arc};

use apibara_core::{
    application::pb as app_pb,
    node::pb as node_pb,
    stream::{MessageData, RawMessageData, Sequence, StreamId, StreamMessage},
};
use futures::{channel::mpsc::SendError, Stream};
use libmdbx::{Environment, EnvironmentKind, Error as MdbxError};
use prost::Message;
use tokio::sync::{broadcast, Mutex};
use tokio_stream::{wrappers::BroadcastStream, StreamExt};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::{
    application::Application,
    db::{tables, MdbxRWTransactionExt, MdbxTransactionExt},
    input::{InputMixer, InputMixerError, MixedInputStreamError},
    message_storage::{MdbxMessageStorage, MdbxMessageStorageError, MessageStorage},
    message_stream::{self, BackfilledMessageStream, BackfilledMessageStreamError, LiveStreamItem},
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
    configuration: ProcessorConfiguration<E>,
    message_tx: broadcast::Sender<ProcessorMessage<A::Message>>,
    _message_rx: broadcast::Receiver<ProcessorMessage<A::Message>>,
}

/// Holds the [Processor] configuration.
pub struct ProcessorConfiguration<E: EnvironmentKind> {
    db: Arc<Environment<E>>,
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
}

pub type Result<T> = std::result::Result<T, ProcessorError>;

const MESSAGE_CHANNEL_SIZE: usize = 128;

pub struct LiveStream<M: MessageData> {
    inner: BroadcastStream<StreamMessage<M>>,
}

impl<A, E> Processor<A, E>
where
    A: Application + Send,
    E: EnvironmentKind,
{
    /// Creates a new stream processor with the given application.
    pub fn new(db: Arc<Environment<E>>, application: A) -> Result<Self> {
        let txn = db.begin_rw_txn()?;
        txn.ensure_table::<tables::InputStreamTable>(None)?;
        txn.commit()?;

        let sequencer = Sequencer::new(db.clone())?;
        let storage = MdbxMessageStorage::new(db.clone())?;
        let configuration = ProcessorConfiguration::new(db)?;

        let (message_tx, message_rx) = broadcast::channel(MESSAGE_CHANNEL_SIZE);

        Ok(Processor {
            application: Mutex::new(application),
            db,
            sequencer,
            storage: Arc::new(storage),
            configuration,
            message_tx,
            _message_rx: message_rx,
        })
    }

    /// Starts the input stream processor.
    ///
    /// The processor will start indexing messages from its inputs,
    /// generating a new sequence of output messages.
    pub async fn start(&self, ct: CancellationToken) -> Result<()> {
        let input_mixer = self.init_application().await?;
        let mut input_stream = input_mixer.stream_messages().await?;

        // enter main loop. keep streaming messages from the inputs and pass them to the application.
        loop {
            if ct.is_cancelled() {
                return Ok(());
            }

            let (input_id, message) = input_stream
                .try_next()
                .await?
                .ok_or(ProcessorError::InputStreamClosed)?;
            self.handle_input_message(input_id, message).await?;
        }
    }

    /// Subscribe to new output messages.
    fn live_stream(&self) -> LiveStream<A::Message> {
        let receiver = self.message_tx.subscribe();
        let inner = BroadcastStream::new(receiver);
        //    .map(|maybe_val| maybe_val.map_err(|err| Box::new(err) as Box<dyn std::error::Error>))
        LiveStream { inner }
    }

    async fn init_application(&self) -> Result<InputMixer> {
        let input_streams = match self.configuration.input_streams()? {
            None => {
                info!("first start. loading config from app");
                let mut application = self.application.lock().await;
                // first time starting the application. retrieve config from it.
                let init_response = application
                    .init()
                    .await
                    .map_err(|err| ProcessorError::Application(Box::new(err)))?;

                self.configuration
                    .set_input_streams(&init_response.inputs)?;

                init_response.inputs
            }
            Some(inputs) => {
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
                        url: input.url,
                    });
                }
                inputs_with_starting_sequence
            }
        };
        let input_mixer = InputMixer::new_from_pb(input_streams);
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
        ct: CancellationToken,
    ) -> std::result::Result<Self::Stream, Self::Error> {
        info!(start = ?starting_sequence, "start stream");
        let latest = self.sequencer.next_output_sequence_start()?;
        let live = self.live_stream();
        let stream = BackfilledMessageStream::new(
            *starting_sequence,
            latest,
            self.storage.clone(),
            live,
            ct,
        );
        Ok(stream)
    }
}

impl<M: MessageData> Stream for LiveStream<M> {
    type Item = std::result::Result<StreamMessage<M>, Box<dyn std::error::Error>>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        todo!()
    }
}

impl<E: EnvironmentKind> ProcessorConfiguration<E> {
    /// Creates a new configuration.
    pub fn new(db: Arc<Environment<E>>) -> Result<Self> {
        let txn = db.begin_rw_txn()?;
        txn.ensure_table::<tables::InputStreamTable>(None)?;
        txn.commit()?;
        Ok(ProcessorConfiguration { db })
    }

    /// Returns a list of the processor input streams.
    ///
    /// Returns [None] if no configuration is stored for the processor.
    pub fn input_streams(&self) -> Result<Option<Vec<app_pb::InputStream>>> {
        let txn = self.db.begin_ro_txn()?;
        let mut inputs_cursor = txn.open_table::<tables::InputStreamTable>()?.cursor()?;
        let mut maybe_input_stream = inputs_cursor.first()?;

        if maybe_input_stream.is_none() {
            txn.commit()?;
            return Ok(None);
        }

        let mut inputs = Vec::default();
        while let Some((_, input_stream)) = maybe_input_stream {
            inputs.push(input_stream);
            maybe_input_stream = inputs_cursor.next()?;
        }
        txn.commit()?;

        Ok(Some(inputs))
    }

    /// Stores the given input streams for later runs.
    pub fn set_input_streams(&self, inputs: &[app_pb::InputStream]) -> Result<()> {
        let txn = self.db.begin_rw_txn()?;
        let mut inputs_cursor = txn.open_table::<tables::InputStreamTable>()?.cursor()?;
        for input in inputs {
            let stream_id = StreamId::from_u64(input.id);
            inputs_cursor.seek_exact(&stream_id)?;
            inputs_cursor.put(&stream_id, input)?;
        }
        txn.commit()?;
        Ok(())
    }
}
