//! Input stream processor.

use std::{marker::PhantomData, pin::Pin, sync::Arc};

use apibara_core::{
    application::pb as app_pb,
    node::pb as node_pb,
    stream::{Sequence, StreamId},
};
use libmdbx::{Environment, EnvironmentKind, Error as MdbxError};
use tokio::sync::Mutex;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::{
    application::Application,
    db::{tables, MdbxRWTransactionExt, MdbxTransactionExt},
    input::{InputMixer, InputMixerError, MixedInputStreamError},
    message_storage::{MdbxMessageStorage, MdbxMessageStorageError},
    sequencer::{Sequencer, SequencerError},
};

/// Service to processor messages from the input stream.
pub struct Processor<A: Application + Send, E: EnvironmentKind> {
    application: Mutex<A>,
    sequencer: Sequencer<E>,
    storage: MdbxMessageStorage<E, A::Message>,
    configuration: ProcessorConfiguration<E>,
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
    #[error("error locking application")]
    ApplicationLock,
    #[error("message contains no data")]
    EmptyMessage,
    #[error("input stream was closed")]
    InputStreamClosed,
}

pub type Result<T> = std::result::Result<T, ProcessorError>;

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

        Ok(Processor {
            application: Mutex::new(application),
            db,
            sequencer,
            storage,
            configuration,
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
            self.storage.insert(&sequence, &messages[index])?;
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
