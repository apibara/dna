use std::sync::Arc;

use libmdbx::{Environment, EnvironmentKind, Error as MdbxError};
use tokio_stream::StreamExt;

use crate::{
    application::Application,
    db::{tables, MdbxRWTransactionExt, MdbxTransactionExt},
    input::{InputMixer, InputMixerError, MixedInputStreamError},
    message_storage::{MdbxMessageStorage, MdbxMessageStorageError},
    o11y::{init_opentelemetry, OpenTelemetryInitError},
    sequencer::{Sequencer, SequencerError},
};
use apibara_core::{
    application::pb as app_pb,
    node::pb as node_pb,
    stream::{Sequence, StreamId},
};
use tracing::{error, info};

pub struct Node<A: Application, E: EnvironmentKind> {
    application: A,
    db: Arc<Environment<E>>,
    sequencer: Sequencer<E>,
    message_storage: MdbxMessageStorage<E, A::Message>,
}

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("database error")]
    Database(#[from] MdbxError),
    #[error("sequencer error")]
    Sequencer(#[from] SequencerError),
    #[error("message storage error")]
    MessageStorage(#[from] MdbxMessageStorageError),
    #[error("message contains no data")]
    EmptyMessage,
    #[error("application error")]
    Application(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("error configuring input mixer")]
    InputMixer(#[from] InputMixerError),
    #[error("error streaming input")]
    InputStream(#[from] MixedInputStreamError),
    #[error("input stream was closed")]
    InputStreamClosed,
    #[error("error configuring opentelemetry")]
    OpenTelemetry(#[from] OpenTelemetryInitError),
}

pub type Result<T> = std::result::Result<T, NodeError>;

impl<A, E> Node<A, E>
where
    A: Application,
    E: EnvironmentKind,
{
    /// Start building a new node.
    pub fn with_application(db: Arc<Environment<E>>, application: A) -> Result<Node<A, E>> {
        let txn = db.begin_rw_txn()?;
        txn.ensure_table::<tables::InputStreamTable>(None)?;
        txn.commit()?;

        let sequencer = Sequencer::new(db.clone())?;
        let message_storage = MdbxMessageStorage::new(db.clone())?;

        Ok(Node {
            db,
            application,
            sequencer,
            message_storage,
        })
    }

    /// Start the node.
    pub async fn start(mut self) -> Result<()> {
        init_opentelemetry()?;

        let input_mixer = self.init_application().await?;
        let mut input_stream = input_mixer.stream_messages().await?;

        // enter main loop. keep streaming messages from the inputs and pass them to the application.
        loop {
            let (input_id, message) = input_stream
                .try_next()
                .await?
                .ok_or(NodeError::InputStreamClosed)?;
            self.handle_input_message(input_id, message).await?;
        }
    }

    async fn handle_input_message(
        &mut self,
        input_id: u64,
        message: node_pb::StreamMessagesResponse,
    ) -> Result<()> {
        match message.message {
            None => {
                error!(input_id = %input_id, "input message contains no message");
                Err(NodeError::InputStreamClosed)
            }
            Some(node_pb::stream_messages_response::Message::Data(data)) => {
                self.handle_data(input_id, data).await
            }
            Some(node_pb::stream_messages_response::Message::Invalidate(invalidate)) => {
                self.handle_invalidate(input_id, invalidate).await
            }
        }
    }

    async fn handle_data(&mut self, input_id: u64, data: node_pb::Data) -> Result<()> {
        let input_id = StreamId::from_u64(input_id);
        let sequence = Sequence::from_u64(data.sequence);
        // TODO: should it check the type url?
        let raw_data = data.data.ok_or(NodeError::EmptyMessage)?.value;
        let messages = self
            .application
            .receive_data(&input_id, &sequence, &raw_data)
            .await
            .map_err(|err| NodeError::Application(Box::new(err)))?;

        // store messages and assign them a sequence number
        let output_sequence = self
            .sequencer
            .register(&input_id, &sequence, messages.len())?;
        for (index, sequence) in output_sequence.enumerate() {
            self.message_storage.insert(&sequence, &messages[index])?;
        }

        Ok(())
    }

    async fn handle_invalidate(
        &mut self,
        _input_id: u64,
        _invalidate: node_pb::Invalidate,
    ) -> Result<()> {
        todo!()
    }

    async fn init_application(&mut self) -> Result<InputMixer> {
        let txn = self.db.begin_rw_txn()?;
        let mut input_streams_cursor = txn.open_table::<tables::InputStreamTable>()?.cursor()?;
        let mut maybe_input_stream = input_streams_cursor.first()?;
        if maybe_input_stream.is_none() {
            info!("first start. loading config from app");
            // first time starting the application. retrieve config from it.
            let init_response = self
                .application
                .init()
                .await
                .map_err(|err| NodeError::Application(Box::new(err)))?;

            // now store configuration into storage for the next launch.
            for input_stream in &init_response.inputs {
                let stream_id = StreamId::from_u64(input_stream.id);
                info!(stream_id = ?stream_id, url = %input_stream.url, starting_sequence = %input_stream.starting_sequence, "store input stream config");
                input_streams_cursor.put(&stream_id, input_stream)?;
            }
            txn.commit()?;

            let input_mixer = InputMixer::new_from_pb(init_response.inputs);
            return Ok(input_mixer);
        }

        let mut inputs = Vec::default();
        while let Some((stream_id, input_stream)) = maybe_input_stream {
            // check whether the input already started streaming, if that's the case start from where it left.
            let starting_input_sequence = self
                .sequencer
                .input_sequence(&stream_id)?
                .map(|seq| seq.successor())
                .unwrap_or_else(|| Sequence::from_u64(input_stream.starting_sequence));

            info!(stream_id = ?stream_id, url = %input_stream.url, starting_sequence = %starting_input_sequence.as_u64(), "loading input stream config");
            inputs.push(app_pb::InputStream {
                id: stream_id.as_u64(),
                starting_sequence: starting_input_sequence.as_u64(),
                url: input_stream.url,
            });
            maybe_input_stream = input_streams_cursor.next()?;
        }

        // configure inputs to start from where they left previously.
        let input_mixer = InputMixer::new_from_pb(inputs);
        Ok(input_mixer)
    }
}
