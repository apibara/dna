use std::sync::Arc;

use libmdbx::{Environment, EnvironmentKind};
use tokio_stream::StreamExt;

use crate::{
    application::Application,
    input::{InputMixer, InputMixerError, MixedInputStreamError},
    message_storage::{MdbxMessageStorage, MdbxMessageStorageError},
    sequencer::{Sequencer, SequencerError},
};
use apibara_core::{
    node::pb as node_pb,
    stream::{Sequence, StreamId},
};
use tracing::error;

pub struct Node<A: Application, E: EnvironmentKind> {
    application: A,
    sequencer: Sequencer<E>,
    message_storage: MdbxMessageStorage<E, A::Message>,
}

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
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
}

pub type Result<T> = std::result::Result<T, NodeError>;

impl<A, E> Node<A, E>
where
    A: Application,
    E: EnvironmentKind,
{
    /// Start building a new node.
    pub fn with_application(db: Arc<Environment<E>>, application: A) -> Result<Node<A, E>> {
        let sequencer = Sequencer::new(db.clone())?;
        let message_storage = MdbxMessageStorage::new(db)?;
        Ok(Node {
            application,
            sequencer,
            message_storage,
        })
    }

    /// Start the node.
    pub async fn start(mut self) -> Result<()> {
        // load configuration from previous run, if it exists.

        // if it doesn't then request it to the application.
        let init_response = self
            .application
            .init()
            .await
            .map_err(|err| NodeError::Application(Box::new(err)))?;
        // now store configuration into storage for the next launch.

        // configure inputs to start from where they left previously.
        let input_mixer = InputMixer::new_from_pb(init_response.inputs);
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
        // let output_sequence = self.sequencer.register(&stream_id, &input_seq, messages.len() + 1)?;
        println!("messages = {:?}", messages);

        Ok(())
    }

    async fn handle_invalidate(
        &mut self,
        _input_id: u64,
        _invalidate: node_pb::Invalidate,
    ) -> Result<()> {
        todo!()
    }
}
