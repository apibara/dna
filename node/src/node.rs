use tokio_stream::StreamExt;

use crate::{
    application::{pb, Application},
    input::{InputMixer, InputMixerError, MixedInputStreamError},
};
use apibara_core::node::pb as node_pb;
use tracing::error;

pub struct Node<A: Application> {
    application: A,
}

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
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

impl<A> Node<A>
where
    A: Application,
{
    /// Start building a new node.
    pub fn with_application(application: A) -> Node<A> {
        Node { application }
    }

    /// Start the node.
    pub async fn start(mut self) -> Result<()> {
        let init_req = pb::InitRequest {};
        let init_response = self
            .application
            .init(init_req)
            .await
            .map_err(|err| NodeError::Application(Box::new(err)))?;

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
        let receive_data_req = pb::ReceiveDataRequest {
            input_id,
            sequence: data.sequence,
            data: data.data,
        };
        let response = self
            .application
            .receive_data(receive_data_req)
            .await
            .map_err(|err| NodeError::Application(Box::new(err)))?;
        let messages = response.data;
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
