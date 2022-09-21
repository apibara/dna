use crate::{application::{pb, Application}, input::{InputStream, InputStreamError}};

pub struct Node<A: Application> {
    application: A,
}

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("application error")]
    Application(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("error configuring or reading input stream")]
    InputStream(#[from] InputStreamError),
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

    pub async fn start(self) -> Result<()> {
        let init_req = pb::InitRequest {};
        let init_response = self.application
            .init(init_req)
            .await
            .map_err(|err| NodeError::Application(Box::new(err)))?;
        let input_stream = InputStream::new_from_pb(&init_response.inputs)?;
        todo!()
    }
}
