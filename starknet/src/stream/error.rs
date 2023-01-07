/// Error while streaming data.
#[derive(thiserror::Error, Debug)]
pub enum StreamError {
    /// Internal error, should not be sent to the client.
    #[error("internal stream error")]
    Internal(Box<dyn std::error::Error + Send + Sync + 'static>),
    /// Error caused by the client.
    #[error("client error: {message}")]
    Client { message: String },
}

impl StreamError {
    /// Creates a new internal error.
    pub fn internal<E>(err: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        StreamError::Internal(Box::new(err))
    }

    /// Creates a new client error.
    pub fn client(message: impl Into<String>) -> Self {
        StreamError::Client {
            message: message.into(),
        }
    }
}
