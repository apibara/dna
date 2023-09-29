use tracing::warn;

#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    #[error("internal error: {0}")]
    Internal(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("quota exceeded")]
    QuotaExceeded,
    #[error("invalid request: {message}")]
    InvalidRequest { message: String },
}

impl StreamError {
    pub fn invalid_request(message: String) -> Self {
        StreamError::InvalidRequest { message }
    }

    pub fn quota_exceeded() -> Self {
        StreamError::QuotaExceeded
    }

    pub fn internal(err: impl Into<Box<dyn std::error::Error + Send + Sync + 'static>>) -> Self {
        StreamError::Internal(err.into())
    }

    pub fn into_status(self) -> tonic::Status {
        match self {
            StreamError::Internal(err) => {
                warn!(err = ?err, "stream error");
                tonic::Status::internal("internal server error")
            }
            StreamError::QuotaExceeded => tonic::Status::resource_exhausted(
                "monthly data quota exceeded. Please contact support.",
            ),
            StreamError::InvalidRequest { message } => tonic::Status::invalid_argument(message),
        }
    }
}
