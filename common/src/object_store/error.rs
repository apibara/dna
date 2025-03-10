use aws_sdk_s3::{config::http::HttpResponse, error::SdkError};
use error_stack::{Context, Report, Result, ResultExt};

#[derive(Debug)]
pub enum ObjectStoreError {
    /// Precondition failed.
    Precondition,
    /// Not modified.
    NotModified,
    /// Not found.
    NotFound,
    /// Request error.
    Request,
    /// Metadata is missing.
    Metadata,
    /// Checksum mismatch.
    ChecksumMismatch,
}

impl Context for ObjectStoreError {}

impl std::fmt::Display for ObjectStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ObjectStoreError::Precondition => write!(f, "precondition failed"),
            ObjectStoreError::NotModified => write!(f, "not modified"),
            ObjectStoreError::NotFound => write!(f, "not found"),
            ObjectStoreError::Request => write!(f, "request error"),
            ObjectStoreError::Metadata => write!(f, "metadata error"),
            ObjectStoreError::ChecksumMismatch => write!(f, "checksum mismatch"),
        }
    }
}

pub trait ObjectStoreResultExt {
    fn is_precondition(&self) -> bool;
    fn is_not_modified(&self) -> bool;
    fn is_not_found(&self) -> bool;
}

impl ObjectStoreResultExt for Report<ObjectStoreError> {
    fn is_precondition(&self) -> bool {
        matches!(self.current_context(), ObjectStoreError::Precondition)
    }

    fn is_not_modified(&self) -> bool {
        matches!(self.current_context(), ObjectStoreError::NotModified)
    }

    fn is_not_found(&self) -> bool {
        matches!(self.current_context(), ObjectStoreError::NotFound)
    }
}

pub trait ToObjectStoreResult: Sized {
    type Ok;

    fn change_to_object_store_context(self) -> Result<Self::Ok, ObjectStoreError>;
}

impl<T, E> ToObjectStoreResult for std::result::Result<T, SdkError<E, HttpResponse>>
where
    SdkError<E, HttpResponse>: error_stack::Context,
{
    type Ok = T;

    fn change_to_object_store_context(self) -> Result<T, ObjectStoreError> {
        match self {
            Ok(value) => Ok(value),
            Err(err) => match err.raw_response().map(|r| r.status().as_u16()) {
                Some(412) => Err(err).change_context(ObjectStoreError::Precondition),
                Some(304) => Err(err).change_context(ObjectStoreError::NotModified),
                Some(404) => Err(err).change_context(ObjectStoreError::NotFound),
                _ => Err(err).change_context(ObjectStoreError::Request),
            },
        }
    }
}
