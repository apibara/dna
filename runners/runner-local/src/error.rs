use error_stack::Context;
use std::fmt;

#[derive(Debug)]
pub enum LocalRunnerError {
    Internal,
    MissingArgument(String),
    InvalidArgument { argument: String, reason: String },
    NotFound(String),
    AlreadyExists(String),
}
impl LocalRunnerError {
    pub fn to_tonic_status(&self) -> tonic::Status {
        match self {
            LocalRunnerError::Internal => tonic::Status::internal("internal error"),
            LocalRunnerError::MissingArgument(argument) => {
                tonic::Status::invalid_argument(format!("missing required argument: {argument}"))
            }
            LocalRunnerError::InvalidArgument { argument, reason } => {
                tonic::Status::invalid_argument(format!("invalid argument {argument}: {reason}"))
            }
            LocalRunnerError::NotFound(name) => {
                tonic::Status::not_found(format!("{} not found", name))
            }
            LocalRunnerError::AlreadyExists(name) => {
                tonic::Status::already_exists(format!("{} already exists", name))
            }
        }
    }
}
impl Context for LocalRunnerError {}
impl fmt::Display for LocalRunnerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("local runner failed")
    }
}

pub type LocalRunnerResult<T> = error_stack::Result<T, LocalRunnerError>;
