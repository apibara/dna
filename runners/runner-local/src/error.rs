use error_stack::{report, Context, Report, ResultExt};
use std::fmt;

#[derive(Debug)]
pub enum LocalRunnerError {
    Internal(String),
    MissingArgument(String),
    InvalidArgument(String, String),
    NotFound(String),
    AlreadyExists(String),
}
impl LocalRunnerError {
    pub fn internal(message: &str) -> Report<LocalRunnerError> {
        report!(LocalRunnerError::Internal(message.to_string()))
            .attach_printable(message.to_string())
    }
    pub fn missing_argument(argument: &str) -> Report<LocalRunnerError> {
        report!(LocalRunnerError::MissingArgument(argument.to_string()))
            .attach_printable(format!("missing required argument: {argument}"))
    }
    pub fn invalid_argument(argument: &str, reason: &str) -> Report<LocalRunnerError> {
        report!(LocalRunnerError::InvalidArgument(
            argument.to_string(),
            reason.to_string()
        ))
        .attach_printable(format!("invalid argument {argument}: {reason}"))
    }
    pub fn not_found(name: &str) -> Report<LocalRunnerError> {
        report!(LocalRunnerError::NotFound(name.to_string()))
            .attach_printable(format!("{} not found", name))
    }
    pub fn already_exists(name: &str) -> Report<LocalRunnerError> {
        report!(LocalRunnerError::AlreadyExists(name.to_string()))
            .attach_printable(format!("{} already exists", name))
    }

    pub fn to_tonic_status(&self) -> tonic::Status {
        match self {
            // TODO: include the internal error message in the tonic message but only for local runner
            LocalRunnerError::Internal(_message) => tonic::Status::internal("internal error"),
            LocalRunnerError::MissingArgument(argument) => {
                tonic::Status::invalid_argument(format!("missing required argument: {argument}"))
            }
            LocalRunnerError::InvalidArgument(argument, reason) => {
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

pub trait LocalRunnerResultExt {
    type Ok;
    fn internal(self, message: &str) -> LocalRunnerResult<Self::Ok>;
    fn missing_argument(self, argument: &str) -> LocalRunnerResult<Self::Ok>;
    fn invalid_argument(self, argument: &str, reason: &str) -> LocalRunnerResult<Self::Ok>;
    fn not_found(self, name: &str) -> LocalRunnerResult<Self::Ok>;
    fn already_exists(self, name: &str) -> LocalRunnerResult<Self::Ok>;
}

pub trait LocalRunnerReportExt {
    fn internal(self, message: &str) -> Report<LocalRunnerError>;
    fn missing_argument(self, argument: &str) -> Report<LocalRunnerError>;
}

impl<T, C> LocalRunnerResultExt for core::result::Result<T, C>
where
    C: Context,
{
    type Ok = T;

    fn internal(self, message: &str) -> LocalRunnerResult<T> {
        self.change_context(LocalRunnerError::Internal(message.to_string()))
            .attach_printable(message.to_string())
    }

    fn missing_argument(self, argument: &str) -> LocalRunnerResult<T> {
        self.change_context(LocalRunnerError::MissingArgument(argument.to_string()))
            .attach_printable(format!("missing required argument {argument}"))
    }

    fn invalid_argument(self, argument: &str, reason: &str) -> LocalRunnerResult<T> {
        self.change_context(LocalRunnerError::InvalidArgument(
            argument.to_string(),
            reason.to_string(),
        ))
        .attach_printable(format!("invalid argument {argument}: {reason}"))
    }

    fn not_found(self, name: &str) -> LocalRunnerResult<T> {
        self.change_context(LocalRunnerError::NotFound(name.to_string()))
            .attach_printable(format!("{name} not found"))
    }

    fn already_exists(self, name: &str) -> LocalRunnerResult<T> {
        self.change_context(LocalRunnerError::AlreadyExists(name.to_string()))
            .attach_printable(format!("{name} already exists"))
    }
}

impl<C> LocalRunnerReportExt for Report<C> {
    fn internal(self, message: &str) -> Report<LocalRunnerError> {
        self.change_context(LocalRunnerError::Internal(message.to_string()))
            .attach_printable(message.to_string())
    }

    fn missing_argument(self, argument: &str) -> Report<LocalRunnerError> {
        self.change_context(LocalRunnerError::MissingArgument(argument.to_string()))
            .attach_printable(format!("missing required argument {argument}"))
    }
}
