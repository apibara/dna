use error_stack::{report, Context, Report, ResultExt};
use std::fmt;

#[derive(Debug)]
pub enum RunnerError {
    Internal(String),
    MissingArgument(String),
    InvalidArgument(String, String),
    NotFound(String),
    AlreadyExists(String),
}
impl RunnerError {
    pub fn internal(message: &str) -> Report<RunnerError> {
        report!(RunnerError::Internal(message.to_string())).attach_printable(message.to_string())
    }
    pub fn missing_argument(argument: &str) -> Report<RunnerError> {
        report!(RunnerError::MissingArgument(argument.to_string()))
            .attach_printable(format!("missing required argument: {argument}"))
    }
    pub fn invalid_argument(argument: &str, reason: &str) -> Report<RunnerError> {
        report!(RunnerError::InvalidArgument(
            argument.to_string(),
            reason.to_string()
        ))
        .attach_printable(format!("invalid argument {argument}: {reason}"))
    }
    pub fn not_found(name: &str) -> Report<RunnerError> {
        report!(RunnerError::NotFound(name.to_string()))
            .attach_printable(format!("{} not found", name))
    }
    pub fn already_exists(name: &str) -> Report<RunnerError> {
        report!(RunnerError::AlreadyExists(name.to_string()))
            .attach_printable(format!("{} already exists", name))
    }

    pub fn to_tonic_status(&self) -> tonic::Status {
        match self {
            // TODO: include the internal error message in the tonic message but only for local runner
            RunnerError::Internal(_message) => tonic::Status::internal("internal error"),
            RunnerError::MissingArgument(argument) => {
                tonic::Status::invalid_argument(format!("missing required argument: {argument}"))
            }
            RunnerError::InvalidArgument(argument, reason) => {
                tonic::Status::invalid_argument(format!("invalid argument {argument}: {reason}"))
            }
            RunnerError::NotFound(name) => tonic::Status::not_found(format!("{} not found", name)),
            RunnerError::AlreadyExists(name) => {
                tonic::Status::already_exists(format!("{} already exists", name))
            }
        }
    }
}
impl Context for RunnerError {}
impl fmt::Display for RunnerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("local runner failed")
    }
}

pub type RunnerResult<T> = error_stack::Result<T, RunnerError>;

pub trait RunnerResultExt {
    type Ok;
    fn internal(self, message: &str) -> RunnerResult<Self::Ok>;
    fn missing_argument(self, argument: &str) -> RunnerResult<Self::Ok>;
    fn invalid_argument(self, argument: &str, reason: &str) -> RunnerResult<Self::Ok>;
    fn not_found(self, name: &str) -> RunnerResult<Self::Ok>;
    fn already_exists(self, name: &str) -> RunnerResult<Self::Ok>;
}

pub trait RunnerReportExt {
    fn internal(self, message: &str) -> Report<RunnerError>;
}

impl<T, C> RunnerResultExt for core::result::Result<T, C>
where
    C: Context,
{
    type Ok = T;

    fn internal(self, message: &str) -> RunnerResult<T> {
        self.change_context(RunnerError::Internal(message.to_string()))
            .attach_printable(message.to_string())
    }

    fn missing_argument(self, argument: &str) -> RunnerResult<T> {
        self.change_context(RunnerError::MissingArgument(argument.to_string()))
            .attach_printable(format!("missing required argument {argument}"))
    }

    fn invalid_argument(self, argument: &str, reason: &str) -> RunnerResult<T> {
        self.change_context(RunnerError::InvalidArgument(
            argument.to_string(),
            reason.to_string(),
        ))
        .attach_printable(format!("invalid argument {argument}: {reason}"))
    }

    fn not_found(self, name: &str) -> RunnerResult<T> {
        self.change_context(RunnerError::NotFound(name.to_string()))
            .attach_printable(format!("{name} not found"))
    }

    fn already_exists(self, name: &str) -> RunnerResult<T> {
        self.change_context(RunnerError::AlreadyExists(name.to_string()))
            .attach_printable(format!("{name} already exists"))
    }
}

impl<C> RunnerReportExt for Report<C> {
    fn internal(self, message: &str) -> Report<RunnerError> {
        self.change_context(RunnerError::Internal(message.to_string()))
            .attach_printable(message.to_string())
    }
}
