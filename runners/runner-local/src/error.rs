use error_stack::{report, Context, Report, ResultExt};
use std::fmt;

#[derive(Debug)]
pub enum LocalRunnerError {
    Internal(String),
    MissingArgument(String),
    InvalidArgument { argument: String, reason: String },
    NotFound(String),
    AlreadyExists(String),
}
impl LocalRunnerError {
    pub fn internal(message: &str) -> Report<LocalRunnerError> {
        report!(LocalRunnerError::Internal(message.to_string()))
            .attach_printable(message.to_string())
    }
    pub fn missing_argument(argument: &str) -> LocalRunnerError {
        LocalRunnerError::MissingArgument(argument.to_string())
    }
    pub fn invalid_argument(argument: &str, reason: &str) -> LocalRunnerError {
        LocalRunnerError::InvalidArgument {
            argument: argument.to_string(),
            reason: reason.to_string(),
        }
    }
    pub fn not_found(name: &str) -> LocalRunnerError {
        LocalRunnerError::NotFound(name.to_string())
    }
    pub fn already_exists(name: &str) -> LocalRunnerError {
        LocalRunnerError::AlreadyExists(name.to_string())
    }

    pub fn to_tonic_status(&self) -> tonic::Status {
        match self {
            // TODO: include the internal error message in the tonic message but only for local runner
            LocalRunnerError::Internal(_message) => tonic::Status::internal("internal error"),
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

pub trait LocalRunnerResultExt {
    type Ok;
    fn internal(self, message: &str) -> LocalRunnerResult<Self::Ok>;
    fn missing_argument(self, argument: &str) -> LocalRunnerResult<Self::Ok>;
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
}

// We can't do this, so functions that already return a Result<T, Report<C>> can't use the LocalRunnerResultExt trait
// For those, we have to use map_err instead.
// Example: `init_opentelemetry().map_err(|err| err.internal("failed to initialize opentelemetry"))?;``
//
// conflicting implementations of trait `LocalRunnerResultExt` for type `Result<_, error_stack::Report<_>>`
// upstream crates may add a new impl of trait `std::error::Error` for type `error_stack::Report<_>` in future versions
// impl<T, C> LocalRunnerResultExt for core::result::Result<T, Report<C>>
// where
//     C: Context,
// {
//     type Ok = T;

//     fn internal(self, message: &str) -> LocalRunnerResult<T> {
//         self.change_context(LocalRunnerError::Internal(message.to_string()))
//             .attach_printable(message.to_string())
//     }

//     fn missing_argument(self, argument: &str) -> LocalRunnerResult<T> {
//         self.change_context(LocalRunnerError::MissingArgument(argument.to_string()))
//             .attach_printable(format!("missing required argument {argument}"))
//     }
// }

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
