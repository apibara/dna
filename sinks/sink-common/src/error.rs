use std::{fmt, process::ExitCode};

use error_stack::{report, Context, Report, Result, ResultExt};

/// Sink error.
///
/// Sink developers should default to returning `SinkError::Temporary` for all errors.
/// `SinkError::Configuration` should be returned for configuration-related errors.
/// `SinkError::Fatal` should only be returned from `Sink::handle_data` and `Sink::handle_invalidate`.
#[derive(Debug)]
pub enum SinkError {
    /// Configuration error. Should not retry.
    Configuration,
    /// Temporary error. Should retry.
    Temporary,
    /// Fatal error. Should not retry.
    Fatal,
    // Status server error
    Status,
    // Persistence error
    Persistence,
}

pub trait ReportExt {
    fn to_exit_code(&self) -> ExitCode;
}

impl error_stack::Context for SinkError {}

impl fmt::Display for SinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // match self {
        //     SinkError::Configuration => f.write_str("sink configuration error"),
        //     SinkError::Temporary => f.write_str("temporary sink error"),
        //     SinkError::Fatal => f.write_str("fatal sink error"),
        //     _ => f.write_str("sink error"),
        // }
        f.write_str("sink error")
    }
}

impl<T> ReportExt for Result<T, SinkError> {
    fn to_exit_code(&self) -> ExitCode {
        match self {
            Ok(_) => ExitCode::SUCCESS,
            Err(err) => {
                eprintln!("{:?}", err);
                // Exit codes based on sysexits.h
                match err.downcast_ref::<SinkError>() {
                    Some(SinkError::Configuration) => ExitCode::from(78),
                    Some(SinkError::Temporary) => ExitCode::from(75),
                    Some(SinkError::Fatal) => ExitCode::FAILURE,
                    Some(_) => ExitCode::FAILURE,
                    None => ExitCode::FAILURE,
                }
            }
        }
    }
}

impl SinkError {
    pub fn status_server_error(reason: &str) -> Report<SinkError> {
        report!(SinkError::Status)
            .attach_printable(format!("status server operation failed: {reason}"))
    }
}

pub trait SinkErrorResultExt {
    type Ok;
    fn status_server_error(self, reason: &str) -> Result<Self::Ok, SinkError>;
    fn persistence_client_error(self, reason: &str) -> Result<Self::Ok, SinkError>;
}

impl<T, C> SinkErrorResultExt for core::result::Result<T, C>
where
    C: Context,
{
    type Ok = T;

    fn status_server_error(self, reason: &str) -> Result<T, SinkError> {
        self.change_context(SinkError::Status)
            .attach_printable(format!("status server operation failed: {reason}"))
    }

    fn persistence_client_error(self, reason: &str) -> Result<T, SinkError> {
        self.change_context(SinkError::Persistence)
            .attach_printable(format!("persistence client operation failed: {reason}"))
    }
}

pub trait SinkErrorReportExt {
    type Ok;
    fn status_server_error(self, reason: &str) -> Report<SinkError>;
}

impl<C> SinkErrorReportExt for Report<C> {
    type Ok = ();

    fn status_server_error(self, reason: &str) -> Report<SinkError> {
        self.change_context(SinkError::Status)
            .attach_printable(format!("status server operation failed: {reason}"))
    }
}
