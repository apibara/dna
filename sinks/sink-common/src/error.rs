use std::{fmt, process::ExitCode};

use error_stack::{report, Context, Report, Result, ResultExt};

/// Sink error.
///
/// Sink developers should default to returning `SinkError::Temporary` for all errors.
/// `SinkError::Configuration` should be returned for configuration-related errors.
/// `SinkError::Fatal` should only be returned from `Sink::handle_data` and `Sink::handle_invalidate`.
#[derive(Debug)]
pub enum SinkConnectorError {
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

impl error_stack::Context for SinkConnectorError {}

impl fmt::Display for SinkConnectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // match self {
        //     SinkConnectorError::Configuration => f.write_str("sink configuration error"),
        //     SinkConnectorError::Temporary => f.write_str("temporary sink error"),
        //     SinkConnectorError::Fatal => f.write_str("fatal sink error"),
        //     _ => f.write_str("sink error"),
        // }
        f.write_str("sink error")
    }
}

impl<T> ReportExt for Result<T, SinkConnectorError> {
    fn to_exit_code(&self) -> ExitCode {
        match self {
            Ok(_) => ExitCode::SUCCESS,
            Err(err) => {
                eprintln!("{:?}", err);
                // Exit codes based on sysexits.h
                match err.downcast_ref::<SinkConnectorError>() {
                    Some(SinkConnectorError::Configuration) => ExitCode::from(78),
                    Some(SinkConnectorError::Temporary) => ExitCode::from(75),
                    Some(SinkConnectorError::Fatal) => ExitCode::FAILURE,
                    Some(_) => ExitCode::FAILURE,
                    None => ExitCode::FAILURE,
                }
            }
        }
    }
}

impl SinkConnectorError {
    pub fn status_server_error(reason: &str) -> Report<SinkConnectorError> {
        report!(SinkConnectorError::Status)
            .attach_printable(format!("status server operation failed: {reason}"))
    }
}

pub trait SinkConnectorErrorResultExt {
    type Ok;
    fn status_server_error(self, reason: &str) -> Result<Self::Ok, SinkConnectorError>;
    fn persistence_client_error(self, reason: &str) -> Result<Self::Ok, SinkConnectorError>;
}

impl<T, C> SinkConnectorErrorResultExt for core::result::Result<T, C>
where
    C: Context,
{
    type Ok = T;

    fn status_server_error(self, reason: &str) -> Result<T, SinkConnectorError> {
        self.change_context(SinkConnectorError::Status)
            .attach_printable(format!("status server operation failed: {reason}"))
    }

    fn persistence_client_error(self, reason: &str) -> Result<T, SinkConnectorError> {
        self.change_context(SinkConnectorError::Persistence)
            .attach_printable(format!("persistence client operation failed: {reason}"))
    }
}

pub trait SinkConnectorErrorReportExt {
    type Ok;
    fn status_server_error(self, reason: &str) -> Report<SinkConnectorError>;
}

impl<C> SinkConnectorErrorReportExt for Report<C> {
    type Ok = ();

    fn status_server_error(self, reason: &str) -> Report<SinkConnectorError> {
        self.change_context(SinkConnectorError::Status)
            .attach_printable(format!("status server operation failed: {reason}"))
    }
}
