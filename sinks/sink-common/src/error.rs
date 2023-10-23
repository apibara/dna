use std::{fmt, process::ExitCode};

use error_stack::Result;

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
}

pub trait ReportExt {
    fn to_exit_code(&self) -> ExitCode;
}

impl error_stack::Context for SinkConnectorError {}

impl fmt::Display for SinkConnectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SinkConnectorError::Configuration => f.write_str("sink configuration error"),
            SinkConnectorError::Temporary => f.write_str("temporary sink error"),
            SinkConnectorError::Fatal => f.write_str("fatal sink error"),
        }
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
                    None => ExitCode::FAILURE,
                    Some(SinkConnectorError::Configuration) => ExitCode::from(78),
                    Some(SinkConnectorError::Temporary) => ExitCode::from(75),
                    Some(SinkConnectorError::Fatal) => ExitCode::FAILURE,
                }
            }
        }
    }
}
