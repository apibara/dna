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
    // Load script error
    LoadScript,
    // Runtime error
    Runtime,
}

pub trait ReportExt {
    fn to_exit_code(&self) -> ExitCode;
}

impl error_stack::Context for SinkError {}

impl fmt::Display for SinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SinkError::Configuration => f.write_str("sink configuration error"),
            SinkError::Temporary => f.write_str("temporary sink error"),
            SinkError::Fatal => f.write_str("fatal sink error"),
            SinkError::Persistence => f.write_str("persistence client operation failed"),
            SinkError::Status => f.write_str("status server operation failed"),
            SinkError::LoadScript => f.write_str("load script failed"),
            SinkError::Runtime => f.write_str("runtime error"),
        }
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
    pub fn configuration(reason: &str) -> Report<SinkError> {
        report!(SinkError::Configuration).attach_printable(reason.to_string())
    }
    pub fn temporary(reason: &str) -> Report<SinkError> {
        report!(SinkError::Temporary).attach_printable(reason.to_string())
    }
    pub fn fatal(reason: &str) -> Report<SinkError> {
        report!(SinkError::Fatal).attach_printable(reason.to_string())
    }
    pub fn status(reason: &str) -> Report<SinkError> {
        report!(SinkError::Status).attach_printable(reason.to_string())
    }
    pub fn load_script(reason: &str) -> Report<SinkError> {
        report!(SinkError::LoadScript).attach_printable(reason.to_string())
    }
    pub fn runtime_error(reason: &str) -> Report<SinkError> {
        report!(SinkError::Runtime).attach_printable(reason.to_string())
    }
}

pub trait SinkErrorResultExt {
    type Ok;
    fn configuration(self, reason: &str) -> Result<Self::Ok, SinkError>;
    fn temporary(self, reason: &str) -> Result<Self::Ok, SinkError>;
    fn fatal(self, reason: &str) -> Result<Self::Ok, SinkError>;
    fn status(self, reason: &str) -> Result<Self::Ok, SinkError>;
    fn persistence(self, reason: &str) -> Result<Self::Ok, SinkError>;
    fn load_script(self, reason: &str) -> Result<Self::Ok, SinkError>;
    fn runtime_error(self, reason: &str) -> Result<Self::Ok, SinkError>;
}

impl<T, C> SinkErrorResultExt for core::result::Result<T, C>
where
    C: Context,
{
    type Ok = T;

    fn configuration(self, reason: &str) -> Result<T, SinkError> {
        self.change_context(SinkError::Configuration)
            .attach_printable(reason.to_string())
    }

    fn temporary(self, reason: &str) -> Result<T, SinkError> {
        self.change_context(SinkError::Temporary)
            .attach_printable(reason.to_string())
    }

    fn fatal(self, reason: &str) -> Result<T, SinkError> {
        self.change_context(SinkError::Fatal)
            .attach_printable(reason.to_string())
    }

    fn status(self, reason: &str) -> Result<T, SinkError> {
        self.change_context(SinkError::Status)
            .attach_printable(reason.to_string())
    }

    fn persistence(self, reason: &str) -> Result<T, SinkError> {
        self.change_context(SinkError::Persistence)
            .attach_printable(reason.to_string())
    }

    fn load_script(self, reason: &str) -> Result<T, SinkError> {
        self.change_context(SinkError::LoadScript)
            .attach_printable(reason.to_string())
    }

    fn runtime_error(self, reason: &str) -> Result<T, SinkError> {
        self.change_context(SinkError::Runtime)
            .attach_printable(reason.to_string())
    }
}

pub trait SinkErrorReportExt {
    type Ok;
    fn configuration(self, reason: &str) -> Report<SinkError>;
    fn temporary(self, reason: &str) -> Report<SinkError>;
    fn fatal(self, reason: &str) -> Report<SinkError>;
    fn status(self, reason: &str) -> Report<SinkError>;
    fn load_script(self, reason: &str) -> Report<SinkError>;
    fn runtime_error(self, reason: &str) -> Report<SinkError>;
}

// Implement SinkErrorReportExt for Option<C>
impl<T> SinkErrorResultExt for Option<T> {
    type Ok = T;

    fn configuration(self, reason: &str) -> Result<T, SinkError> {
        self.ok_or(SinkError::Configuration)
            .attach_printable(reason.to_string())
    }

    fn temporary(self, reason: &str) -> Result<T, SinkError> {
        self.ok_or(SinkError::Temporary)
            .attach_printable(reason.to_string())
    }

    fn fatal(self, reason: &str) -> Result<T, SinkError> {
        self.ok_or(SinkError::Fatal)
            .attach_printable(reason.to_string())
    }

    fn status(self, reason: &str) -> Result<T, SinkError> {
        self.ok_or(SinkError::Status)
            .attach_printable(reason.to_string())
    }

    fn load_script(self, reason: &str) -> Result<T, SinkError> {
        self.ok_or(SinkError::LoadScript)
            .attach_printable(reason.to_string())
    }

    fn runtime_error(self, reason: &str) -> Result<T, SinkError> {
        self.ok_or(SinkError::Runtime)
            .attach_printable(reason.to_string())
    }

    fn persistence(self, reason: &str) -> Result<Self::Ok, SinkError> {
        self.ok_or(SinkError::Persistence)
            .attach_printable(reason.to_string())
    }
}

impl<C> SinkErrorReportExt for Report<C> {
    type Ok = ();

    fn configuration(self, reason: &str) -> Report<SinkError> {
        self.change_context(SinkError::Configuration)
            .attach_printable(reason.to_string())
    }

    fn temporary(self, reason: &str) -> Report<SinkError> {
        self.change_context(SinkError::Temporary)
            .attach_printable(reason.to_string())
    }

    fn fatal(self, reason: &str) -> Report<SinkError> {
        self.change_context(SinkError::Fatal)
            .attach_printable(reason.to_string())
    }

    fn status(self, reason: &str) -> Report<SinkError> {
        self.change_context(SinkError::Status)
            .attach_printable(reason.to_string())
    }

    fn load_script(self, reason: &str) -> Report<SinkError> {
        self.change_context(SinkError::LoadScript)
            .attach_printable(reason.to_string())
    }

    fn runtime_error(self, reason: &str) -> Report<SinkError> {
        self.change_context(SinkError::Runtime)
            .attach_printable(reason.to_string())
    }
}
