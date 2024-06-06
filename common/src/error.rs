use std::{fmt, process::ExitCode};

#[derive(Debug)]
pub enum DnaError {
    /// Fatal error. Should not retry.
    Fatal,
    /// Configuration error. Should not retry.
    Configuration,
    /// Io error. Can retry.
    Io,
    /// Storage error. Should not retry.
    Storage,
}

pub type Result<T> = error_stack::Result<T, DnaError>;

impl error_stack::Context for DnaError {}

impl fmt::Display for DnaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DnaError::Fatal => f.write_str("dna error: fatal"),
            DnaError::Configuration => f.write_str("dna error: configuration"),
            DnaError::Io => f.write_str("dna error: io"),
            DnaError::Storage => f.write_str("dna error: storage"),
        }
    }
}

pub trait ReportExt {
    fn to_exit_code(&self) -> ExitCode;
}

impl<T> ReportExt for Result<T> {
    fn to_exit_code(&self) -> ExitCode {
        match self {
            Ok(_) => ExitCode::SUCCESS,
            Err(err) => {
                eprintln!("{:?}", err);
                // Exit codes based on sysexits.h
                match err.downcast_ref::<DnaError>() {
                    Some(_) => ExitCode::FAILURE,
                    None => ExitCode::FAILURE,
                }
            }
        }
    }
}
