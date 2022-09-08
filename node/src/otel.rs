//! # OpenTelemetry helpers

use clap::Command;
use tracing::dispatcher::SetGlobalDefaultError;

#[derive(Debug, thiserror::Error)]
pub enum OpenTelemetryInitError {
    #[error("error setting global default subscriber")]
    SetGlobalDefault(#[from] SetGlobalDefaultError),
}

/// Trait used to add common open telemetry arguments to clap apps.
pub trait OpenTelemetryClapCommandExt {
    fn open_telemetry_args(self) -> Self;
}

impl<'help> OpenTelemetryClapCommandExt for Command<'help> {
    fn open_telemetry_args(self) -> Self {
        self
    }
}

pub fn init_opentelemetry() -> Result<(), OpenTelemetryInitError> {
    // TODO: change tracer and metrics to otlp exporter once v0.18 is released.
    // see https://github.com/open-telemetry/opentelemetry-rust/pull/779
    env_logger::init();
    Ok(())
}
