//! # OpenTelemetry helpers

use clap::Command;
use opentelemetry::{
    global,
    metrics::{Meter, MetricsError},
    sdk::{
        self, export::metrics::aggregation::cumulative_temporality_selector, metrics::selectors,
        Resource,
    },
    trace::TraceError,
};
use opentelemetry_otlp::WithExportConfig;
use tracing::dispatcher::SetGlobalDefaultError;

pub use opentelemetry::metrics::{ObservableCounter, ObservableGauge};
pub use opentelemetry::{Context, KeyValue};

#[derive(Debug, thiserror::Error)]
pub enum OpenTelemetryInitError {
    #[error("error setting global default subscriber")]
    SetGlobalDefault(#[from] SetGlobalDefaultError),
    #[error("error configuring tracing")]
    Trace(#[from] TraceError),
    #[error("error configuring metrics")]
    Metrics(#[from] MetricsError),
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

pub fn meter(name: &'static str) -> Meter {
    global::meter(name)
}

pub fn init_opentelemetry() -> Result<(), OpenTelemetryInitError> {
    env_logger::init();

    // Both tracer and meter are configured with environment variables.
    let _tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(opentelemetry_otlp::new_exporter().tonic().with_env())
        .with_trace_config(sdk::trace::config().with_resource(Resource::default()))
        .install_batch(opentelemetry::runtime::Tokio)?;

    let _meter = opentelemetry_otlp::new_pipeline()
        .metrics(
            selectors::simple::inexpensive(),
            cumulative_temporality_selector(),
            opentelemetry::runtime::Tokio,
        )
        .with_exporter(opentelemetry_otlp::new_exporter().tonic().with_env())
        .with_resource(Resource::default())
        .build()?;

    Ok(())
}
