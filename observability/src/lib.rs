//! # OpenTelemetry helpers

use std::env;

use opentelemetry::{
    global,
    metrics::MetricsError,
    sdk::{
        self, export::metrics::aggregation::cumulative_temporality_selector, metrics::selectors,
        Resource,
    },
    trace::TraceError,
};
use opentelemetry_otlp::WithExportConfig;
use tracing::{dispatcher::SetGlobalDefaultError, Subscriber};

pub use opentelemetry::metrics::{ObservableCounter, ObservableGauge};
pub use opentelemetry::{Context, Key, KeyValue};
use tracing_opentelemetry::MetricsLayer;
use tracing_subscriber::{prelude::*, registry::LookupSpan, EnvFilter, Layer};

pub use opentelemetry::metrics::{Counter, Meter};

const OTEL_SDK_DISABLED: &str = "OTEL_SDK_DISABLED";

pub type BoxedLayer<S> = Box<dyn Layer<S> + Send + Sync>;

#[derive(Debug, thiserror::Error)]
pub enum OpenTelemetryInitError {
    #[error("error setting global default subscriber")]
    SetGlobalDefault(#[from] SetGlobalDefaultError),
    #[error("error configuring tracing")]
    Trace(#[from] TraceError),
    #[error("error configuring metrics")]
    Metrics(#[from] MetricsError),
}

pub fn meter(name: &'static str) -> Meter {
    global::meter(name)
}

pub fn init_opentelemetry() -> Result<(), OpenTelemetryInitError> {
    // The otel sdk doesn't follow the disabled env variable flag.
    // so we manually implement it to disable otel exports.
    // we diverge from the spec by defaulting to disabled.
    let sdk_disabled = env::var(OTEL_SDK_DISABLED)
        .map(|v| v == "true")
        .unwrap_or(true);

    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }

    let mut layers = vec![stdout()];

    if !sdk_disabled {
        let otel_layer = otel()?;
        layers.push(otel_layer);
    }

    tracing_subscriber::registry().with(layers).init();

    Ok(())
}

fn otel<S>() -> Result<BoxedLayer<S>, OpenTelemetryInitError>
where
    S: Subscriber + Send + Sync,
    for<'a> S: LookupSpan<'a>,
{
    // filter traces by crate/level
    let otel_env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("INFO"));

    // Both tracer and meter are configured with environment variables.
    let meter = opentelemetry_otlp::new_pipeline()
        .metrics(
            selectors::simple::inexpensive(),
            cumulative_temporality_selector(),
            opentelemetry::runtime::Tokio,
        )
        .with_exporter(opentelemetry_otlp::new_exporter().tonic().with_env())
        .with_resource(Resource::default())
        .build()?;

    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(opentelemetry_otlp::new_exporter().tonic().with_env())
        .with_trace_config(sdk::trace::config().with_resource(Resource::default()))
        .install_batch(opentelemetry::runtime::Tokio)?;

    // export traces and metrics to otel
    let otel_trace_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    let otel_metrics_layer = MetricsLayer::new(meter);
    let otel_layer = otel_trace_layer
        .and_then(otel_metrics_layer)
        .and_then(otel_env_filter)
        .boxed();
    Ok(otel_layer)
}

fn stdout<S>() -> BoxedLayer<S>
where
    S: Subscriber,
    for<'a> S: LookupSpan<'a>,
{
    let log_env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("INFO"));

    let json_fmt = std::env::var("RUST_LOG_FORMAT")
        .map(|val| val == "json")
        .unwrap_or(false);

    if json_fmt {
        tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_target(true)
            .json()
            .with_filter(log_env_filter)
            .boxed()
    } else {
        tracing_subscriber::fmt::layer()
            .with_ansi(true)
            .with_target(true)
            .with_filter(log_env_filter)
            .boxed()
    }
}
