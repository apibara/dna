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
use tracing::dispatcher::SetGlobalDefaultError;

pub use opentelemetry::metrics::{ObservableCounter, ObservableGauge};
pub use opentelemetry::{Context, Key, KeyValue};
use tracing_opentelemetry::MetricsLayer;
use tracing_subscriber::{filter, prelude::*, EnvFilter};

pub use opentelemetry::metrics::{Counter, Meter};

const OTEL_SDK_DISABLED: &str = "OTEL_SDK_DISABLED";

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
    let sdk_disabled = env::var(OTEL_SDK_DISABLED)
        .map(|v| v == "true")
        .unwrap_or(false);

    if sdk_disabled {
        init_opentelemetry_no_sdk()
    } else {
        init_opentelemetry_with_sdk()
    }
}

fn init_opentelemetry_no_sdk() -> Result<(), OpenTelemetryInitError> {
    let log_env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("INFO"));
    let logtree_layer = tracing_tree::HierarchicalLayer::new(2).and_then(log_env_filter);

    tracing_subscriber::Registry::default()
        .with(logtree_layer)
        .init();
    Ok(())
}

fn init_opentelemetry_with_sdk() -> Result<(), OpenTelemetryInitError> {
    // filter traces by crate/level
    let otel_env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("INFO"));
    let log_env_filter =
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
        .and_then(otel_env_filter);

    // display traces on stdout
    let logtree_layer = tracing_tree::HierarchicalLayer::new(2)
        .and_then(log_env_filter)
        .with_filter(filter::filter_fn(|metadata| {
            metadata.fields().field("data.is_metrics").is_none()
        }));

    tracing_subscriber::Registry::default()
        .with(otel_layer)
        .with(logtree_layer)
        .init();

    Ok(())
}
