//! # OpenTelemetry helpers

mod dna_fmt;
mod request;

use std::borrow::Cow;
use std::time::Duration;

use error_stack::{Result, ResultExt};
use mixtrics::metrics::BoxedRegistry;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::{global, InstrumentationScope};
use opentelemetry_otlp::{MetricExporter, SpanExporter};
use opentelemetry_sdk::metrics::{MeterProviderBuilder, PeriodicReader};
use opentelemetry_sdk::resource::{ResourceDetector, SdkProvidedResourceDetector};
use opentelemetry_sdk::trace::TracerProvider;
use tracing::Subscriber;

pub use opentelemetry::metrics::{ObservableCounter, ObservableGauge};
pub use opentelemetry::trace::{SpanContext, TraceContextExt};
pub use opentelemetry::{Context, Key, KeyValue};
use tracing_opentelemetry::MetricsLayer;
pub use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::{prelude::*, registry::LookupSpan, EnvFilter, Layer};

pub use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter, UpDownCounter};

pub use self::request::{RecordRequest, RecordedRequest, RequestMetrics};

const OTEL_SDK_DISABLED: &str = "OTEL_SDK_DISABLED";

pub type BoxedLayer<S> = Box<dyn Layer<S> + Send + Sync>;

#[derive(Debug)]
pub struct OpenTelemetryInitError;
impl error_stack::Context for OpenTelemetryInitError {}

impl std::fmt::Display for OpenTelemetryInitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("failed to initialize opentelemetry")
    }
}

pub fn meter(name: &'static str) -> Meter {
    global::meter(name)
}

pub fn mixtrics_registry(name: &'static str) -> BoxedRegistry {
    let m = meter(name);
    let registry = mixtrics::registry::opentelemetry::OpenTelemetryMetricsRegistry::new(m);
    Box::new(registry)
}

/// Initialize OpenTelemetry.
///
/// This function initializes the OpenTelemetry SDK and sets up the tracing and metrics layers.
/// It should be called once during the application startup.
///
/// ```rs
/// use apibara_observability::init_opentelemetry;
///
/// init_opentelemetry(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION")).unwrap();
/// ```
pub fn init_opentelemetry(
    package_name: impl Into<Cow<'static, str>>,
    package_version: impl Into<Cow<'static, str>>,
) -> Result<(), OpenTelemetryInitError> {
    {
        // The otel sdk doesn't follow the disabled env variable flag.
        // so we manually implement it to disable otel exports.
        // we diverge from the spec by defaulting to disabled.
        let sdk_disabled = std::env::var(OTEL_SDK_DISABLED)
            .map(|v| v == "true")
            .unwrap_or(true);

        if std::env::var("RUST_LOG").is_err() {
            std::env::set_var("RUST_LOG", "info");
        }

        let mut layers = vec![stdout()];

        if !sdk_disabled {
            let otel_layer = otel(package_name, package_version)?;
            layers.push(otel_layer);
        }

        tracing_subscriber::registry().with(layers).init();
    }

    Ok(())
}

fn otel<S>(
    package_name: impl Into<Cow<'static, str>>,
    version: impl Into<Cow<'static, str>>,
) -> Result<BoxedLayer<S>, OpenTelemetryInitError>
where
    S: Subscriber + Send + Sync,
    for<'a> S: LookupSpan<'a>,
{
    let package_name = package_name.into();
    let version = version.into();

    // filter traces by crate/level
    let otel_env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("INFO"));

    let resource = SdkProvidedResourceDetector.detect(Duration::from_secs(1));
    let instrumentation_lib = InstrumentationScope::builder(package_name.clone())
        .with_version(version.clone())
        .build();

    let span_exporter = SpanExporter::builder()
        .with_tonic()
        .build()
        .change_context(OpenTelemetryInitError)
        .attach_printable("failed to create span exporter")?;

    let trace_provider = TracerProvider::builder()
        .with_resource(resource.clone())
        .with_batch_exporter(span_exporter, opentelemetry_sdk::runtime::Tokio)
        .build();

    let tracer = trace_provider.tracer_with_scope(instrumentation_lib.clone());

    let metrics_exporter = MetricExporter::builder()
        .with_tonic()
        .build()
        .change_context(OpenTelemetryInitError)
        .attach_printable("failed to create metrics exporter")?;

    let metrics_reader =
        PeriodicReader::builder(metrics_exporter, opentelemetry_sdk::runtime::Tokio)
            .with_interval(Duration::from_secs(10))
            .build();

    let meter_provider = MeterProviderBuilder::default()
        .with_resource(resource.clone())
        .with_reader(metrics_reader)
        .build();

    global::set_meter_provider(meter_provider.clone());

    // export traces and metrics to otel
    let otel_trace_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    let otel_metrics_layer = MetricsLayer::new(meter_provider);
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
            .event_format(dna_fmt::DnaFormat::default())
            .fmt_fields(dna_fmt::DnaFormat::default())
            .with_filter(log_env_filter)
            .boxed()
    }
}
