use apibara_node::o11y::{self, Counter, KeyValue};
use tonic::metadata::MetadataMap;
use tracing::{info_span, Span};

pub trait RequestObserver: Send + Sync + 'static {
    type Meter: RequestMeter;

    /// Returns a span to be used when tracing a `stream_data` request.
    fn stream_data_span(&self, metadata: &MetadataMap) -> Span;

    /// Returns a meter to be used when metering a `stream_data` request.
    fn stream_data_meter(&self, metadata: &MetadataMap) -> Self::Meter;
}

pub trait RequestMeter: Send + Sync + 'static {
    /// Increments the counter for the given name by the given amount.
    fn increment_counter(&self, name: &'static str, amount: u64);
}

/// A [RequestObserver] that adds no context.
#[derive(Debug, Default)]
pub struct SimpleRequestObserver {}

/// A [RequestMeter] that adds no context.
pub struct SimpleMeter {
    counter: Counter<u64>,
}

/// A [RequestObserver] that adds a specific metadata value to the span and meter.
///
/// This can be used to add information like current user or api keys.
pub struct MetadataKeyRequestObserver {
    key: String,
}

/// A [RequestMeter] that adds information about the key used.
pub struct MetadataKeyMeter {
    key: String,
    counter: Counter<u64>,
}

impl Default for SimpleMeter {
    fn default() -> Self {
        let counter = new_data_out_counter();
        SimpleMeter { counter }
    }
}

impl MetadataKeyMeter {
    pub fn new(key: String) -> Self {
        let counter = new_data_out_counter();
        MetadataKeyMeter { key, counter }
    }
}

impl MetadataKeyRequestObserver {
    pub fn new(key: String) -> Self {
        MetadataKeyRequestObserver { key }
    }

    fn request_api_key(&self, metadata: &MetadataMap) -> Option<String> {
        metadata
            .get(&self.key)
            .and_then(|value| value.to_str().ok())
            .map(|s| s.to_string())
    }
}

impl RequestObserver for SimpleRequestObserver {
    type Meter = SimpleMeter;

    fn stream_data_span(&self, _metadata: &MetadataMap) -> Span {
        info_span!("stream_data")
    }

    fn stream_data_meter(&self, _metadata: &MetadataMap) -> Self::Meter {
        SimpleMeter::default()
    }
}

impl RequestMeter for SimpleMeter {
    fn increment_counter(&self, name: &'static str, amount: u64) {
        let cx = o11y::Context::current();
        self.counter
            .add(&cx, amount, &[KeyValue::new("datum", name)]);
    }
}

impl RequestObserver for MetadataKeyRequestObserver {
    type Meter = MetadataKeyMeter;

    fn stream_data_span(&self, metadata: &MetadataMap) -> Span {
        if let Some(api_key) = self.request_api_key(metadata) {
            info_span!("stream_data", user.key = api_key)
        } else {
            info_span!("stream_data")
        }
    }

    fn stream_data_meter(&self, metadata: &MetadataMap) -> Self::Meter {
        if let Some(api_key) = self.request_api_key(metadata) {
            MetadataKeyMeter::new(api_key)
        } else {
            MetadataKeyMeter::new("anon".to_string())
        }
    }
}

impl RequestMeter for MetadataKeyMeter {
    fn increment_counter(&self, name: &'static str, amount: u64) {
        let cx = o11y::Context::current();
        self.counter.add(
            &cx,
            amount,
            &[
                KeyValue::new("datum", name),
                KeyValue::new("user.key", self.key.clone()),
            ],
        );
    }
}

fn new_data_out_counter() -> Counter<u64> {
    let meter = o11y::meter("stream_data");
    meter.u64_counter("data_out").init()
}
