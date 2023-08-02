use crate::o11y::{self, Counter, KeyValue};
use tonic::metadata::MetadataMap;
use tracing::{debug_span, Span};

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

    /// Increments the counter for the total bytes sent by the given amount.
    fn increment_bytes_sent_counter(&self, amount: u64);
}

/// A [RequestObserver] that adds no context.
#[derive(Debug, Default)]
pub struct SimpleRequestObserver {}

/// A [RequestMeter] that adds no context.
pub struct SimpleMeter {
    counter: Counter<u64>,
    bytes_sent_counter: Counter<u64>,
}

/// A [RequestObserver] that adds a specific metadata value to the span and meter.
///
/// This can be used to add information like current user or api keys.
pub struct MetadataKeyRequestObserver {
    keys: Vec<String>,
}

/// A [RequestMeter] that adds information about the key used.
pub struct MetadataKeyMeter {
    metadata: Vec<KeyValue>,
    counter: Counter<u64>,
    bytes_sent_counter: Counter<u64>,
}

impl Default for SimpleMeter {
    fn default() -> Self {
        let counter = new_data_out_counter();
        let bytes_sent_counter = new_bytes_sent_counter();
        SimpleMeter {
            counter,
            bytes_sent_counter,
        }
    }
}

impl MetadataKeyMeter {
    pub fn new(metadata: Vec<KeyValue>) -> Self {
        let counter = new_data_out_counter();
        let bytes_sent_counter = new_bytes_sent_counter();
        MetadataKeyMeter {
            metadata,
            counter,
            bytes_sent_counter,
        }
    }
}

impl MetadataKeyRequestObserver {
    pub fn new(keys: Vec<String>) -> Self {
        MetadataKeyRequestObserver { keys }
    }
}

impl RequestObserver for SimpleRequestObserver {
    type Meter = SimpleMeter;

    fn stream_data_span(&self, _metadata: &MetadataMap) -> Span {
        debug_span!("stream_data")
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

    fn increment_bytes_sent_counter(&self, amount: u64) {
        let cx = o11y::Context::current();
        self.bytes_sent_counter.add(&cx, amount, &[]);
    }
}

impl RequestObserver for MetadataKeyRequestObserver {
    type Meter = MetadataKeyMeter;

    fn stream_data_span(&self, _metadata: &MetadataMap) -> Span {
        debug_span!("stream_data")
    }

    fn stream_data_meter(&self, metadata: &MetadataMap) -> Self::Meter {
        let mut result = Vec::with_capacity(self.keys.len());
        for key in &self.keys {
            if let Some(value) = metadata.get(key) {
                if let Ok(value) = value.to_str() {
                    result.push(KeyValue::new(key.clone(), value.to_owned()));
                }
            }
        }
        MetadataKeyMeter::new(result)
    }
}

impl RequestMeter for MetadataKeyMeter {
    fn increment_counter(&self, name: &'static str, amount: u64) {
        let cx = o11y::Context::current();
        // Once otel supports default attributes, we can use those instead of
        // concatenating the attributes here.
        let attributes = &[&[KeyValue::new("datum", name)], self.metadata.as_slice()].concat();
        self.counter.add(&cx, amount, attributes);
    }

    fn increment_bytes_sent_counter(&self, amount: u64) {
        let cx = o11y::Context::current();
        let attributes = self.metadata.as_slice();
        self.bytes_sent_counter.add(&cx, amount, attributes);
    }
}

fn new_data_out_counter() -> Counter<u64> {
    let meter = o11y::meter("stream_data");
    meter.u64_counter("data_out").init()
}

fn new_bytes_sent_counter() -> Counter<u64> {
    let meter = o11y::meter("stream_data");
    meter.u64_counter("stream_bytes_sent").init()
}
