use tonic::metadata::MetadataMap;
use tracing::{info_span, Span};

pub trait RequestSpan: Send + Sync + 'static {
    /// Returns a span to be used when tracing a `stream_data` request.
    fn stream_data_span(&self, metadata: &MetadataMap) -> Span;
}

/// A [RequestSpan] that creates a simple span.
pub struct SimpleRequestSpan {}

/// A [RequestSpan] that adds a specific metadata value to the span.
///
/// This can be used to add information like current user or api keys
/// to the span.
pub struct MetadataKeyRequestSpan {
    key: String,
}

impl MetadataKeyRequestSpan {
    pub fn new(key: String) -> Self {
        MetadataKeyRequestSpan { key }
    }
}

impl Default for SimpleRequestSpan {
    fn default() -> Self {
        SimpleRequestSpan {}
    }
}

impl RequestSpan for SimpleRequestSpan {
    fn stream_data_span(&self, _metadata: &MetadataMap) -> Span {
        info_span!("stream_data")
    }
}

impl RequestSpan for MetadataKeyRequestSpan {
    fn stream_data_span(&self, metadata: &MetadataMap) -> Span {
        let api_key = metadata
            .get(&self.key)
            .and_then(|value| value.to_str().ok());
        if let Some(api_key) = api_key {
            info_span!("stream_data", api_key = api_key)
        } else {
            info_span!("stream_data")
        }
    }
}
