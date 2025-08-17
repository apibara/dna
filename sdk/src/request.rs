use apibara_dna_protocol::dna::stream::{Cursor, DataFinality, StreamDataRequest};
use prost::Message;

/// This builder is used to create a [StreamDataRequest].
#[derive(Debug, Clone)]
pub struct StreamDataRequestBuilder {
    inner: StreamDataRequest,
}

impl StreamDataRequestBuilder {
    /// Creates a new [StreamDataRequestBuilder] for data with accepted finality.
    pub fn new() -> Self {
        let inner = StreamDataRequest {
            finality: Some(DataFinality::Accepted as i32),
            ..Default::default()
        };
        Self { inner }
    }

    /// Sets the finality for the stream.
    pub fn with_finality(mut self, finality: DataFinality) -> Self {
        self.inner.finality = Some(finality as i32);
        self
    }

    /// Sets the starting cursor for the stream.
    ///
    /// When streaming data, each data message will have a `end_cursor` field
    /// that you can use to resume the stream from the next message.
    ///
    /// A value of `None` means that the stream will start from the beginning.
    pub fn with_starting_cursor(mut self, cursor: Option<Cursor>) -> Self {
        self.inner.starting_cursor = cursor;
        self
    }

    /// Adds a filter to the stream.
    ///
    /// Filters are used to limit the data returned by the stream.
    pub fn add_filter<F: Message>(mut self, filter: F) -> Self {
        self.inner.filter.push(filter.encode_to_vec());
        self
    }

    /// Builds the [StreamDataRequest].
    pub fn build(self) -> StreamDataRequest {
        self.inner
    }
}

impl Default for StreamDataRequestBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl From<StreamDataRequestBuilder> for StreamDataRequest {
    fn from(builder: StreamDataRequestBuilder) -> Self {
        builder.inner
    }
}
