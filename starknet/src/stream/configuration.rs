//! Wrap a tonic request stream and returns a stream of configuration changes.

use std::{
    pin::Pin,
    task::{self, Poll},
};

use futures::Stream;
use pin_project::pin_project;
use tracing::warn;

use crate::core::{
    pb::{
        starknet::v1alpha2::Filter,
        stream::v1alpha2::{DataFinality, StreamDataRequest},
    },
    GlobalBlockId,
};

use super::StreamError;

const MIN_BATCH_SIZE: usize = 1;
const MAX_BATCH_SIZE: usize = 50;
const DEFAULT_BATCH_SIZE: usize = 20;

#[derive(Debug, Clone)]
pub struct StreamConfiguration {
    pub batch_size: usize,
    pub stream_id: u64,
    pub finality: DataFinality,
    pub starting_cursor: Option<GlobalBlockId>,
    pub filter: Filter,
}

#[derive(Default)]
struct StreamConfigurationStreamState {
    current: Option<StreamConfiguration>,
}

#[pin_project]
pub struct StreamConfigurationStream<S, E>
where
    S: Stream<Item = Result<StreamDataRequest, E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    #[pin]
    inner: S,
    state: StreamConfigurationStreamState,
}

impl<S, E> StreamConfigurationStream<S, E>
where
    S: Stream<Item = Result<StreamDataRequest, E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    pub fn new(inner: S) -> Self {
        StreamConfigurationStream {
            inner,
            state: Default::default(),
        }
    }
}

impl StreamConfigurationStreamState {
    fn handle_request(
        &mut self,
        request: StreamDataRequest,
    ) -> Result<StreamConfiguration, StreamError> {
        let batch_size = request.batch_size.unwrap_or(DEFAULT_BATCH_SIZE as u64) as usize;
        let batch_size = batch_size.clamp(MIN_BATCH_SIZE, MAX_BATCH_SIZE);

        let finality = request
            .finality
            .and_then(DataFinality::from_i32)
            .unwrap_or(DataFinality::DataStatusAccepted);

        let stream_id = request.stream_id.unwrap_or_default();

        let filter = request
            .filter
            .ok_or_else(|| StreamError::client("filter is required"))?;

        let starting_cursor = request
            .starting_cursor
            .map(|c| GlobalBlockId::from_cursor(&c))
            .transpose()
            .map_err(|_| StreamError::client("invalid stream cursor"))?;

        let configuration = StreamConfiguration {
            batch_size,
            finality,
            stream_id,
            filter,
            starting_cursor,
        };

        self.current = Some(configuration.clone());

        Ok(configuration)
    }
}

impl<S, E> Stream for StreamConfigurationStream<S, E>
where
    S: Stream<Item = Result<StreamDataRequest, E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    type Item = Result<StreamConfiguration, StreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match this.inner.poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(err))) => {
                warn!(err = ?err, "configuration stream error");
                let err = Err(StreamError::internal(err));
                Poll::Ready(Some(err))
            }
            Poll::Ready(Some(Ok(request))) => {
                let result = this.state.handle_request(request);
                Poll::Ready(Some(result))
            }
        }
    }
}
