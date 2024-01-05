use std::{
    pin::Pin,
    task::{self, Poll},
};

use apibara_core::node::v1alpha2::{DataFinality, StreamDataRequest};
use futures::Stream;
use pin_project::pin_project;
use prost::Message;
use tracing::warn;

use crate::core::Cursor;

use super::error::StreamError;

const MIN_BATCH_SIZE: usize = 1;
const MAX_BATCH_SIZE: usize = 50;
const DEFAULT_BATCH_SIZE: usize = 20;

#[derive(Default, Clone, Debug)]
pub struct StreamConfiguration<C, F>
where
    C: Cursor,
    F: Message + Default + Clone,
{
    pub batch_size: usize,
    pub stream_id: u64,
    pub finality: DataFinality,
    pub starting_cursor: Option<C>,
    pub filter: Vec<F>,
}

#[derive(Default)]
struct StreamConfigurationStreamState<C, F>
where
    C: Cursor,
    F: Message + Default + Clone,
{
    current: Option<StreamConfiguration<C, F>>,
}

#[pin_project]
pub struct StreamConfigurationStream<C, F, S, E>
where
    C: Cursor,
    F: Message + Default + Clone,
    S: Stream<Item = Result<StreamDataRequest, E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    #[pin]
    inner: S,
    state: StreamConfigurationStreamState<C, F>,
}

impl<C, F, S, E> StreamConfigurationStream<C, F, S, E>
where
    C: Cursor,
    F: Message + Default + Clone,
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

impl<C, F> StreamConfigurationStreamState<C, F>
where
    C: Cursor,
    F: Message + Default + Clone,
{
    fn handle_request(
        &mut self,
        request: StreamDataRequest,
    ) -> Result<StreamConfiguration<C, F>, StreamError> {
        let batch_size = request.batch_size.unwrap_or(DEFAULT_BATCH_SIZE as u64) as usize;
        let batch_size = batch_size.clamp(MIN_BATCH_SIZE, MAX_BATCH_SIZE);

        let finality = request
            .finality
            .and_then(DataFinality::from_i32)
            .unwrap_or(DataFinality::DataStatusAccepted);

        let stream_id = request.stream_id.unwrap_or_default();

        let filter: Vec<F> = if request.filter.is_empty() {
            if request.multi_filter.is_empty() {
                return Err(StreamError::invalid_request(
                    "missing filter configuration".to_string(),
                ));
            }

            if batch_size != 1 {
                return Err(StreamError::invalid_request(
                    "multi-filter configuration is only supported with batch size 1".to_string(),
                ));
            }

            request
                .multi_filter
                .iter()
                .map(|v| F::decode(v.as_ref()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| {
                    StreamError::invalid_request("invalid multi-filter configuration".to_string())
                })?
        } else {
            let filter = F::decode(request.filter.as_ref()).map_err(|_| {
                StreamError::invalid_request("invalid filter configuration".to_string())
            })?;

            vec![filter]
        };

        let starting_cursor = match request.starting_cursor {
            None => None,
            Some(starting_cursor) => match C::from_proto(&starting_cursor) {
                Some(cursor) => Some(cursor),
                None => {
                    return Err(StreamError::invalid_request(
                        "invalid starting cursor".to_string(),
                    ));
                }
            },
        };

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

impl<C, F, S, E> Stream for StreamConfigurationStream<C, F, S, E>
where
    C: Cursor,
    F: Message + Default + Clone,
    S: Stream<Item = Result<StreamDataRequest, E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    type Item = Result<StreamConfiguration<C, F>, StreamError>;

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
