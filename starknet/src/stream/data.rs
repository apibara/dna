//! Root data stream.

use std::{
    pin::Pin,
    sync::Arc,
    task::{self, Poll},
};

use apibara_core::node::v1alpha2::StreamDataResponse;
use futures::Stream;
use pin_project::pin_project;
use tracing::info_span;

use crate::{
    core::IngestionMessage, db::StorageReader, healer::HealerClient, server::RequestMeter,
};

use super::{configuration::StreamConfiguration, filtered::FilteredDataStream, StreamError};

#[derive(Debug, thiserror::Error)]
pub enum DataStreamError {
    #[error("ingestion stream closed")]
    IngestionClosed,
    #[error("data stream closed")]
    DataClosed,
}

#[pin_project]
pub struct DataStream<C, L, R, M>
where
    C: Stream<Item = Result<StreamConfiguration, StreamError>>,
    L: Stream<Item = Result<IngestionMessage, StreamError>>,
    R: StorageReader,
    M: RequestMeter,
{
    #[pin]
    configuration_stream: C,
    #[pin]
    ingestion_stream: L,
    #[pin]
    inner: FilteredDataStream<R, M>,
}

impl<C, L, R, M> DataStream<C, L, R, M>
where
    C: Stream<Item = Result<StreamConfiguration, StreamError>>,
    L: Stream<Item = Result<IngestionMessage, StreamError>>,
    R: StorageReader,
    M: RequestMeter,
{
    /// Creates a new data stream.
    pub fn new(
        configuration_stream: C,
        ingestion_stream: L,
        storage: Arc<R>,
        healer: Arc<HealerClient>,
        meter: Arc<M>,
    ) -> Self {
        DataStream {
            configuration_stream,
            ingestion_stream,
            inner: FilteredDataStream::new(storage, healer, meter),
        }
    }
}

impl<C, L, R, M> Stream for DataStream<C, L, R, M>
where
    C: Stream<Item = Result<StreamConfiguration, StreamError>>,
    L: Stream<Item = Result<IngestionMessage, StreamError>>,
    R: StorageReader,
    M: RequestMeter,
{
    type Item = Result<StreamDataResponse, StreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        let _span = info_span!("poll_data_stream");

        // listen for configuration changes
        match this.configuration_stream.poll_next(cx) {
            Poll::Pending => {
                // stream consumed fully.
                // can now consume ingestion stream
            }
            Poll::Ready(None) => {
                // client closed stream.
                // close this stream too.
                return Poll::Ready(None);
            }
            Poll::Ready(Some(Err(err))) => {
                // forward configuration error
                return Poll::Ready(Some(Err(err)));
            }
            Poll::Ready(Some(Ok(configuration))) => {
                // configuration changed.
                // update and restart, or return error
                match this.inner.reconfigure_data_stream(configuration) {
                    Ok(_) => {
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                    Err(err) => return Poll::Ready(Some(Err(err))),
                }
            }
        }

        // listen for ingestion changes
        match this.ingestion_stream.poll_next(cx) {
            Poll::Pending => {
                // stream consumed fully
                // can continue sending data to stream
            }
            Poll::Ready(None) => {
                // ingestion stream should never be closed.
                let err = StreamError::internal(DataStreamError::IngestionClosed);
                return Poll::Ready(Some(Err(err)));
            }
            Poll::Ready(Some(Err(err))) => {
                // forward error
                return Poll::Ready(Some(Err(err)));
            }
            Poll::Ready(Some(Ok(message))) => {
                // update state based on ingestion message.
                match this.inner.handle_ingestion_message(message) {
                    Ok(_) => {
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                    Err(err) => return Poll::Ready(Some(Err(err))),
                }
            }
        }

        // then yield new batches
        match this.inner.poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                // data stream should never close
                let err = StreamError::internal(DataStreamError::DataClosed);
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(Some(Err(err))) => {
                // forward data stream error
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(Some(Ok(data))) => {
                // forward data
                Poll::Ready(Some(Ok(data)))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}
