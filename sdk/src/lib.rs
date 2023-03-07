pub mod config;

use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use apibara_core::node::v1alpha2::{
    stream_client::StreamClient, StreamDataRequest, StreamDataResponse,
};
use futures::Stream;
use pin_project::pin_project;
use prost::Message;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Channel, Streaming};

pub use crate::config::Configuration;

#[derive(Debug, thiserror::Error)]
pub enum ClientBuilderError {
    #[error("Failed to build indexer")]
    FailedToBuildIndexer,
    #[error(transparent)]
    TonicError(#[from] tonic::transport::Error),
    #[error(transparent)]
    FailedToConfigureStream(Box<dyn std::error::Error>),
    #[error(transparent)]
    StreamError(#[from] tonic::Status),
}

#[derive(Default)]
pub struct ClientBuilder<F, D>
where
    F: Message + Default,
    D: Message + Default,
{
    client: Option<StreamClient<Channel>>,
    configuration: Option<Configuration<F>>,
    _data: PhantomData<D>,
}

#[derive(Debug)]
#[pin_project]
pub struct DataStream<F, D>
where
    F: Message + Default,
    D: Message + Default,
{
    stream_id: u64,
    configuration_rx: Receiver<Configuration<F>>,
    #[pin]
    inner: Streaming<StreamDataResponse>,
    inner_tx: Sender<StreamDataRequest>,
    _data: PhantomData<D>,
}

impl<F, D> ClientBuilder<F, D>
where
    F: Message + Default,
    D: Message + Default,
{
    pub fn with_client(mut self, client: StreamClient<Channel>) -> Self {
        self.client = Some(client);
        self
    }

    pub fn with_configuration(mut self, configuration: Configuration<F>) -> Self {
        self.configuration = Some(configuration);
        self
    }

    /// Apibara indexer SDK
    ///
    /// let base_configuration = Configuration::default();
    /// let (mut stream, _configuration_handle) =
    ///     ApibaraIndexer::default().build().await?;
    /// while let Some(response) = stream.next().await {
    ///     // Handle your own business logic in there.
    /// }
    pub async fn build(
        self,
        indexer_url: Option<String>,
    ) -> Result<(DataStream<F, D>, Sender<Configuration<F>>), ClientBuilderError> {
        let mut client = self.client.unwrap_or(
            StreamClient::connect(indexer_url.unwrap_or("http://0.0.0.0:7171".into())).await?,
        );
        let (configuration_tx, configuration_rx) = tokio::sync::mpsc::channel(128);
        let (inner_tx, inner_rx) = tokio::sync::mpsc::channel(128);

        if let Some(configuration) = self.configuration {
            configuration_tx.send(configuration).await.unwrap();
        }

        let inner_stream = client
            .stream_data(ReceiverStream::new(inner_rx))
            .await?
            .into_inner();

        Ok((
            DataStream {
                stream_id: 0,
                configuration_rx,
                inner: inner_stream,
                inner_tx,
                _data: PhantomData::default(),
            },
            configuration_tx,
        ))
    }
}

impl<F, D> Stream for DataStream<F, D>
where
    F: Message + Default,
    D: Message + Default,
{
    type Item = Result<StreamDataResponse, Box<dyn std::error::Error>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.configuration_rx.poll_recv(cx) {
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Ready(Some(configuration)) => {
                self.stream_id += 1;
                let request = StreamDataRequest {
                    stream_id: Some(self.stream_id),
                    batch_size: Some(configuration.batch_size),
                    starting_cursor: configuration.starting_cursor,
                    finality: configuration.finality,
                    filter: configuration.filter.encode_to_vec(),
                };

                self.inner_tx.try_send(request)?;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Pending => {}
        }

        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(response))) => {
                if response.stream_id != self.stream_id {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }

                // TODO: decode response.data to `D`.
                Poll::Ready(Some(Ok(response)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(Box::new(e)))),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{ClientBuilder, Configuration};
    use apibara_core::starknet::v1alpha2::{Block, Filter, HeaderFilter};
    use futures_util::StreamExt;

    async fn test_apibara_high_level_api() -> Result<(), Box<dyn std::error::Error>> {
        let (stream, configuration_handle) = ClientBuilder::<Filter, Block>::default()
            .build(Some("http://0.0.0.0:7171".into()))
            .await?;

        let mut config = Configuration::<Filter>::default();
        configuration_handle
            .send(
                config
                    .starting_at_block(1)
                    .with_filter(|filter| filter.with_header(HeaderFilter { weak: false }))
                    .clone(),
            )
            .await?;

        let mut stream = stream.take(2);
        while let Some(response) = stream.next().await {
            println!("Response: {:?}", response);
        }

        Ok(())
    }
}
