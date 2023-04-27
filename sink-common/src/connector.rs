use std::marker::PhantomData;

use apibara_core::node::v1alpha2::{Cursor, DataFinality};
use apibara_sdk::{ClientBuilder, Configuration, DataMessage, Uri};
use async_trait::async_trait;
use prost::Message;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

#[async_trait]
pub trait Sink<B>
where
    B: Message,
{
    type Error: std::error::Error + Send + Sync + 'static;

    async fn handle_data(
        &mut self,
        cursor: Option<Cursor>,
        end_cursor: Cursor,
        finality: DataFinality,
        batch: Vec<B>,
    ) -> Result<(), Self::Error>;
    async fn handle_invalidate(&mut self, cursor: Option<Cursor>) -> Result<(), Self::Error>;
}

#[derive(Debug, thiserror::Error)]
pub enum SinkConnectorError {
    #[error(transparent)]
    ClientBuilder(#[from] apibara_sdk::ClientBuilderError),
    #[error("Failed to send configuration")]
    SendConfiguration,
    #[error("Stream error: {0}")]
    Stream(#[from] Box<dyn std::error::Error>),
}

pub struct SinkConnector<F, B>
where
    F: Message + Default + Clone,
    B: Message + Default,
{
    configuration: Configuration<F>,
    stream_url: Uri,
    _phantom: PhantomData<B>,
}

impl<F, B> SinkConnector<F, B>
where
    F: Message + Default + Clone,
    B: Message + Default,
{
    /// Creates a new connector with the given stream URL.
    pub fn new(stream_url: Uri, configuration: Configuration<F>) -> Self {
        Self {
            stream_url,
            configuration,
            _phantom: PhantomData::default(),
        }
    }

    /// Start consuming the stream, calling the configured callback for each message.
    pub async fn consume_stream<S>(
        self,
        mut sink: S,
        ct: CancellationToken,
    ) -> Result<(), SinkConnectorError>
    where
        S: Sink<B>,
    {
        debug!("start consume stream");
        let (mut data_stream, data_client) = ClientBuilder::<F, B>::default()
            .connect(self.stream_url.clone())
            .await?;

        debug!(configuration = ?self.configuration, "sending configuration");
        data_client
            .send(self.configuration.clone())
            .await
            .map_err(|_| SinkConnectorError::SendConfiguration)?;

        loop {
            tokio::select! {
                _ = ct.cancelled() => {
                    return Ok(())
                }
                maybe_message = data_stream.try_next() => {
                    match maybe_message.map_err(SinkConnectorError::Stream)? {
                        None => {
                            warn!("data stream closed");
                            return Ok(())
                        }
                        Some(message) => {
                            self.handle_message(message, &mut sink, ct.clone()).await?;
                        }
                    }
                }
            }
        }
    }

    async fn handle_message<S>(
        &self,
        message: DataMessage<B>,
        sink: &mut S,
        _ct: CancellationToken,
    ) -> Result<(), SinkConnectorError>
    where
        S: Sink<B>,
    {
        match message {
            DataMessage::Data {
                cursor,
                end_cursor,
                finality,
                batch,
            } => {
                debug!(cursor = ?cursor, end_cursor = ?end_cursor, "received data");
                // TODO: handle backoff
                sink.handle_data(cursor, end_cursor, finality, batch)
                    .await
                    .unwrap();
                Ok(())
            }
            DataMessage::Invalidate { cursor } => {
                debug!(cursor = ?cursor, "received invalidate");
                // TODO: handle backoff
                sink.handle_invalidate(cursor).await.unwrap();
                Ok(())
            }
        }
    }
}
