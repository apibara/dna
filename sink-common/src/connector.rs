use std::marker::PhantomData;

use apibara_sdk::{ClientBuilder, Configuration, DataMessage, Uri};
use async_trait::async_trait;
use prost::Message;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

#[async_trait]
pub trait Sink {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn handle_data(&mut self) -> Result<(), Self::Error>;
    async fn handle_invalidate(&mut self) -> Result<(), Self::Error>;
}

#[derive(Debug, thiserror::Error)]
pub enum SinkConnectorError {
    #[error(transparent)]
    ClientBuilder(#[from] apibara_sdk::ClientBuilderError),
    #[error("Failed to send configuration")]
    SendConfiguration,
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
        S: Sink,
    {
        let (mut data_stream, data_client) = ClientBuilder::<F, B>::default()
            .connect(self.stream_url)
            .await?;

        println!("send configuration = {:?}", self.configuration);
        data_client
            .send(self.configuration)
            .await
            .map_err(|_| SinkConnectorError::SendConfiguration)?;

        while let Some(data) = data_stream.try_next().await.unwrap() {
            match data {
                DataMessage::Data {
                    cursor,
                    end_cursor,
                    finality,
                    batch,
                } => {
                    println!("data = {:?}", batch);
                    // TODO: handle backoff
                    sink.handle_data().await.unwrap();
                }
                DataMessage::Invalidate { cursor } => {
                    println!("invalidate = {:?}", cursor);
                    // TODO: handle backoff
                    sink.handle_invalidate().await.unwrap();
                }
            }
        }
        Ok(())
    }
}
