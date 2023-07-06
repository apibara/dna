use std::{
    marker::PhantomData,
    net::SocketAddr,
    time::{Duration, Instant},
};

use apibara_core::node::v1alpha2::{Cursor, DataFinality};
use apibara_sdk::{configuration, ClientBuilder, Configuration, DataMessage, MetadataMap, Uri};
use apibara_transformer::{Transformer, TransformerError};
use async_trait::async_trait;
use exponential_backoff::Backoff;
use prost::Message;
use serde::ser::Serialize;
use serde_json::{json, Value};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn};

use crate::{
    persistence::{Persistence, PersistenceClient, PersistenceError},
    start_status_server,
};

#[async_trait]
pub trait Sink {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn handle_data(
        &mut self,
        cursor: &Option<Cursor>,
        end_cursor: &Cursor,
        finality: &DataFinality,
        batch: &Value,
    ) -> Result<(), Self::Error>;

    async fn handle_invalidate(&mut self, cursor: &Option<Cursor>) -> Result<(), Self::Error>;

    async fn cleanup(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn handle_heartbeat(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SinkConnectorError {
    #[error(transparent)]
    ClientBuilder(#[from] apibara_sdk::ClientBuilderError),
    #[error("Failed to send configuration")]
    SendConfiguration,
    #[error("Stream error: {0}")]
    Stream(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("Transform error: {0}")]
    Transform(#[from] TransformerError),
    #[error("Persistence error: {0}")]
    Persistence(#[from] PersistenceError),
    #[error("CtrlC handler error: {0}")]
    CtrlC(#[from] ctrlc::Error),
    #[error("JSON conversion error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Sink error: {0}")]
    Sink(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("Maximum number of retries exceeded")]
    MaximumRetriesExceeded,
}

pub struct SinkConnector<F, B>
where
    F: Message + Default + Clone,
    B: Message + Default,
{
    configuration: Configuration<F>,
    stream_url: Uri,
    backoff: Backoff,
    transformer: Option<Transformer>,
    metadata: MetadataMap,
    bearer_token: Option<String>,
    persistence: Option<Persistence>,
    max_message_size: usize,
    status_server_address: SocketAddr,
    _phantom: PhantomData<B>,
}

pub struct SinkConnectorOptions<F>
where
    F: Message + Default + Clone,
{
    pub stream_url: Uri,
    pub configuration: Configuration<F>,
    pub transformer: Option<Transformer>,
    pub metadata: MetadataMap,
    pub bearer_token: Option<String>,
    pub persistence: Option<Persistence>,
    pub max_message_size: usize,
    pub status_server_address: SocketAddr,
}

impl<F, B> SinkConnector<F, B>
where
    F: Message + Default + Clone,
    B: Message + Default + Serialize,
{
    /// Creates a new connector with the given stream URL.
    pub fn new(options: SinkConnectorOptions<F>) -> Self {
        let retries = 8;
        let min_delay = Duration::from_secs(10);
        let max_delay = Duration::from_secs(60 * 60);
        let mut backoff = Backoff::new(retries, min_delay, Some(max_delay));
        backoff.set_factor(5);

        Self {
            stream_url: options.stream_url,
            configuration: options.configuration,
            backoff,
            transformer: options.transformer,
            metadata: options.metadata,
            bearer_token: options.bearer_token,
            persistence: options.persistence,
            max_message_size: options.max_message_size,
            status_server_address: options.status_server_address,
            _phantom: PhantomData::default(),
        }
    }

    /// Start consuming the stream, calling the configured callback for each message.
    pub async fn consume_stream<S>(
        mut self,
        mut sink: S,
        ct: CancellationToken,
    ) -> Result<(), SinkConnectorError>
    where
        S: Sink + Sync + Send,
    {
        // correctly handling Ctrl-C is very important when using persistence
        // otherwise the lock will be released after the lease expires.
        ctrlc::set_handler({
            let ct = ct.clone();
            move || {
                ct.cancel();
            }
        })?;

        let mut persistence = if let Some(persistence) = self.persistence.take() {
            Some(persistence.connect().await?)
        } else {
            None
        };

        let mut lock = if let Some(persistence) = persistence.as_mut() {
            info!("acquiring persistence lock");
            // lock will block until it's acquired.
            // limit the time we wait for the lock to 30 seconds or until the cancellation token is
            // cancelled.
            // notice we can straight exit if the cancellation token is cancelled, since the lock
            // is not held by us.
            tokio::select! {
                lock = persistence.lock() => {
                    Some(lock?)
                }
                _ = tokio::time::sleep(Duration::from_secs(30)) => {
                    info!("failed to acquire persistence lock within 30 seconds");
                    return Ok(())
                }
                _ = ct.cancelled() => {
                    return Ok(())
                }
            }
        } else {
            None
        };

        let mut configuration = self.configuration.clone();
        if let Some(persistence) = persistence.as_mut() {
            let starting_cursor = persistence.get_cursor().await?;
            if starting_cursor.is_some() {
                info!(cursor = ?starting_cursor, "restarting from last cursor");
                configuration.starting_cursor = starting_cursor;
            }
        }

        let (configuration_client, configuration_stream) = configuration::channel(128);

        debug!(configuration = ?self.configuration, "sending configuration");
        configuration_client
            .send(configuration)
            .await
            .map_err(|_| SinkConnectorError::SendConfiguration)?;

        debug!("start consume stream");
        let mut stream_builder = ClientBuilder::<F, B>::default()
            .with_max_message_size(self.max_message_size)
            .with_metadata(self.metadata.clone());

        stream_builder = if let Some(bearer_token) = self.bearer_token.take() {
            stream_builder.with_bearer_token(bearer_token)
        } else {
            stream_builder
        };

        let mut data_stream = stream_builder
            .connect(self.stream_url.clone(), configuration_stream)
            .await?;

        let mut last_lock_renewal = Instant::now();
        let min_lock_refresh = Duration::from_secs(30);

        // Only start status server at this moment.
        // We want to avoid tricking k8s into believing the server is running fine when it's stuck
        // waiting for the lock.
        let mut status_server = Box::pin(start_status_server(self.status_server_address));

        loop {
            tokio::select! {
                _ = ct.cancelled() => {
                    break;
                }
                _ = &mut status_server => {
                    break;
                }
                maybe_message = data_stream.try_next() => {
                    match maybe_message.map_err(SinkConnectorError::Stream)? {
                        None => {
                            warn!("data stream closed");
                            break;
                        }
                        Some(message) => {
                            self.handle_message(message, &mut sink, persistence.as_mut(), ct.clone()).await?;

                            // Renew the lock every 30 seconds to avoid hammering etcd.
                            if last_lock_renewal.elapsed() > min_lock_refresh {
                                if let Some(lock) = lock.as_mut() {
                                    // persistence.renew_lock(&lock).await?;
                                    lock.keep_alive().await?;
                                }
                                last_lock_renewal = Instant::now();
                            }
                        }
                    }
                }
            }
        }

        sink.cleanup()
            .await
            .map_err(Into::into)
            .map_err(SinkConnectorError::Sink)?;

        // unlock the lock, if any.
        if let Some(mut persistence) = persistence {
            persistence.unlock(lock).await?;
        }

        Ok(())
    }

    async fn handle_message<S>(
        &mut self,
        message: DataMessage<B>,
        sink: &mut S,
        persistence: Option<&mut PersistenceClient>,
        ct: CancellationToken,
    ) -> Result<(), SinkConnectorError>
    where
        S: Sink + Sync + Send,
    {
        match message {
            DataMessage::Data {
                cursor,
                end_cursor,
                finality,
                batch,
            } => {
                trace!(cursor = ?cursor, end_cursor = ?end_cursor, "received data");
                let data = if let Some(transformer) = &mut self.transformer {
                    let json_batch = serde_json::to_value(&batch)?;
                    let result = transformer.transform(&json_batch).await?;
                    json!(result)
                } else {
                    json!(batch)
                };
                for duration in &self.backoff {
                    match sink
                        .handle_data(&cursor, &end_cursor, &finality, &data)
                        .await
                    {
                        Ok(_) => {
                            if let Some(persistence) = persistence {
                                persistence.put_cursor(end_cursor).await?;
                            }

                            return Ok(());
                        }
                        Err(err) => {
                            warn!(err = ?err, "handle_data error");
                            if ct.is_cancelled() {
                                return Err(SinkConnectorError::Sink(err.into()));
                            }
                            tokio::select! {
                                _ = tokio::time::sleep(duration) => {},
                                _ = ct.cancelled() => {
                                    return Ok(())
                                }
                            };
                        }
                    }
                }
                Err(SinkConnectorError::MaximumRetriesExceeded)
            }
            DataMessage::Invalidate { cursor } => {
                debug!(cursor = ?cursor, "received invalidate");
                for duration in &self.backoff {
                    match sink.handle_invalidate(&cursor).await {
                        Ok(_) => {
                            if let Some(persistence) = persistence {
                                // if the sink started streaming from the genesis block
                                // and if the genesis block has been reorged, delete the
                                // stored cursor value to restart from genesis.
                                match cursor {
                                    None => {
                                        persistence.delete_cursor().await?;
                                    }
                                    Some(cursor) => {
                                        persistence.put_cursor(cursor).await?;
                                    }
                                }
                            }
                            return Ok(());
                        }
                        Err(err) => {
                            warn!(err = ?err, "handle_invalidate error");
                            if ct.is_cancelled() {
                                return Err(SinkConnectorError::Sink(err.into()));
                            }
                            tokio::select! {
                                _ = tokio::time::sleep(duration) => {},
                                _ = ct.cancelled() => {
                                    return Ok(())
                                }
                            };
                        }
                    }
                }
                Err(SinkConnectorError::MaximumRetriesExceeded)
            }
            DataMessage::Heartbeat => sink
                .handle_heartbeat()
                .await
                .map_err(Into::into)
                .map_err(SinkConnectorError::Sink),
        }
    }
}
