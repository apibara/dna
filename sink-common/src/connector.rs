use std::time::{Duration, Instant};

use apibara_core::node::v1alpha2::{Cursor, DataFinality};
use apibara_script::{Script, ScriptError};
use apibara_sdk::{configuration, ClientBuilder, Configuration, DataMessage, MetadataMap, Uri};
use async_trait::async_trait;
use bytesize::ByteSize;
use exponential_backoff::Backoff;
use prost::Message;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use serde_json::Value;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn};

use crate::{
    cli::LoadScriptError,
    persistence::{Persistence, PersistenceClient, PersistenceError},
    status::StatusServer,
    PersistenceOptionsError, StatusServerOptionsError, StreamOptionsError,
};

pub trait SinkOptions: DeserializeOwned {
    fn merge(self, other: Self) -> Self;
}

pub enum CursorAction {
    Persist,
    Skip,
}

#[async_trait]
pub trait Sink {
    type Options: SinkOptions;
    type Error: std::error::Error + Send + Sync + 'static;

    async fn from_options(options: Self::Options) -> Result<Self, Self::Error>
    where
        Self: Sized;

    async fn handle_data(
        &mut self,
        cursor: &Option<Cursor>,
        end_cursor: &Cursor,
        finality: &DataFinality,
        batch: &Value,
    ) -> Result<CursorAction, Self::Error>;

    async fn handle_invalidate(&mut self, cursor: &Option<Cursor>) -> Result<(), Self::Error>;

    async fn cleanup(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn handle_heartbeat(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct StreamConfiguration {
    pub stream_url: Uri,
    pub max_message_size_bytes: ByteSize,
    pub metadata: MetadataMap,
    pub bearer_token: Option<String>,
}

pub struct SinkConnectorOptions {
    pub stream: StreamConfiguration,
    pub persistence: Option<Persistence>,
    pub status_server: StatusServer,
}

#[derive(Debug, thiserror::Error)]
pub enum OptionsError {
    #[error("Failed to load persistence options: {0}")]
    Persistence(#[from] PersistenceOptionsError),
    #[error("Failed to load status server options: {0}")]
    StatusServer(#[from] StatusServerOptionsError),
    #[error("Failed to load stream options: {0}")]
    Stream(#[from] StreamOptionsError),
    #[error("Failed to load sink options: {0}")]
    Sink(Box<dyn std::error::Error + Send + Sync + 'static>),
}

#[derive(Debug, thiserror::Error)]
pub enum SinkConnectorError {
    #[error(transparent)]
    Options(#[from] OptionsError),
    #[error(transparent)]
    ClientBuilder(#[from] apibara_sdk::ClientBuilderError),
    #[error("Failed to send configuration")]
    SendConfiguration,
    #[error("Stream error: {0}")]
    Stream(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("Script error: {0}")]
    Script(#[from] ScriptError),
    #[error("Error loading script: {0}")]
    LoadScript(#[from] LoadScriptError),
    #[error("Persistence error: {0}")]
    Persistence(#[from] PersistenceError),
    #[error("CtrlC handler error: {0}")]
    CtrlC(#[from] ctrlc::Error),
    #[error("JSON conversion error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Failed to load environment variables: {0}")]
    Dotenv(#[from] dotenvy::Error),
    #[error("Sink error: {0}")]
    Sink(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("Maximum number of retries exceeded")]
    MaximumRetriesExceeded,
}

pub struct SinkConnector<S>
where
    S: Sink + Send + Sync,
{
    script: Script,
    sink: S,
    stream_configuration: StreamConfiguration,
    backoff: Backoff,
    persistence: Option<Persistence>,
    status_server: StatusServer,
    needs_invalidation: bool,
}

impl<S> SinkConnector<S>
where
    S: Sink + Send + Sync,
{
    /// Creates a new connector with the given stream URL.
    pub fn new(script: Script, sink: S, options: SinkConnectorOptions) -> Self {
        let retries = 8;
        let min_delay = Duration::from_secs(10);
        let max_delay = Duration::from_secs(60 * 60);
        let mut backoff = Backoff::new(retries, min_delay, Some(max_delay));
        backoff.set_factor(5);

        Self {
            script,
            sink,
            backoff,
            stream_configuration: options.stream,
            persistence: options.persistence,
            status_server: options.status_server,
            needs_invalidation: false,
        }
    }

    /// Start consuming the stream, calling the configured callback for each message.
    pub async fn consume_stream<F, B>(
        mut self,
        mut configuration: Configuration<F>,
        ct: CancellationToken,
    ) -> Result<(), SinkConnectorError>
    where
        F: Message + Default,
        B: Message + Default + Serialize,
    {
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

        if let Some(persistence) = persistence.as_mut() {
            let starting_cursor = persistence.get_cursor().await?;
            if starting_cursor.is_some() {
                info!(cursor = ?starting_cursor, "restarting from last cursor");
                configuration.starting_cursor = starting_cursor.clone();

                self.handle_invalidate(starting_cursor, Some(persistence), ct.clone())
                    .await?;
            }
        }

        let (configuration_client, configuration_stream) = configuration::channel(128);

        debug!(configuration = ?configuration, "sending configuration");
        configuration_client
            .send(configuration)
            .await
            .map_err(|_| SinkConnectorError::SendConfiguration)?;

        debug!("start consume stream");
        let mut stream_builder = ClientBuilder::<F, B>::default()
            .with_max_message_size(
                self.stream_configuration.max_message_size_bytes.as_u64() as usize
            )
            .with_metadata(self.stream_configuration.metadata.clone());

        stream_builder = if let Some(bearer_token) = self.stream_configuration.bearer_token.take() {
            stream_builder.with_bearer_token(bearer_token)
        } else {
            stream_builder
        };

        let mut data_stream = stream_builder
            .connect(
                self.stream_configuration.stream_url.clone(),
                configuration_stream,
            )
            .await?;

        let mut last_lock_renewal = Instant::now();
        let min_lock_refresh = Duration::from_secs(30);

        // Only start status server at this moment.
        // We want to avoid tricking k8s into believing the server is running fine when it's stuck
        // waiting for the lock.
        let mut status_server = Box::pin(self.status_server.clone().start(ct.clone()));

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
                            self.handle_message(message, persistence.as_mut(), ct.clone()).await?;

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

        self.sink
            .cleanup()
            .await
            .map_err(Into::into)
            .map_err(SinkConnectorError::Sink)?;

        // unlock the lock, if any.
        if let Some(mut persistence) = persistence {
            persistence.unlock(lock).await?;
        }

        Ok(())
    }

    async fn handle_invalidate(
        &mut self,
        cursor: Option<Cursor>,
        persistence: Option<&mut PersistenceClient>,
        ct: CancellationToken,
    ) -> Result<(), SinkConnectorError>
    where
        S: Sink + Sync + Send,
    {
        debug!(cursor = ?cursor, "received invalidate");
        for duration in &self.backoff {
            match self.sink.handle_invalidate(&cursor).await {
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

    #[allow(clippy::too_many_arguments)]
    async fn handle_data<B>(
        &mut self,
        cursor: Option<Cursor>,
        end_cursor: Cursor,
        finality: DataFinality,
        batch: Vec<B>,
        persistence: Option<&mut PersistenceClient>,
        ct: CancellationToken,
    ) -> Result<(), SinkConnectorError>
    where
        B: Message + Default + Serialize,
    {
        let mut persistence = persistence;

        trace!(cursor = ?cursor, end_cursor = ?end_cursor, "received data");
        let json_batch = serde_json::to_value(&batch)?;
        let data = self.script.transform(&json_batch).await?;

        if self.needs_invalidation {
            let mut persistence = persistence.as_mut().map(|persistence| persistence.clone());

            self.handle_invalidate(cursor.clone(), persistence.as_mut(), ct.clone())
                .await?;
            self.needs_invalidation = false;
        }

        for duration in &self.backoff {
            match self
                .sink
                .handle_data(&cursor, &end_cursor, &finality, &data)
                .await
            {
                Ok(cursor_action) => {
                    if finality == DataFinality::DataStatusPending {
                        self.needs_invalidation = true;
                    } else if let Some(persistence) = persistence {
                        if let CursorAction::Persist = cursor_action {
                            persistence.put_cursor(end_cursor).await?;
                        }
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

    async fn handle_message<B>(
        &mut self,
        message: DataMessage<B>,
        persistence: Option<&mut PersistenceClient>,
        ct: CancellationToken,
    ) -> Result<(), SinkConnectorError>
    where
        B: Message + Default + Serialize,
    {
        match message {
            DataMessage::Data {
                cursor,
                end_cursor,
                finality,
                batch,
            } => {
                self.handle_data(cursor, end_cursor, finality, batch, persistence, ct)
                    .await
            }
            DataMessage::Invalidate { cursor } => {
                self.handle_invalidate(cursor, persistence, ct).await
            }
            DataMessage::Heartbeat => self
                .sink
                .handle_heartbeat()
                .await
                .map_err(Into::into)
                .map_err(SinkConnectorError::Sink),
        }
    }
}
