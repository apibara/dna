use std::time::Duration;

use apibara_core::node::v1alpha2::{Cursor, DataFinality};
use apibara_script::{Script, ScriptError};
use apibara_sdk::{
    configuration, ClientBuilder, Configuration, DataMessage, MetadataMap, StreamClient, Uri,
};
use async_trait::async_trait;
use bytesize::ByteSize;
use color_eyre::{
    eyre::{eyre, Context},
    Result,
};
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
    persistence::{Persistence, PersistenceClientError},
    status::StatusServer,
    PersistenceClient, PersistenceError, PersistenceOptionsError, StatusServerClient,
    StatusServerOptionsError, StreamOptionsError,
};

pub trait SinkOptions: DeserializeOwned {
    fn merge(self, other: Self) -> Self;
}

#[derive(Debug, PartialEq)]
pub enum CursorAction {
    Persist,
    Skip,
}

#[async_trait]
pub trait Sink {
    type Options: SinkOptions;
    type Error: std::error::Error + Send + Sync + 'static;

    async fn from_options(options: Self::Options) -> std::result::Result<Self, Self::Error>
    where
        Self: Sized;

    async fn handle_data(
        &mut self,
        cursor: &Option<Cursor>,
        end_cursor: &Cursor,
        finality: &DataFinality,
        batch: &Value,
    ) -> Result<CursorAction, Self::Error>;

    async fn handle_invalidate(
        &mut self,
        cursor: &Option<Cursor>,
    ) -> std::result::Result<(), Self::Error>;

    async fn cleanup(&mut self) -> std::result::Result<(), Self::Error> {
        Ok(())
    }

    async fn handle_heartbeat(&mut self) -> std::result::Result<(), Self::Error> {
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
    pub persistence: Persistence,
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
    #[error("Failed to send configuration")]
    SendConfiguration,
    #[error("Stream error: {0}")]
    Stream(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("Script error: {0}")]
    Script(#[from] ScriptError),
    #[error("Error loading script: {0}")]
    LoadScript(#[from] LoadScriptError),
    #[error("Persistence error: {0}")]
    PersistenceFactory(#[from] PersistenceError),
    #[error("Persistence error: {0}")]
    Persistence(#[from] PersistenceClientError),
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
    persistence: Persistence,
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
    ) -> Result<()>
    where
        F: Message + Default,
        B: Message + Default + Serialize,
    {
        let mut persistence = self.persistence.connect().await?;

        let stream_client = self.new_stream_client().await?;

        let (status_client, mut status_server) = self
            .status_server
            .clone()
            .start(stream_client.clone(), ct.clone())
            .await?;

        // Set starting cursor now, before it's modified.
        status_client
            .set_starting_cursor(configuration.starting_cursor.clone())
            .await?;

        info!("acquiring persistence lock");
        // lock will block until it's acquired.
        // limit the time we wait for the lock to 30 seconds or until the cancellation token is
        // cancelled.
        // notice we can straight exit if the cancellation token is cancelled, since the lock
        // is not held by us.
        tokio::select! {
            ret = persistence.lock() => {
                ret?;
                info!("lock acquired");
            }
            _ = tokio::time::sleep(Duration::from_secs(30)) => {
                info!("failed to acquire persistence lock within 30 seconds");
                return Ok(())
            }
            _ = ct.cancelled() => {
                return Ok(())
            }
        }

        let starting_cursor = persistence.get_cursor().await?;
        if starting_cursor.is_some() {
            info!(cursor = ?starting_cursor, "restarting from last cursor");
            configuration.starting_cursor = starting_cursor.clone();

            self.handle_invalidate(
                starting_cursor.clone(),
                &status_client,
                &mut persistence,
                ct.clone(),
            )
            .await?;
        }

        let (configuration_client, configuration_stream) = configuration::channel(128);

        debug!(configuration = ?configuration, "sending configuration");
        configuration_client
            .send(configuration)
            .await
            .map_err(|_| SinkConnectorError::SendConfiguration)?;

        debug!("start consume stream");

        let mut data_stream = stream_client
            .start_stream::<F, B, _>(configuration_stream)
            .await?;

        loop {
            tokio::select! {
                _ = ct.cancelled() => {
                    break;
                }
                _ = &mut status_server => {
                    break;
                }
                maybe_message = data_stream.try_next() => {
                    match maybe_message.context("failed to read dna stream")? {
                        None => {
                            warn!("data stream closed");
                            break;
                        }
                        Some(message) => {
                            self.handle_message(message, &status_client, &mut persistence, ct.clone()).await?;
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
        persistence.unlock().await?;

        Ok(())
    }

    async fn new_stream_client(&self) -> Result<StreamClient> {
        let mut stream_builder = ClientBuilder::default()
            .with_max_message_size(
                self.stream_configuration.max_message_size_bytes.as_u64() as usize
            )
            .with_metadata(self.stream_configuration.metadata.clone());

        stream_builder = if let Some(bearer_token) = self.stream_configuration.bearer_token.clone()
        {
            stream_builder.with_bearer_token(Some(bearer_token))
        } else {
            stream_builder
        };

        let client = stream_builder
            .connect(self.stream_configuration.stream_url.clone())
            .await?;

        Ok(client)
    }

    async fn handle_invalidate<P>(
        &mut self,
        cursor: Option<Cursor>,
        status_client: &StatusServerClient,
        persistence: &mut P,
        ct: CancellationToken,
    ) -> Result<()>
    where
        P: PersistenceClient + Send,
        S: Sink + Sync + Send,
    {
        debug!(cursor = ?cursor, "received invalidate");
        for duration in &self.backoff {
            match self.sink.handle_invalidate(&cursor).await {
                Ok(_) => {
                    // if the sink started streaming from the genesis block
                    // and if the genesis block has been reorged, delete the
                    // stored cursor value to restart from genesis.
                    match cursor {
                        None => {
                            persistence.delete_cursor().await?;
                            status_client.update_cursor(None).await?;
                        }
                        Some(cursor) => {
                            persistence.put_cursor(cursor.clone()).await?;
                            status_client.update_cursor(Some(cursor)).await?;
                        }
                    }
                    return Ok(());
                }
                Err(err) => {
                    warn!(err = ?err, "handle_invalidate error");
                    if ct.is_cancelled() {
                        return Err(err).context("handle invalidate");
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

        Err(eyre!("handle invalidate failed after retry"))
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_data<B, P>(
        &mut self,
        cursor: Option<Cursor>,
        end_cursor: Cursor,
        finality: DataFinality,
        batch: Vec<B>,
        status_client: &StatusServerClient,
        persistence: &mut P,
        ct: CancellationToken,
    ) -> Result<()>
    where
        B: Message + Default + Serialize,
        P: PersistenceClient + Send,
    {
        trace!(cursor = ?cursor, end_cursor = ?end_cursor, "received data");
        let json_batch = serde_json::to_value(&batch)?;
        let data = self.script.transform(&json_batch).await?;

        if self.needs_invalidation {
            self.handle_invalidate(cursor.clone(), status_client, persistence, ct.clone())
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
                    } else if let CursorAction::Persist = cursor_action {
                        persistence.put_cursor(end_cursor.clone()).await?;
                        status_client.update_cursor(Some(end_cursor)).await?;
                    }

                    return Ok(());
                }
                Err(err) => {
                    warn!(err = ?err, "handle_data error");
                    if ct.is_cancelled() {
                        return Err(err).context("handle data");
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

        Err(eyre!("handle data failed after retry"))
    }

    async fn handle_message<B, P>(
        &mut self,
        message: DataMessage<B>,
        status_client: &StatusServerClient,
        persistence: &mut P,
        ct: CancellationToken,
    ) -> Result<()>
    where
        B: Message + Default + Serialize,
        P: PersistenceClient + Send,
    {
        match message {
            DataMessage::Data {
                cursor,
                end_cursor,
                finality,
                batch,
            } => {
                self.handle_data(
                    cursor,
                    end_cursor,
                    finality,
                    batch,
                    status_client,
                    persistence,
                    ct,
                )
                .await
            }
            DataMessage::Invalidate { cursor } => {
                self.handle_invalidate(cursor, status_client, persistence, ct)
                    .await
            }
            DataMessage::Heartbeat => {
                self.sink
                    .handle_heartbeat()
                    .await
                    .context("handle heartbeat")?;
                status_client.heartbeat().await?;
                Ok(())
            }
        }
    }
}
