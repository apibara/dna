use std::time::Duration;

use apibara_core::node::v1alpha2::{Cursor, DataFinality};
use apibara_script::Script;
use apibara_sdk::{
    configuration, ClientBuilder, Configuration, DataMessage, MetadataMap, StreamClient, Uri,
};
use async_trait::async_trait;
use bytesize::ByteSize;
use error_stack::{Result, ResultExt};
use exponential_backoff::Backoff;
use prost::Message;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use serde_json::Value;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn};

use crate::{
    persistence::Persistence, status::StatusServer, PersistenceClient, SinkConnectorError,
    StatusServerClient,
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
    type Error: error_stack::Context + Send + Sync + 'static;

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
    pub timeout_duration: Duration,
}

pub struct SinkConnectorOptions {
    pub stream: StreamConfiguration,
    pub persistence: Persistence,
    pub status_server: StatusServer,
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
    ) -> Result<(), SinkConnectorError>
    where
        F: Message + Default,
        B: Message + Default + Serialize,
    {
        let mut persistence = self
            .persistence
            .connect()
            .await
            .change_context(SinkConnectorError::Temporary)
            .attach_printable("failed to connect to persistence")?;

        let stream_client = self.new_stream_client().await?;

        let (status_client, mut status_server) = self
            .status_server
            .clone()
            .start(stream_client.clone(), ct.clone())
            .await
            .change_context(SinkConnectorError::Temporary)
            .attach_printable("failed to start status server")?;

        // Set starting cursor now, before it's modified.
        status_client
            .set_starting_cursor(configuration.starting_cursor.clone())
            .await
            .change_context(SinkConnectorError::Temporary)
            .attach_printable("failed to initialize status server")?;

        info!("acquiring persistence lock");
        // lock will block until it's acquired.
        // limit the time we wait for the lock to 30 seconds or until the cancellation token is
        // cancelled.
        // notice we can straight exit if the cancellation token is cancelled, since the lock
        // is not held by us.
        tokio::select! {
            ret = persistence.lock() => {
                ret.change_context(SinkConnectorError::Temporary)?;
                info!("lock acquired");
            }
            _ = tokio::time::sleep(Duration::from_secs(30)) => {
                info!("failed to acquire persistence lock within 30 seconds");
                return Err(SinkConnectorError::Configuration)
                    .attach_printable("failed to acquire persistence lock within 30 seconds");
            }
            _ = ct.cancelled() => {
                return Ok(())
            }
        }

        let starting_cursor = persistence
            .get_cursor()
            .await
            .change_context(SinkConnectorError::Temporary)
            .attach_printable("failed to get starting cursor")?;
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
            .change_context(SinkConnectorError::Temporary)
            .attach_printable("failed to send stream configuration")?;

        debug!("start consume stream");

        let mut data_stream = stream_client
            .start_stream::<F, B, _>(configuration_stream)
            .await
            .change_context(SinkConnectorError::Temporary)
            .attach_printable("failed to start stream")?;

        let mut ret = Ok(());
        loop {
            tokio::select! {
                _ = ct.cancelled() => {
                    break;
                }
                _ = &mut status_server => {
                    break;
                }
                maybe_message = data_stream.try_next() => {
                    match maybe_message {
                        Err(err) => {
                            ret = Err(err)
                                .change_context(SinkConnectorError::Temporary)
                                .attach_printable("data stream error");
                            break;
                        }
                        Ok(None) => {
                            ret = Err(SinkConnectorError::Temporary)
                                .attach_printable("data stream closed");
                            break;
                        }
                        Ok(Some(message)) => {
                            self.handle_message(message, &status_client, &mut persistence, ct.clone()).await?;
                        }
                    }
                }
            }
        }

        self.sink
            .cleanup()
            .await
            .change_context(SinkConnectorError::Temporary)
            .attach_printable("failed to cleanup sink")?;

        // unlock the lock, if any.
        persistence
            .unlock()
            .await
            .change_context(SinkConnectorError::Temporary)
            .attach_printable("failed to unlock persistence")?;

        ret
    }

    async fn new_stream_client(&self) -> Result<StreamClient, SinkConnectorError> {
        let mut stream_builder = ClientBuilder::default()
            .with_max_message_size(
                self.stream_configuration.max_message_size_bytes.as_u64() as usize
            )
            .with_metadata(self.stream_configuration.metadata.clone())
            .with_timeout(self.stream_configuration.timeout_duration);

        stream_builder = if let Some(bearer_token) = self.stream_configuration.bearer_token.clone()
        {
            stream_builder.with_bearer_token(Some(bearer_token))
        } else {
            stream_builder
        };

        let client = stream_builder
            .connect(self.stream_configuration.stream_url.clone())
            .await
            .change_context(SinkConnectorError::Temporary)
            .attach_printable("failed to connect to stream")?;

        Ok(client)
    }

    async fn handle_invalidate<P>(
        &mut self,
        cursor: Option<Cursor>,
        status_client: &StatusServerClient,
        persistence: &mut P,
        ct: CancellationToken,
    ) -> Result<(), SinkConnectorError>
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
                            persistence
                                .delete_cursor()
                                .await
                                .change_context(SinkConnectorError::Temporary)?;
                            status_client
                                .update_cursor(None)
                                .await
                                .change_context(SinkConnectorError::Temporary)?;
                        }
                        Some(cursor) => {
                            persistence
                                .put_cursor(cursor.clone())
                                .await
                                .change_context(SinkConnectorError::Temporary)?;
                            status_client
                                .update_cursor(Some(cursor))
                                .await
                                .change_context(SinkConnectorError::Temporary)?;
                        }
                    }
                    return Ok(());
                }
                Err(err) => {
                    warn!(err = ?err, "handle_invalidate error");
                    if ct.is_cancelled() {
                        return Err(err)
                            .change_context(SinkConnectorError::Fatal)
                            .attach_printable("handle invalidate failed and cancelled");
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

        Err(SinkConnectorError::Fatal).attach_printable("handle invalidate failed after retry")
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
    ) -> Result<(), SinkConnectorError>
    where
        B: Message + Default + Serialize,
        P: PersistenceClient + Send,
    {
        trace!(cursor = ?cursor, end_cursor = ?end_cursor, "received data");
        // fatal error since if the sink is restarted it will receive the same data again.
        let json_batch = serde_json::to_value(&batch)
            .change_context(SinkConnectorError::Fatal)
            .attach_printable("failed to serialize batch data")?;
        let data = self
            .script
            .transform(&json_batch)
            .await
            .change_context(SinkConnectorError::Fatal)
            .attach_printable("failed to transform batch data")?;

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
                        persistence
                            .put_cursor(end_cursor.clone())
                            .await
                            .change_context(SinkConnectorError::Temporary)?;
                        status_client
                            .update_cursor(Some(end_cursor))
                            .await
                            .change_context(SinkConnectorError::Temporary)?;
                    }

                    return Ok(());
                }
                Err(err) => {
                    warn!(err = ?err, "handle_data error");
                    if ct.is_cancelled() {
                        return Err(err)
                            .change_context(SinkConnectorError::Fatal)
                            .attach_printable("handle data failed and cancelled");
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

        Err(SinkConnectorError::Fatal).attach_printable("handle data failed after retry")
    }

    async fn handle_message<B, P>(
        &mut self,
        message: DataMessage<B>,
        status_client: &StatusServerClient,
        persistence: &mut P,
        ct: CancellationToken,
    ) -> Result<(), SinkConnectorError>
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
                    .change_context(SinkConnectorError::Fatal)
                    .attach_printable("handle heartbeat failed")?;
                status_client
                    .heartbeat()
                    .await
                    .change_context(SinkConnectorError::Temporary)
                    .attach_printable("failed to update status server after heartbeat")?;
                Ok(())
            }
        }
    }
}
