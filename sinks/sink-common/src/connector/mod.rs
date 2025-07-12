pub mod batching;
mod default;
mod factory;
mod sink;
mod state;
mod stream;

use std::time::Duration;

use apibara_core::filter::Filter;
use apibara_script::Script;
use apibara_sdk::{Configuration, MetadataMap, Uri};
use bytesize::ByteSize;
use error_stack::Result;
use exponential_backoff::Backoff;
use prost::Message;
use serde::ser::Serialize;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    connector::{state::StateManager, stream::StreamClientFactory},
    error::{SinkError, SinkErrorReportExt},
    persistence::Persistence,
    sink::Sink,
    status::StatusServer,
};

pub use self::{default::DefaultConnector, factory::FactoryConnector, sink::SinkWithBackoff};

#[derive(Debug)]
pub struct StreamConfiguration {
    pub stream_url: Uri,
    pub max_message_size_bytes: ByteSize,
    pub metadata: MetadataMap,
    pub bearer_token: Option<String>,
    pub timeout_duration: Duration,
    pub ending_block: Option<u64>,
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
}

impl<S> SinkConnector<S>
where
    S: Sink + Send + Sync,
{
    /// Creates a new connector with the given stream URL.
    pub fn new(script: Script, sink: S, options: SinkConnectorOptions) -> Self {
        Self {
            script,
            sink,
            backoff: default_backoff(),
            stream_configuration: options.stream,
            persistence: options.persistence,
            status_server: options.status_server,
        }
    }

    /// Start consuming the stream, calling the configured callback for each message.
    pub async fn consume_stream<F, B>(
        mut self,
        configuration: Configuration<F>,
        ct: CancellationToken,
    ) -> Result<(), SinkError>
    where
        F: Filter,
        B: Message + Default + Serialize,
    {
        let stream_ending_block = self.stream_configuration.ending_block;

        let stream_client_factory = StreamClientFactory::new(self.stream_configuration);
        let stream_client = stream_client_factory.new_stream_client().await?;

        let (state_manager, mut state_manager_fut) = StateManager::start(
            self.persistence,
            self.status_server,
            stream_client,
            ct.clone(),
        )
        .await?;

        let use_factory_mode = self
            .script
            .has_factory()
            .await
            .map_err(|err| err.configuration("failed to detect mode"))?;

        let sink = SinkWithBackoff::new(self.sink, self.backoff);

        let mut inner = if use_factory_mode {
            InnerConnector::<S, F, B>::new_factory(
                self.script,
                sink,
                stream_ending_block,
                configuration,
                stream_client_factory,
                state_manager,
            )
        } else {
            InnerConnector::<S, F, B>::new_default(
                self.script,
                sink,
                stream_ending_block,
                configuration,
                stream_client_factory,
                state_manager,
            )
        };

        loop {
            let inner_fut = inner.start(ct.clone());
            tokio::select! {
                _ = &mut state_manager_fut => {
                    info!("status server stopped");
                    break;
                }
                _ = ct.cancelled() => {
                    break;
                }
                ret = inner_fut => {
                    match ret {
                        Ok(_) => {
                            info!("connector stopped.");
                            break;
                        }
                        Err(err) => {
                            match err.downcast_ref::<SinkError>() {
                                Some(SinkError::Temporary) => {
                                    warn!(err = ?err, "connector failed. restarting.");
                                }
                                _ => {
                                    return Err(err);
                                }
                            };
                        }
                    }
                }
            };

            // Wait before restarting.
            tokio::select! {
                _ = ct.cancelled() => {
                    break;
                }
                _ = tokio::time::sleep(Duration::from_secs(10)) => {
                    // continue
                }
            }
        }

        Ok(())
    }
}

enum InnerConnector<S, F, B>
where
    S: Sink + Send + Sync,
    F: Filter,
    B: Message + Default + Serialize,
{
    Default(DefaultConnector<S, F, B>),
    Factory(FactoryConnector<S, F, B>),
}

impl<S, F, B> InnerConnector<S, F, B>
where
    S: Sink + Send + Sync,
    F: Filter,
    B: Message + Default + Serialize,
{
    pub fn new_default(
        script: Script,
        sink: SinkWithBackoff<S>,
        ending_block: Option<u64>,
        starting_configuration: Configuration<F>,
        stream_client_factory: StreamClientFactory,
        state_manager: StateManager,
    ) -> Self {
        let inner = DefaultConnector::new(
            script,
            sink,
            ending_block,
            starting_configuration,
            stream_client_factory,
            state_manager,
        );
        Self::Default(inner)
    }

    pub fn new_factory(
        script: Script,
        sink: SinkWithBackoff<S>,
        ending_block: Option<u64>,
        starting_configuration: Configuration<F>,
        stream_client_factory: StreamClientFactory,
        state_manager: StateManager,
    ) -> Self {
        let inner = FactoryConnector::new(
            script,
            sink,
            ending_block,
            starting_configuration,
            stream_client_factory,
            state_manager,
        );
        Self::Factory(inner)
    }

    pub async fn start(&mut self, ct: CancellationToken) -> Result<(), SinkError> {
        match self {
            Self::Default(inner) => inner.start(ct).await,
            Self::Factory(inner) => inner.start(ct).await,
        }
    }
}

fn default_backoff() -> Backoff {
    let retries = 10;
    let min_delay = Duration::from_secs(3);
    let max_delay = Duration::from_secs(60);
    let mut backoff = Backoff::new(retries, min_delay, Some(max_delay));
    backoff.set_factor(3);
    backoff
}
