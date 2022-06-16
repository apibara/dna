//! Applications that handle on-chain and off-chain data.
use anyhow::{Error, Result};
use futures::{Future, FutureExt, StreamExt};
use std::{
    pin::Pin,
    sync::Arc,
    task::{self, Poll},
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, info, warn};

mod persistence;

pub use persistence::{ApplicationId, ApplicationPersistence, MongoPersistence};

use crate::{
    chain::{starknet::StarkNetProvider, BlockHeader, ChainProvider},
    indexer::{Indexer, IndexerConfig, IndexerHandle, IndexerStream},
};

/// Application root object.
#[derive(Debug)]
pub struct Application {
    /// application id
    pub id: ApplicationId,
    pub indexer_config: IndexerConfig,
}

/// Interact with a running application.
#[derive(Debug)]
pub struct ApplicationClient {
    tx: mpsc::Sender<ClientToApplicationMessage>,
}

#[derive(Debug)]
pub enum ApplicationState {
    Started { head: Option<BlockHeader> },
    NotStarted,
}

type Responder<T> = oneshot::Sender<Result<T>>;

#[derive(Debug)]
enum ClientToApplicationMessage {
    StartIndexing(Responder<IndexerStream>),
    GetState(Responder<ApplicationState>),
}

#[derive(Debug)]
struct ApplicationIndexer<P: ChainProvider> {
    config: IndexerConfig,
    provider: Arc<P>,
    inner: Option<IndexerHandle>,
}

impl Application {
    /// Create a new application with the given id and indexer.
    pub fn new(id: ApplicationId, indexer_config: IndexerConfig) -> Application {
        Application { id, indexer_config }
    }

    pub async fn start(self) -> Result<(JoinHandle<Result<()>>, ApplicationClient)> {
        // TODO: create storage based on user configuration.
        // TODO: need to create unique indexes on state. Maybe as an init step?
        let persistence =
            MongoPersistence::new_with_uri("mongodb://apibara:apibara@localhost:27017").await?;
        // TODO: don't reset state at every restart.
        // should use this to start the indexer component at the right block.
        if persistence.get_state(&self.id).await?.is_some() {
            persistence.delete_state(&self.id).await?;
        }

        // TODO: create provider based on user configuration.
        let provider = Arc::new(StarkNetProvider::new("http://192.168.8.100:9545")?);

        // TODO: configure buffer size?
        let (tx, mut rx) = mpsc::channel(128);

        let client = ApplicationClient { tx };

        let handle: JoinHandle<Result<()>> = tokio::spawn(async move {
            let mut indexer = ApplicationIndexer::new(provider, self.indexer_config.clone());

            info!("starting application service");
            loop {
                tokio::select! {
                    Some(msg) = rx.recv() => {
                        debug!("received message: {:?}", msg);
                        match msg {
                            ClientToApplicationMessage::StartIndexing(resp) => {
                                // TODO: read state to know at what block to start.
                                if indexer.is_started() {
                                    let _ = resp.send(Err(Error::msg("indexer already started")));
                                } else {
                                    let stream = indexer.start()?;
                                    let _ = resp.send(Ok(stream));
                                }
                            }
                            ClientToApplicationMessage::GetState(resp) => {
                                let state = if indexer.is_started() {
                                    ApplicationState::NotStarted
                                } else {
                                    let head = indexer.current_head();
                                    ApplicationState::Started { head }
                                };
                                let _ = resp.send(Ok(state));
                            }
                        }
                    }
                    _ = &mut indexer => {
                        // TODO: what to do?
                        warn!("indexer stopped");
                    }
                }
            }
        });

        Ok((handle, client))
    }
}

impl ApplicationClient {
    pub async fn start_indexing(&self) -> Result<IndexerStream> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(ClientToApplicationMessage::StartIndexing(tx))
            .await?;
        rx.await?
    }

    pub async fn application_state(&self) -> Result<ApplicationState> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(ClientToApplicationMessage::GetState(tx))
            .await?;
        rx.await?
    }
}

impl ApplicationState {
    pub fn is_started(&self) -> bool {
        !matches!(*self, ApplicationState::NotStarted)
    }
}

impl<P: ChainProvider> ApplicationIndexer<P> {
    pub fn new(provider: Arc<P>, config: IndexerConfig) -> ApplicationIndexer<P> {
        ApplicationIndexer {
            config,
            provider,
            inner: None,
        }
    }

    pub fn is_started(&self) -> bool {
        self.inner.is_some()
    }

    pub fn current_head(&self) -> Option<BlockHeader> {
        None
    }

    pub fn start(&mut self) -> Result<IndexerStream> {
        if self.inner.is_some() {
            return Err(Error::msg("indexer stream already started"));
        }

        // TODO: read from persistence.
        let indexer = Indexer::new(
            self.provider.clone(),
            self.config.from_block,
            self.config.filters.clone(),
        );

        let (tx, rx) = mpsc::channel(128);
        let handle: IndexerHandle = tokio::spawn(async move {
            // start inner indexer loop.
            // This loop is used to sync the raw indexer event stream with the client's
            // stream. The client needs to ack each block before the stream can continue.
            // This loop also updates the indexing state to persistence.
            let (mut handle, mut stream) = indexer.start()?;
            loop {
                tokio::select! {
                    _ = &mut handle => {
                        return Err(Error::msg("indexer stream stopped"));
                    }
                    Some(msg) = stream.next() => {
                        debug!("indexer msg: {:?}", msg);
                        tx.send(msg).await?;
                    }
                }
            }
        });

        self.inner = Some(handle);

        let stream = ReceiverStream::new(rx);

        Ok(stream)
    }
}

impl<P: ChainProvider> Future for ApplicationIndexer<P> {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
        if let Some(mut inner) = self.inner.take() {
            // poll inner handle. if the task finished resolve to the returned value
            // and reset the inner task.
            let (res, new_inner) = match inner.poll_unpin(cx) {
                Poll::Pending => (Poll::Pending, Some(inner)),
                Poll::Ready(Ok(join_res)) => (Poll::Ready(join_res), None),
                Poll::Ready(Err(join_err)) => {
                    let err = Error::new(join_err);
                    (Poll::Ready(Err(err)), None)
                }
            };
            self.inner = new_inner;
            res
        } else {
            Poll::Pending
        }
    }
}
