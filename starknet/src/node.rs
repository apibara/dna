use std::{
    fs,
    marker::PhantomData,
    net::{AddrParseError, SocketAddr},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use apibara_node::db::{
    libmdbx::{self, Environment, EnvironmentKind},
    node_data_dir, MdbxEnvironmentExt,
};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    db::tables,
    ingestion::{BlockIngestion, BlockIngestionConfig, BlockIngestionError},
    provider::{HttpProviderError, Provider},
    server::{RequestSpan, Server, ServerError, SimpleRequestSpan},
    HttpProvider,
};

pub struct StarkNetNode<G, S, E>
where
    G: Provider + Send + Sync + 'static,
    S: RequestSpan,
    E: EnvironmentKind,
{
    db: Arc<Environment<E>>,
    sequencer_provider: Arc<G>,
    request_span: S,
}

#[derive(Debug, thiserror::Error)]
pub enum StarkNetNodeError {
    #[error("failed while ingesting blocks")]
    BlockIngestion(BlockIngestionError),
    #[error("database operation failed")]
    Database(#[from] libmdbx::Error),
    #[error("server error")]
    Server(#[from] ServerError),
    #[error("error parsing server address")]
    AddressParseError(#[from] AddrParseError),
}

impl<G, S, E> StarkNetNode<G, S, E>
where
    G: Provider + Send + Sync + 'static,
    S: RequestSpan,
    E: EnvironmentKind,
{
    /// Creates a new builder, used to configure the node.
    pub fn builder(
        url: &str,
    ) -> Result<StarkNetNodeBuilder<SimpleRequestSpan, E>, StarkNetNodeBuilderError> {
        StarkNetNodeBuilder::<SimpleRequestSpan, E>::new(url)
    }

    pub(crate) fn new(db: Environment<E>, sequencer_provider: G, request_span: S) -> Self {
        let db = Arc::new(db);
        let sequencer_provider = Arc::new(sequencer_provider);
        StarkNetNode {
            db,
            sequencer_provider,
            request_span,
        }
    }

    /// Starts the node.
    pub async fn start(self, ct: CancellationToken) -> Result<(), StarkNetNodeError> {
        info!("starting starknet node");
        self.ensure_tables()?;

        // TODO: config from command line
        let (block_ingestion_client, block_ingestion) = BlockIngestion::new(
            self.sequencer_provider.clone(),
            self.db.clone(),
            BlockIngestionConfig::default(),
        );

        let mut block_ingestion_handle = tokio::spawn({
            let ct = ct.clone();
            async move {
                block_ingestion
                    .start(ct)
                    .await
                    .map_err(StarkNetNodeError::BlockIngestion)
            }
        });

        // TODO: configure from command line
        let server_addr: SocketAddr = "0.0.0.0:7171".parse()?;
        let server = Server::new(self.db.clone(), block_ingestion_client)
            .with_request_span(self.request_span);
        let mut server_handle = tokio::spawn({
            let ct = ct.clone();
            async move {
                server
                    .start(server_addr, ct)
                    .await
                    .map_err(StarkNetNodeError::Server)
            }
        });

        // TODO: based on which handles terminates first, it needs to wait
        // for the other handle to terminate too.
        tokio::select! {
            ret = &mut block_ingestion_handle => {
                warn!(result = ?ret, "block ingestion terminated");
            }
            ret = &mut server_handle => {
                warn!(result = ?ret, "server terminated");
            }
        }

        info!("terminated. bye");
        Ok(())
    }

    fn ensure_tables(&self) -> Result<(), StarkNetNodeError> {
        let txn = self.db.begin_rw_txn()?;
        tables::ensure(&txn)?;
        txn.commit()?;
        Ok(())
    }
}

pub struct StarkNetNodeBuilder<S: RequestSpan, E: EnvironmentKind> {
    datadir: PathBuf,
    provider: HttpProvider,
    poll_interval: Duration,
    request_span: S,
    _phantom: PhantomData<E>,
}

#[derive(Debug, thiserror::Error)]
pub enum StarkNetNodeBuilderError {
    #[error("failed to create datadir")]
    CreateDatadir(std::io::Error),
    #[error("failed to open mdbx database")]
    DatabaseOpen(libmdbx::Error),
    #[error("failed to parse provider url")]
    ProviderUrl(#[from] url::ParseError),
    #[error("failed to create sequencer")]
    Provider(#[from] HttpProviderError),
}

impl<S, E> StarkNetNodeBuilder<S, E>
where
    S: RequestSpan,
    E: EnvironmentKind,
{
    pub(crate) fn new(
        url: &str,
    ) -> Result<StarkNetNodeBuilder<SimpleRequestSpan, E>, StarkNetNodeBuilderError> {
        let datadir = node_data_dir("starknet").expect("no datadir");
        let url = url.parse()?;
        let sequencer = HttpProvider::new(url);
        let poll_interval = Duration::from_millis(5_000);
        let request_span = SimpleRequestSpan::default();
        let builder = StarkNetNodeBuilder {
            datadir,
            provider: sequencer,
            poll_interval,
            request_span,
            _phantom: Default::default(),
        };
        Ok(builder)
    }

    pub fn with_datadir(&mut self, datadir: PathBuf) {
        self.datadir = datadir;
    }

    pub fn with_poll_interval(&mut self, poll_interval: Duration) {
        self.poll_interval = poll_interval;
    }

    pub fn with_request_span<SP: RequestSpan>(
        self,
        request_span: SP,
    ) -> StarkNetNodeBuilder<SP, E> {
        StarkNetNodeBuilder {
            datadir: self.datadir,
            provider: self.provider,
            poll_interval: self.poll_interval,
            request_span,
            _phantom: self._phantom,
        }
    }

    pub fn build(self) -> Result<StarkNetNode<HttpProvider, S, E>, StarkNetNodeBuilderError> {
        fs::create_dir_all(&self.datadir).map_err(StarkNetNodeBuilderError::CreateDatadir)?;

        let db = Environment::<E>::builder()
            .with_size_gib(10, 100)
            .with_growth_step_gib(2)
            .open(&self.datadir)
            .map_err(StarkNetNodeBuilderError::DatabaseOpen)?;

        Ok(StarkNetNode::new(db, self.provider, self.request_span))
    }
}
