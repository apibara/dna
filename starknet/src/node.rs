use std::{fs, marker::PhantomData, path::PathBuf, sync::Arc, time::Duration};

use apibara_node::db::{
    libmdbx::{self, Environment, EnvironmentKind},
    node_data_dir, MdbxEnvironmentExt,
};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    db::tables,
    ingestion::{BlockIngestion, BlockIngestionConfig, BlockIngestionError},
    provider::{HttpProviderError, Provider},
    HttpProvider,
};

pub struct StarkNetNode<G, E>
where
    G: Provider + Send + Sync + 'static,
    E: EnvironmentKind,
{
    db: Arc<Environment<E>>,
    sequencer_provider: Arc<G>,
    poll_interval: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum StarkNetNodeError {
    #[error("failed while ingesting blocks")]
    BlockIngestion(BlockIngestionError),
    #[error("database operation failed")]
    Database(#[from] libmdbx::Error),
}

impl<G, E> StarkNetNode<G, E>
where
    G: Provider + Send + Sync + 'static,
    E: EnvironmentKind,
{
    /// Creates a new builder, used to configure the node.
    pub fn builder(url: &str) -> Result<StarkNetNodeBuilder<E>, StarkNetNodeBuilderError> {
        StarkNetNodeBuilder::new(url)
    }

    pub(crate) fn new(db: Environment<E>, sequencer_provider: G, poll_interval: Duration) -> Self {
        let db = Arc::new(db);
        let sequencer_provider = Arc::new(sequencer_provider);
        StarkNetNode {
            db,
            sequencer_provider,
            poll_interval,
        }
    }

    /// Starts the node.
    pub async fn start(self, ct: CancellationToken) -> Result<(), StarkNetNodeError> {
        info!("starting starknet node");
        self.ensure_tables()?;

        // TODO: config from command line
        let block_ingestion = BlockIngestion::new(
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

        tokio::select! {
            ret = &mut block_ingestion_handle => {
                info!(
                    result = ?ret,
                    "block ingestion terminated"
                );
            }
        }
        todo!()
    }

    fn ensure_tables(&self) -> Result<(), StarkNetNodeError> {
        let txn = self.db.begin_rw_txn()?;
        tables::ensure(&txn)?;
        txn.commit()?;
        Ok(())
    }
}

pub struct StarkNetNodeBuilder<E: EnvironmentKind> {
    datadir: PathBuf,
    provider: HttpProvider,
    poll_interval: Duration,
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

impl<E> StarkNetNodeBuilder<E>
where
    E: EnvironmentKind,
{
    pub(crate) fn new(url: &str) -> Result<StarkNetNodeBuilder<E>, StarkNetNodeBuilderError> {
        let datadir = node_data_dir("starknet").expect("no datadir");
        let url = url.parse()?;
        let sequencer = HttpProvider::new(url);
        let poll_interval = Duration::from_millis(5_000);
        let builder = StarkNetNodeBuilder {
            datadir,
            provider: sequencer,
            poll_interval,
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

    pub fn build(self) -> Result<StarkNetNode<HttpProvider, E>, StarkNetNodeBuilderError> {
        fs::create_dir_all(&self.datadir).map_err(StarkNetNodeBuilderError::CreateDatadir)?;

        let db = Environment::<E>::builder()
            .with_size_gib(10, 100)
            .with_growth_step_gib(2)
            .open(&self.datadir)
            .map_err(StarkNetNodeBuilderError::DatabaseOpen)?;

        Ok(StarkNetNode::new(db, self.provider, self.poll_interval))
    }
}
