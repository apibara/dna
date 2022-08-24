//! # StarkNet source node
//!
//! This node indexes all StarkNet blocks and produces a stream of
//! blocks with transaction data.
use std::sync::Arc;

use apibara_node::db::{
    libmdbx::{Environment, EnvironmentKind, Error as MdbxError},
    MdbxRWTransactionExt, MdbxTransactionExt,
};
use starknet::providers::jsonrpc::{HttpTransport, JsonRpcClient};
use tokio_util::sync::CancellationToken;
use tracing::info;
use url::Url;

use crate::{
    block_builder::{self, BlockBuilder},
    chain_tracker::{StarkNetChainTracker, StarkNetChainTrackerError},
    db::tables,
};

#[derive(Debug)]
pub struct StarkNetSourceNode<E: EnvironmentKind> {
    db: Arc<Environment<E>>,
}

#[derive(Debug, thiserror::Error)]
pub enum SourceNodeError {
    #[error("error tracking the chain state")]
    ChainTracker(#[from] StarkNetChainTrackerError),
    #[error("database error")]
    Database(#[from] MdbxError),
    #[error("error setting up signal handler")]
    SignalHandler(#[from] ctrlc::Error),
    #[error("error parsing url")]
    UrlParse(#[from] url::ParseError),
}

pub type Result<T> = std::result::Result<T, SourceNodeError>;

impl<E: EnvironmentKind> StarkNetSourceNode<E> {
    pub fn new(db: Arc<Environment<E>>) -> Result<Self> {
        let txn = db.begin_rw_txn()?;
        txn.ensure_table::<tables::NodeStateTable>(None)?;
        txn.commit()?;
        Ok(StarkNetSourceNode { db })
    }

    pub async fn start(self) -> Result<()> {
        // TODO: Check latest indexed block.
        // - compare stored hash with live hash to detect reorgs
        //   while node was not running.
        let state = self.get_state()?;

        // Setup cancellation for graceful shutdown
        let cts = CancellationToken::new();
        let ct = cts.clone();
        ctrlc::set_handler(move || {
            cts.cancel();
        })?;

        // Now start the node and setup the channels between the different subsystems.
        // let starknet_provider_url = Url::parse("https://starknet-goerli.apibara.com")?;
        // let starknet_client = JsonRpcClient::new(HttpTransport::new(starknet_provider_url));
        // let starknet_client = Arc::new(starknet_client);

        let block_builder = BlockBuilder::new();

        let xxx = block_builder.latest_block().await.unwrap();
        info!("{:?}", xxx);

        Ok(())
    }

    pub fn get_state(&self) -> Result<tables::NodeState> {
        let txn = self.db.begin_ro_txn()?;
        let db = txn.open_table::<tables::NodeStateTable>()?;
        let mut cursor = db.cursor()?;
        let state = cursor.seek_exact(&())?.map(|d| d.1).unwrap_or_default();
        txn.commit()?;
        Ok(state)
    }
}
