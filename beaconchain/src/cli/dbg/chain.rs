use apibara_dna_common::{
    cli::{EtcdArgs, ObjectStoreArgs},
    ingestion::{IngestionService, IngestionServiceOptions},
};
use clap::Subcommand;
use error_stack::{Result, ResultExt};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{cli::rpc::RpcArgs, error::BeaconChainError, ingestion::BeaconChainBlockIngestion};

#[derive(Subcommand, Debug)]
pub enum DebugChainCommand {
    /// Create and upload the canonical chain.
    Create {
        #[clap(flatten)]
        rpc: RpcArgs,
        #[clap(flatten)]
        object_store: ObjectStoreArgs,
        #[clap(flatten)]
        etcd: EtcdArgs,
    },
}

impl DebugChainCommand {
    pub async fn run(self) -> Result<(), BeaconChainError> {
        match self {
            DebugChainCommand::Create {
                rpc,
                object_store,
                etcd,
            } => {
                let provider = rpc.to_beacon_api_provider()?;
                let object_store = object_store.into_object_store_client().await;
                let mut etcd_client = etcd
                    .into_etcd_client()
                    .await
                    .change_context(BeaconChainError)?;

                let status_response = etcd_client
                    .status()
                    .await
                    .change_context(BeaconChainError)?;

                info!(
                    version = status_response.version(),
                    "connected to etcd cluster"
                );

                let ingestion = BeaconChainBlockIngestion::new(provider);
                let ingestion_service = IngestionService::new(
                    ingestion,
                    etcd_client,
                    object_store,
                    IngestionServiceOptions {
                        chain_segment_size: 1000,
                        ..Default::default()
                    },
                );

                let ct = CancellationToken::new();

                ingestion_service
                    .start(ct)
                    .await
                    .change_context(BeaconChainError)
                    .attach_printable("ingestion service error")?;

                Ok(())
            }
        }
    }
}
