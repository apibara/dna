use apibara_dna_common::{
    block_store::BlockStoreReader,
    chain_view::chain_view_sync_loop,
    cli::{EtcdArgs, ObjectStoreArgs},
    compaction::{compaction_service_loop, CompactionArgs},
    file_cache::FileCache,
    ingestion::{ingestion_service_loop, IngestionArgs},
    server::{server_loop, ServerArgs},
};
use clap::Args;
use error_stack::{Result, ResultExt};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    cli::rpc::RpcArgs, error::BeaconChainError, ingestion::BeaconChainBlockIngestion,
    scanner::BeaconChainScannerFactory, segment::BeaconChainSegmentBuilder,
};

#[derive(Args, Debug)]
pub struct StartCommand {
    #[clap(flatten)]
    rpc: RpcArgs,
    #[clap(flatten)]
    object_store: ObjectStoreArgs,
    #[clap(flatten)]
    etcd: EtcdArgs,
    #[clap(flatten)]
    ingestion: IngestionArgs,
    #[clap(flatten)]
    compaction: CompactionArgs,
    #[clap(flatten)]
    server: ServerArgs,
}

impl StartCommand {
    pub async fn run(self, ct: CancellationToken) -> Result<(), BeaconChainError> {
        info!("Starting Beaconchain DNA server");
        let provider = self.rpc.to_beacon_api_provider()?;
        let object_store = self.object_store.into_object_store_client().await;
        let mut etcd_client = self
            .etcd
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

        let ingestion_handle = if self.ingestion.ingestion_enabled {
            let ingestion_options = self.ingestion.to_ingestion_options();
            let ingestion = BeaconChainBlockIngestion::new(provider);
            tokio::spawn(ingestion_service_loop(
                ingestion,
                etcd_client.clone(),
                object_store.clone(),
                ingestion_options,
                ct.clone(),
            ))
        } else {
            tokio::spawn({
                let ct = ct.clone();
                async move {
                    ct.cancelled().await;
                    Ok(())
                }
            })
        };

        let file_cache_options = self
            .server
            .to_file_cache_options()
            .change_context(BeaconChainError)?;
        let file_cache = FileCache::new(file_cache_options);
        file_cache
            .restore_from_disk()
            .await
            .change_context(BeaconChainError)?;

        let (chain_view, chain_view_sync) = chain_view_sync_loop(
            file_cache.clone(),
            etcd_client.clone(),
            object_store.clone(),
        )
        .await
        .change_context(BeaconChainError)
        .attach_printable("failed to start chain view sync service")?;

        let sync_handle = tokio::spawn(chain_view_sync.start(ct.clone()));

        let compaction_handle = if self.compaction.compaction_enabled {
            let options = self.compaction.to_compaction_options();
            let builder = BeaconChainSegmentBuilder::default();

            tokio::spawn(compaction_service_loop(
                builder,
                etcd_client.clone(),
                object_store.clone(),
                chain_view.clone(),
                options,
                ct.clone(),
            ))
        } else {
            tokio::spawn({
                let ct = ct.clone();
                async move {
                    ct.cancelled().await;
                    Ok(())
                }
            })
        };

        let data_store = BlockStoreReader::new(object_store.clone(), file_cache.clone());
        let scanner_factory = BeaconChainScannerFactory::new(data_store);

        let server_handle = if self.server.server_enabled {
            let options = self
                .server
                .to_server_options()
                .change_context(BeaconChainError)?;
            tokio::spawn(server_loop(scanner_factory, chain_view, options, ct))
        } else {
            tokio::spawn({
                let ct = ct.clone();
                async move {
                    ct.cancelled().await;
                    Ok(())
                }
            })
        };

        tokio::select! {
            ingestion = ingestion_handle => {
                ingestion.change_context(BeaconChainError)?.change_context(BeaconChainError)?;
            }
            compaction = compaction_handle => {
                compaction.change_context(BeaconChainError)?.change_context(BeaconChainError)?;
            }
            sync = sync_handle => {
                sync.change_context(BeaconChainError)?.change_context(BeaconChainError)?;
            }
            server = server_handle => {
                server.change_context(BeaconChainError)?.change_context(BeaconChainError)?;
            }
        }

        Ok(())
    }
}
