pub mod block_store;
pub mod chain;
pub mod chain_store;
pub mod chain_view;
pub mod cli;
pub mod compaction;
mod core;
pub mod data_stream;
pub mod dbg;
pub mod file_cache;
pub mod fragment;
pub mod index;
pub mod ingestion;
pub mod join;
pub mod object_store;
pub mod options_store;
pub mod query;
pub mod rkyv;
pub mod segment;
pub mod server;

pub use apibara_etcd as etcd;
use data_stream::BlockFilterFactory;
use fragment::FragmentInfo;
use ingestion::BlockIngestion;

pub use self::core::{testing::new_test_cursor, Cursor, GetCursor, Hash};

pub use self::cli::StartArgs;

pub trait ChainSupport {
    type BlockIngestion: BlockIngestion + Send + Sync + 'static;
    type BlockFilterFactory: BlockFilterFactory + Send + Sync + 'static;

    /// Returns the fragments generated by the chain.
    fn fragment_info(&self) -> Vec<FragmentInfo>;

    /// Returns the block ingestion service.
    fn block_ingestion(&self) -> Self::BlockIngestion;

    /// Returns the block filter factory.
    fn block_filter_factory(&self) -> Self::BlockFilterFactory;
}

pub use self::server_impl::{run_server, ServerError};

mod server_impl {
    use std::collections::HashMap;

    use crate::{
        block_store::BlockStoreReader, chain_view::chain_view_sync_loop,
        compaction::compaction_service_loop, fragment, ingestion::ingestion_service_loop,
        server::server_loop, ChainSupport, StartArgs,
    };
    use error_stack::ResultExt;
    use tokio_util::sync::CancellationToken;
    use tracing::info;

    #[derive(Debug)]
    pub struct ServerError;

    pub async fn run_server<CS>(
        chain_support: CS,
        args: StartArgs,
        version: &'static str,
        ct: CancellationToken,
    ) -> error_stack::Result<(), ServerError>
    where
        CS: ChainSupport,
    {
        emit_dna_up_metric(version);

        let object_store = args
            .object_store
            .into_object_store_client()
            .await
            .change_context(ServerError)?;
        let mut etcd_client = args
            .etcd
            .into_etcd_client()
            .await
            .change_context(ServerError)?;

        let status_response = etcd_client.status().await.change_context(ServerError)?;

        info!(
            version = status_response.version(),
            "connected to etcd cluster"
        );

        let file_cache = args
            .cache
            .to_file_cache()
            .await
            .change_context(ServerError)?;

        let ingestion_options = args
            .ingestion
            .to_ingestion_service_options()
            .change_context(ServerError)?;

        let etcd_renew_handle =
            tokio::spawn(etcd_client.clone().start_renew_auth_token(ct.clone()));

        let ingestion_handle = if args.ingestion.ingestion_enabled {
            let ingestion = chain_support.block_ingestion();
            tokio::spawn(ingestion_service_loop(
                ingestion,
                etcd_client.clone(),
                object_store.clone(),
                file_cache.clone(),
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

        let (chain_view, chain_view_sync) = chain_view_sync_loop(
            file_cache.clone(),
            etcd_client.clone(),
            object_store.clone(),
        )
        .await
        .change_context(ServerError)
        .attach_printable("failed to start chain view sync service")?;

        let sync_handle = tokio::spawn(chain_view_sync.start(ct.clone()));

        let compaction_handle = if args.compaction.compaction_enabled {
            let options = args.compaction.to_compaction_options();

            tokio::spawn(compaction_service_loop(
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

        let block_filter_factory = chain_support.block_filter_factory();
        let fragment_id_to_name = {
            let mut fragment_id_to_name = HashMap::from([
                (
                    fragment::HEADER_FRAGMENT_ID,
                    fragment::HEADER_FRAGMENT_NAME.to_string(),
                ),
                (
                    fragment::INDEX_FRAGMENT_ID,
                    fragment::INDEX_FRAGMENT_NAME.to_string(),
                ),
                (
                    fragment::JOIN_FRAGMENT_ID,
                    fragment::JOIN_FRAGMENT_NAME.to_string(),
                ),
            ]);

            for fragment_info in chain_support.fragment_info() {
                if let Some(existing) = fragment_id_to_name
                    .insert(fragment_info.fragment_id, fragment_info.name.clone())
                {
                    return Err(ServerError)
                        .attach_printable("duplicate fragment id")
                        .attach_printable_lazy(|| {
                            format!("fragment id: {}", fragment_info.fragment_id)
                        })
                        .attach_printable_lazy(|| format!("existing fragment name: {}", existing))
                        .attach_printable_lazy(|| {
                            format!("new fragment name: {}", fragment_info.name)
                        });
                }
            }

            fragment_id_to_name
        };

        let block_store = BlockStoreReader::new(object_store.clone(), file_cache.clone());

        let server_handle = if args.server.server_enabled {
            let options = args
                .server
                .to_server_options()
                .change_context(ServerError)?;

            tokio::spawn(server_loop(
                block_filter_factory,
                chain_view,
                fragment_id_to_name,
                block_store,
                options,
                ct,
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

        tokio::select! {
            etcd_renew = etcd_renew_handle => {
                info!("etcd auth token renewal loop terminated");
                etcd_renew.change_context(ServerError)?.change_context(ServerError)?;
            }
            ingestion = ingestion_handle => {
                info!("ingestion loop terminated");
                ingestion.change_context(ServerError)?.change_context(ServerError)?;
            }
            compaction = compaction_handle => {
                info!("compaction loop terminated");
                compaction.change_context(ServerError)?.change_context(ServerError)?;
            }
            sync = sync_handle => {
                info!("sync loop terminated");
                sync.change_context(ServerError)?.change_context(ServerError)?;
            }
            server = server_handle => {
                info!("server terminated");
                server.change_context(ServerError)?.change_context(ServerError)?;
            }
        }

        Ok(())
    }

    fn emit_dna_up_metric(version: &'static str) {
        use apibara_observability::KeyValue;

        let meter = apibara_observability::meter("dna");

        let up = meter
            .u64_gauge("dna.up")
            .with_description("DNA server is up")
            .build();

        up.record(1, &[KeyValue::new("version", version)]);
    }

    impl error_stack::Context for ServerError {}

    impl std::fmt::Display for ServerError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "DNA server error")
        }
    }
}
