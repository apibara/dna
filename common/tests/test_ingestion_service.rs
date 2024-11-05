use std::sync::Arc;

use alloy_rpc_types::BlockNumberOrTag;
use apibara_etcd::EtcdClient;
use error_stack::{Result, ResultExt};
use foyer::HybridCacheBuilder;
use testcontainers::{runners::AsyncRunner, ContainerAsync};

use apibara_dna_common::{
    chain::BlockInfo,
    file_cache::FileCache,
    fragment,
    ingestion::{
        state_client::testing::{etcd_server_container, EtcdServer, EtcdServerExt},
        BlockIngestion, IngestionError, IngestionService, IngestionServiceOptions,
        IngestionStateClient,
    },
    object_store::{
        testing::{minio_container, MinIO, MinIOExt},
        ObjectStore, ObjectStoreOptions,
    },
    Cursor, Hash,
};
use testing::{
    anvil_server_container, AnvilProvider, AnvilProviderExt, AnvilServer, AnvilServerExt,
};

async fn init_minio() -> (ContainerAsync<MinIO>, ObjectStore) {
    let minio = minio_container().start().await.unwrap();
    let config = minio.s3_config().await;

    let client = ObjectStore::new_from_config(
        config,
        ObjectStoreOptions {
            bucket: "test".to_string(),
            ..Default::default()
        },
    );

    client.ensure_bucket().await.unwrap();

    (minio, client)
}

async fn init_etcd_server() -> (ContainerAsync<EtcdServer>, EtcdClient) {
    let etcd_server = etcd_server_container().start().await.unwrap();
    let etcd_client = etcd_server.etcd_client().await;

    (etcd_server, etcd_client)
}

async fn init_anvil() -> (ContainerAsync<AnvilServer>, Arc<AnvilProvider>) {
    let anvil_server = anvil_server_container().start().await.unwrap();

    let provider = anvil_server.alloy_provider().await;
    (anvil_server, provider)
}

async fn init_file_cache() -> FileCache {
    HybridCacheBuilder::default()
        .memory(1024 * 1024)
        .storage(foyer::Engine::Large)
        .build()
        .await
        .expect("failed to create file cache")
}

#[tokio::test]
async fn test_ingestion_initialize() {
    let (_minio, object_store) = init_minio().await;
    let (_etcd_server, etcd_client) = init_etcd_server().await;
    let (_anvil_server, anvil_provider) = init_anvil().await;

    let mut state_client = IngestionStateClient::new(&etcd_client);
    let file_cache = init_file_cache().await;

    let block_ingestion = TestBlockIngestion {
        provider: anvil_provider.clone(),
    };

    let mut service = IngestionService::new(
        block_ingestion,
        etcd_client,
        object_store,
        file_cache,
        IngestionServiceOptions::default(),
    );

    anvil_provider.anvil_mine(100, 3).await;
    let header = anvil_provider.get_header(BlockNumberOrTag::Latest).await;
    assert_eq!(header.number, 100);

    let starting_state = service.initialize().await.unwrap();
    let ingest_state = starting_state.as_ingest().unwrap();

    assert_eq!(ingest_state.head.number, 100);

    let starting_block = state_client.get_starting_block().await.unwrap();
    assert_eq!(starting_block, Some(0));

    let finalized = state_client.get_finalized().await.unwrap();
    assert!(finalized.is_some());
    assert_eq!(ingest_state.finalized.number, finalized.unwrap());

    // Ingested is updated only after a chain segment is uploaded.
    let ingested = state_client.get_ingested().await.unwrap();
    assert!(ingested.is_none());
}

#[tokio::test]
async fn test_ingestion_initialize_with_starting_block() {
    let (_minio, object_store) = init_minio().await;
    let (_etcd_server, etcd_client) = init_etcd_server().await;
    let (_anvil_server, anvil_provider) = init_anvil().await;

    let mut state_client = IngestionStateClient::new(&etcd_client);
    let file_cache = init_file_cache().await;

    let block_ingestion = TestBlockIngestion {
        provider: anvil_provider.clone(),
    };

    let options = IngestionServiceOptions {
        override_starting_block: Some(100),
        ..Default::default()
    };

    let mut service = IngestionService::new(
        block_ingestion,
        etcd_client,
        object_store,
        file_cache,
        options,
    );

    anvil_provider.anvil_mine(200, 3).await;
    let header = anvil_provider.get_header(BlockNumberOrTag::Latest).await;
    assert_eq!(header.number, 200);

    let starting_state = service.initialize().await.unwrap();
    let ingest_state = starting_state.as_ingest().unwrap();

    assert_eq!(ingest_state.head.number, 200);

    let starting_block = state_client.get_starting_block().await.unwrap();
    assert_eq!(starting_block, Some(100));

    let finalized = state_client.get_finalized().await.unwrap();
    assert!(finalized.is_some());
    assert_eq!(ingest_state.finalized.number, finalized.unwrap());

    // Ingested is updated only after a chain segment is uploaded.
    let ingested = state_client.get_ingested().await.unwrap();
    assert!(ingested.is_none());
}

#[tokio::test]
async fn test_ingestion_advances_as_head_changes() {
    let (_minio, object_store) = init_minio().await;
    let (_etcd_server, etcd_client) = init_etcd_server().await;
    let (_anvil_server, anvil_provider) = init_anvil().await;

    let mut state_client = IngestionStateClient::new(&etcd_client);
    let file_cache = init_file_cache().await;

    let block_ingestion = TestBlockIngestion {
        provider: anvil_provider.clone(),
    };

    let options = IngestionServiceOptions {
        chain_segment_size: 10,
        chain_segment_upload_offset_size: 1,
        max_concurrent_tasks: 5,
        ..Default::default()
    };

    let mut service = IngestionService::new(
        block_ingestion,
        etcd_client,
        object_store,
        file_cache,
        options,
    );

    anvil_provider.anvil_mine(10, 3).await;
    let header = anvil_provider.get_header(BlockNumberOrTag::Latest).await;
    assert_eq!(header.number, 10);

    let starting_state = service.initialize().await.unwrap();
    assert_eq!(service.task_queue_len(), 0);

    // Nothing changed, so state is the same.
    let state = starting_state.take_ingest().unwrap();
    let prev_head = state.head.clone();
    let state = service.tick_refresh_head(state).await.unwrap();
    let state = state.take_ingest().unwrap();
    assert_eq!(service.task_queue_len(), 0);
    assert_eq!(state.head, prev_head);

    // We need a lot of blocks to push the finalized block forward.
    anvil_provider.anvil_mine(100, 3).await;
    let header = anvil_provider.get_header(BlockNumberOrTag::Latest).await;
    assert_eq!(header.number, 110);

    let state = service.tick_refresh_head(state).await.unwrap();
    let state = state.take_ingest().unwrap();
    let state = service.tick_refresh_finalized(state).await.unwrap();
    let state = state.take_ingest().unwrap();

    assert_eq!(service.task_queue_len(), 5);
    assert_eq!(state.head.number, header.number);
    assert_eq!(state.queued_block_number, 5);

    let mut state = Some(state);
    for offset in 0..5 {
        let join_result = service.task_queue_next().await;
        let next_state = service
            .tick_with_task_result(state.take().unwrap(), join_result)
            .await
            .unwrap();
        let next_state = next_state.take_ingest().unwrap();
        assert_eq!(next_state.queued_block_number, 5 + offset + 1);
        state = Some(next_state);
    }

    // Not enough blocks ingested yet.
    let ingested = state_client.get_ingested().await.unwrap();
    assert!(ingested.is_none());

    for _ in 0..4 {
        let join_result = service.task_queue_next().await;
        let next_state = service
            .tick_with_task_result(state.take().unwrap(), join_result)
            .await
            .unwrap();
        let next_state = next_state.take_ingest().unwrap();
        state = Some(next_state);
    }

    let ingested = state_client.get_ingested().await.unwrap();
    assert!(ingested.is_none());

    let join_result = service.task_queue_next().await;
    service
        .tick_with_task_result(state.take().unwrap(), join_result)
        .await
        .unwrap();

    let ingested = state_client.get_ingested().await.unwrap();
    assert!(ingested.is_some());
}

#[tokio::test]
async fn test_ingestion_not_affected_by_reorg_after_ingested_block() {
    let (_minio, object_store) = init_minio().await;
    let (_etcd_server, etcd_client) = init_etcd_server().await;
    let (_anvil_server, anvil_provider) = init_anvil().await;

    let file_cache = init_file_cache().await;

    let block_ingestion = TestBlockIngestion {
        provider: anvil_provider.clone(),
    };

    let options = IngestionServiceOptions {
        chain_segment_size: 10,
        chain_segment_upload_offset_size: 1,
        max_concurrent_tasks: 5,
        ..Default::default()
    };

    let mut service = IngestionService::new(
        block_ingestion,
        etcd_client,
        object_store,
        file_cache,
        options,
    );

    anvil_provider.anvil_mine(100, 3).await;
    let header = anvil_provider.get_header(BlockNumberOrTag::Latest).await;
    assert_eq!(header.number, 100);

    let starting_state = service.initialize().await.unwrap();
    assert_eq!(service.task_queue_len(), 0);

    let state = starting_state.take_ingest().unwrap();
    let state = service.tick_refresh_head(state).await.unwrap();
    let state = state.take_ingest().unwrap();
    let state = service.tick_refresh_finalized(state).await.unwrap();
    let state = state.take_ingest().unwrap();
    let prev_head = state.head.clone();
    assert_eq!(service.task_queue_len(), 0);
    assert_eq!(state.head.number, 100);

    anvil_provider.anvil_reorg(5).await;
    let header = anvil_provider.get_header(BlockNumberOrTag::Latest).await;
    assert_eq!(header.number, 100);

    let state = service.tick_refresh_head(state).await.unwrap();
    let state = state.take_ingest().unwrap();
    assert_ne!(state.head, prev_head);
}

#[tokio::test]
async fn test_ingestion_detect_shrinking_reorg_on_head_refresh() {
    let (_minio, object_store) = init_minio().await;
    let (_etcd_server, etcd_client) = init_etcd_server().await;
    let (_anvil_server, anvil_provider) = init_anvil().await;

    let file_cache = init_file_cache().await;

    let block_ingestion = TestBlockIngestion {
        provider: anvil_provider.clone(),
    };

    let options = IngestionServiceOptions {
        chain_segment_size: 10,
        chain_segment_upload_offset_size: 1,
        max_concurrent_tasks: 5,
        ..Default::default()
    };

    let mut service = IngestionService::new(
        block_ingestion,
        etcd_client,
        object_store,
        file_cache,
        options,
    );

    anvil_provider.anvil_mine(90, 3).await;
    let snapshot_id = anvil_provider.anvil_snapshot().await;
    anvil_provider.anvil_mine(10, 3).await;
    let header = anvil_provider.get_header(BlockNumberOrTag::Latest).await;
    assert_eq!(header.number, 100);

    let starting_state = service.initialize().await.unwrap();
    assert_eq!(service.task_queue_len(), 0);

    let state = starting_state.take_ingest().unwrap();
    let state = service.tick_refresh_head(state).await.unwrap();
    let state = state.take_ingest().unwrap();
    let state = service.tick_refresh_finalized(state).await.unwrap();
    let state = state.take_ingest().unwrap();

    assert_eq!(service.task_queue_len(), 0);
    assert_eq!(state.head.number, 100);

    let mut state = Some(state);
    for _ in 0..=100 {
        let join_result = service.task_queue_next().await;
        let next_state = service
            .tick_with_task_result(state.take().unwrap(), join_result)
            .await
            .unwrap();
        let next_state = next_state.take_ingest().unwrap();
        state = Some(next_state);
    }

    let state = state.unwrap();
    assert_eq!(state.last_ingested.number, 100);
    assert_eq!(service.task_queue_len(), 0);

    anvil_provider.anvil_revert(snapshot_id).await;
    anvil_provider.anvil_mine(5, 13).await;
    let header = anvil_provider.get_header(BlockNumberOrTag::Latest).await;
    assert_eq!(header.number, 95);

    let state = service.tick_refresh_head(state).await.unwrap();
    assert!(state.is_recover());
}

#[tokio::test]
async fn test_ingestion_detect_shrinking_reorg_on_block_ingestion() {
    let (_minio, object_store) = init_minio().await;
    let (_etcd_server, etcd_client) = init_etcd_server().await;
    let (_anvil_server, anvil_provider) = init_anvil().await;

    let file_cache = init_file_cache().await;

    let block_ingestion = TestBlockIngestion {
        provider: anvil_provider.clone(),
    };

    let options = IngestionServiceOptions {
        chain_segment_size: 10,
        chain_segment_upload_offset_size: 1,
        max_concurrent_tasks: 5,
        ..Default::default()
    };

    let mut service = IngestionService::new(
        block_ingestion,
        etcd_client,
        object_store,
        file_cache,
        options,
    );

    anvil_provider.anvil_mine(90, 3).await;
    let snapshot_id = anvil_provider.anvil_snapshot().await;
    anvil_provider.anvil_mine(10, 3).await;
    let header = anvil_provider.get_header(BlockNumberOrTag::Latest).await;
    assert_eq!(header.number, 100);

    let starting_state = service.initialize().await.unwrap();
    assert_eq!(service.task_queue_len(), 0);

    let state = starting_state.take_ingest().unwrap();
    let state = service.tick_refresh_head(state).await.unwrap();
    let state = state.take_ingest().unwrap();
    let state = service.tick_refresh_finalized(state).await.unwrap();
    let state = state.take_ingest().unwrap();

    assert_eq!(service.task_queue_len(), 0);
    assert_eq!(state.head.number, 100);

    let mut state = Some(state);
    for _ in 0..=100 {
        let join_result = service.task_queue_next().await;
        let next_state = service
            .tick_with_task_result(state.take().unwrap(), join_result)
            .await
            .unwrap();
        let next_state = next_state.take_ingest().unwrap();
        state = Some(next_state);
    }

    let state = state.unwrap();
    assert_eq!(state.last_ingested.number, 100);
    assert_eq!(service.task_queue_len(), 0);

    anvil_provider.anvil_revert(snapshot_id).await;
    anvil_provider.anvil_mine(5, 13).await;
    let header = anvil_provider.get_header(BlockNumberOrTag::Latest).await;
    assert_eq!(header.number, 95);

    // Simulate a task queued before the reorg happened.
    service.push_ingest_block_by_number(101);
    let join_result = service.task_queue_next().await;
    let state = service
        .tick_with_task_result(state, join_result)
        .await
        .unwrap();

    assert!(state.is_recover());
}

#[tokio::test]
async fn test_ingestion_detect_offline_reorg() {
    let (_minio, object_store) = init_minio().await;
    let (_etcd_server, etcd_client) = init_etcd_server().await;
    let (_anvil_server, anvil_provider) = init_anvil().await;

    let file_cache = init_file_cache().await;

    let block_ingestion = TestBlockIngestion {
        provider: anvil_provider.clone(),
    };

    let options = IngestionServiceOptions {
        chain_segment_size: 10,
        chain_segment_upload_offset_size: 1,
        max_concurrent_tasks: 5,
        ..Default::default()
    };

    let mut service = IngestionService::new(
        block_ingestion.clone(),
        etcd_client.clone(),
        object_store.clone(),
        file_cache.clone(),
        options.clone(),
    );

    anvil_provider.anvil_mine(100, 3).await;
    let header = anvil_provider.get_header(BlockNumberOrTag::Latest).await;
    assert_eq!(header.number, 100);

    let starting_state = service.initialize().await.unwrap();
    assert_eq!(service.task_queue_len(), 0);

    let state = starting_state.take_ingest().unwrap();
    let state = service.tick_refresh_head(state).await.unwrap();
    let state = state.take_ingest().unwrap();
    let state = service.tick_refresh_finalized(state).await.unwrap();
    let state = state.take_ingest().unwrap();

    assert_eq!(service.task_queue_len(), 0);
    assert_eq!(state.head.number, 100);

    let mut state = Some(state);
    for _ in 0..=100 {
        let join_result = service.task_queue_next().await;
        let next_state = service
            .tick_with_task_result(state.take().unwrap(), join_result)
            .await
            .unwrap();
        let next_state = next_state.take_ingest().unwrap();
        state = Some(next_state);
    }

    anvil_provider.anvil_reorg(10).await;
    anvil_provider.anvil_mine(20, 7).await;

    let mut service = IngestionService::new(
        block_ingestion,
        etcd_client,
        object_store,
        file_cache,
        options,
    );

    let starting_state = service.initialize().await.unwrap();
    assert_eq!(service.task_queue_len(), 0);
    assert!(starting_state.is_recover());
}

#[tokio::test]
async fn test_ingestion_detect_reorg_on_head_refresh() {
    let (_minio, object_store) = init_minio().await;
    let (_etcd_server, etcd_client) = init_etcd_server().await;
    let (_anvil_server, anvil_provider) = init_anvil().await;

    let file_cache = init_file_cache().await;

    let block_ingestion = TestBlockIngestion {
        provider: anvil_provider.clone(),
    };

    let options = IngestionServiceOptions {
        chain_segment_size: 10,
        chain_segment_upload_offset_size: 1,
        max_concurrent_tasks: 5,
        ..Default::default()
    };

    let mut service = IngestionService::new(
        block_ingestion,
        etcd_client,
        object_store,
        file_cache,
        options,
    );

    anvil_provider.anvil_mine(100, 3).await;
    let header = anvil_provider.get_header(BlockNumberOrTag::Latest).await;
    assert_eq!(header.number, 100);

    let starting_state = service.initialize().await.unwrap();
    assert_eq!(service.task_queue_len(), 0);

    let state = starting_state.take_ingest().unwrap();
    let state = service.tick_refresh_head(state).await.unwrap();
    let state = state.take_ingest().unwrap();
    let state = service.tick_refresh_finalized(state).await.unwrap();
    let state = state.take_ingest().unwrap();

    assert_eq!(service.task_queue_len(), 0);
    assert_eq!(state.head.number, 100);

    let mut state = Some(state);
    for _ in 0..=100 {
        let join_result = service.task_queue_next().await;
        let next_state = service
            .tick_with_task_result(state.take().unwrap(), join_result)
            .await
            .unwrap();
        let next_state = next_state.take_ingest().unwrap();
        state = Some(next_state);
    }

    let state = state.unwrap();
    assert_eq!(state.last_ingested.number, 100);
    assert_eq!(service.task_queue_len(), 0);

    anvil_provider.anvil_reorg(10).await;
    let header = anvil_provider.get_header(BlockNumberOrTag::Latest).await;
    assert_eq!(header.number, 100);

    let state = service.tick_refresh_head(state).await.unwrap();
    assert!(state.is_recover());
}

#[tokio::test]
async fn test_ingestion_detect_reorg_on_block_ingestion() {
    let (_minio, object_store) = init_minio().await;
    let (_etcd_server, etcd_client) = init_etcd_server().await;
    let (_anvil_server, anvil_provider) = init_anvil().await;

    let file_cache = init_file_cache().await;

    let block_ingestion = TestBlockIngestion {
        provider: anvil_provider.clone(),
    };

    let options = IngestionServiceOptions {
        chain_segment_size: 10,
        chain_segment_upload_offset_size: 1,
        max_concurrent_tasks: 5,
        ..Default::default()
    };

    let mut service = IngestionService::new(
        block_ingestion,
        etcd_client,
        object_store,
        file_cache,
        options,
    );

    anvil_provider.anvil_mine(100, 3).await;
    let header = anvil_provider.get_header(BlockNumberOrTag::Latest).await;
    assert_eq!(header.number, 100);

    let starting_state = service.initialize().await.unwrap();
    assert_eq!(service.task_queue_len(), 0);

    let state = starting_state.take_ingest().unwrap();
    let state = service.tick_refresh_head(state).await.unwrap();
    let state = state.take_ingest().unwrap();
    let state = service.tick_refresh_finalized(state).await.unwrap();
    let state = state.take_ingest().unwrap();

    assert_eq!(service.task_queue_len(), 0);
    assert_eq!(state.head.number, 100);

    let mut state = Some(state);
    for _ in 0..=100 {
        let join_result = service.task_queue_next().await;
        let next_state = service
            .tick_with_task_result(state.take().unwrap(), join_result)
            .await
            .unwrap();
        let next_state = next_state.take_ingest().unwrap();
        state = Some(next_state);
    }

    let state = state.unwrap();
    assert_eq!(state.last_ingested.number, 100);
    assert_eq!(service.task_queue_len(), 0);

    anvil_provider.anvil_reorg(10).await;
    anvil_provider.anvil_mine(20, 7).await;
    let header = anvil_provider.get_header(BlockNumberOrTag::Latest).await;
    assert_eq!(header.number, 120);

    let state = service.tick_refresh_head(state).await.unwrap();
    let state = state.take_ingest().unwrap();
    assert_eq!(service.task_queue_len(), 5);

    let join_result = service.task_queue_next().await;
    let state = service
        .tick_with_task_result(state, join_result)
        .await
        .unwrap();

    assert!(state.is_recover());
}

#[derive(Clone)]
struct TestBlockIngestion {
    provider: Arc<AnvilProvider>,
}

impl BlockIngestion for TestBlockIngestion {
    async fn get_head_cursor(&self) -> Result<Cursor, IngestionError> {
        let header = self.provider.get_header(BlockNumberOrTag::Latest).await;
        let hash = Hash(header.hash.to_vec());
        Ok(Cursor::new(header.number, hash))
    }

    async fn get_finalized_cursor(&self) -> Result<Cursor, IngestionError> {
        let header = self.provider.get_header(BlockNumberOrTag::Finalized).await;
        let hash = Hash(header.hash.to_vec());
        Ok(Cursor::new(header.number, hash))
    }

    async fn get_block_info_by_number(&self, number: u64) -> Result<BlockInfo, IngestionError> {
        let Some(header) = self
            .provider
            .get_maybe_header(BlockNumberOrTag::Number(number))
            .await
        else {
            return Err(IngestionError::BlockNotFound).attach_printable("missing block");
        };
        let hash = Hash(header.hash.to_vec());
        let parent_hash = Hash(header.parent_hash.to_vec());

        Ok(BlockInfo {
            number,
            hash,
            parent: parent_hash,
        })
    }

    async fn ingest_block_by_number(
        &self,
        number: u64,
    ) -> Result<(BlockInfo, fragment::Block), IngestionError> {
        let info = self.get_block_info_by_number(number).await?;

        let header = fragment::HeaderFragment {
            data: Vec::default(),
        };
        let index = fragment::IndexGroupFragment {
            indexes: Vec::default(),
        };
        let join = fragment::JoinGroupFragment {
            joins: Vec::default(),
        };

        let block = fragment::Block {
            header,
            index,
            join,
            body: Vec::default(),
        };

        Ok((info, block))
    }
}

pub mod testing {
    use std::sync::Arc;

    use alloy_provider::{network::Ethereum, Provider, ProviderBuilder, RootProvider};
    use alloy_rpc_client::ClientBuilder;
    use alloy_rpc_types::{BlockId, BlockNumberOrTag, BlockTransactionsKind, Header};
    use alloy_transport_http::Http;
    use futures::Future;
    use reqwest::Client;
    use testcontainers::{core::ContainerPort, ContainerAsync, Image};
    use url::Url;

    pub struct AnvilServer;

    pub type AnvilProvider = RootProvider<Http<Client>, Ethereum>;

    pub trait AnvilServerExt {
        fn alloy_provider(&self) -> impl Future<Output = Arc<AnvilProvider>> + Send;
    }

    pub trait AnvilProviderExt {
        fn get_maybe_header(
            &self,
            block: BlockNumberOrTag,
        ) -> impl Future<Output = Option<Header>> + Send;

        #[allow(async_fn_in_trait)]
        async fn get_header(&self, block: BlockNumberOrTag) -> Header {
            self.get_maybe_header(block)
                .await
                .expect("get_header request failed")
        }

        fn anvil_mine(
            &self,
            block_count: u64,
            interval_sec: u64,
        ) -> impl Future<Output = ()> + Send;
        fn anvil_reorg(&self, block_count: u64) -> impl Future<Output = ()> + Send;
        fn anvil_snapshot(&self) -> impl Future<Output = String> + Send;
        fn anvil_revert(&self, snapshot_id: String) -> impl Future<Output = ()> + Send;
    }

    impl Image for AnvilServer {
        fn name(&self) -> &str {
            "anvil"
        }

        fn tag(&self) -> &str {
            "latest"
        }

        fn expose_ports(&self) -> &[ContainerPort] {
            &[ContainerPort::Tcp(8545)]
        }

        fn ready_conditions(&self) -> Vec<testcontainers::core::WaitFor> {
            Vec::default()
        }
    }

    pub fn anvil_server_container() -> AnvilServer {
        AnvilServer
    }

    impl AnvilServerExt for ContainerAsync<AnvilServer> {
        async fn alloy_provider(&self) -> Arc<AnvilProvider> {
            let port = self
                .get_host_port_ipv4(8545)
                .await
                .expect("Anvil port 8545 not found");

            let url: Url = format!("http://localhost:{port}").parse().unwrap();
            let client = ClientBuilder::default().http(url);
            let provider = ProviderBuilder::default().on_client(client);

            Arc::new(provider)
        }
    }

    impl AnvilProviderExt for Arc<AnvilProvider> {
        async fn get_maybe_header(&self, block: BlockNumberOrTag) -> Option<Header> {
            self.get_block(BlockId::Number(block), BlockTransactionsKind::Hashes)
                .await
                .expect("get_header request failed")
                .map(|response| response.header)
        }

        async fn anvil_mine(&self, block_count: u64, interval_sec: u64) {
            self.raw_request::<_, serde_json::Value>(
                "anvil_mine".into(),
                &(block_count, interval_sec),
            )
            .await
            .expect("anvil_mine request failed");
        }

        async fn anvil_reorg(&self, block_count: u64) {
            self.raw_request::<_, serde_json::Value>(
                "anvil_reorg".into(),
                &(block_count, Vec::<u64>::default()),
            )
            .await
            .expect("anvil_reorg request failed");
        }

        async fn anvil_snapshot(&self) -> String {
            self.raw_request::<_, String>("anvil_snapshot".into(), ())
                .await
                .expect("anvil_snapshot request failed")
        }

        async fn anvil_revert(&self, snapshot_id: String) {
            println!("reverting snapshot {}", snapshot_id);
            self.raw_request::<_, serde_json::Value>("anvil_revert".into(), &(snapshot_id,))
                .await
                .expect("anvil_revert request failed");
        }
    }
}
