use apibara_etcd::{EtcdClient, KvClient, WatchClient};
use error_stack::{Result, ResultExt};
use futures::{Stream, StreamExt};
use tokio_util::sync::CancellationToken;

use crate::object_store::ObjectETag;

pub static INGESTION_PREFIX_KEY: &str = "ingestion/";
pub static INGESTED_KEY: &str = "ingestion/ingested";
pub static PENDING_KEY: &str = "ingestion/pending";
pub static STARTING_BLOCK_KEY: &str = "ingestion/starting_block";
pub static FINALIZED_KEY: &str = "ingestion/finalized";
pub static SEGMENTED_KEY: &str = "ingestion/segmented";
pub static GROUPED_KEY: &str = "ingestion/grouped";

#[derive(Debug)]
pub struct IngestionStateClientError;

#[derive(Clone)]
pub struct IngestionStateClient {
    kv_client: KvClient,
    watch_client: WatchClient,
}

#[derive(Clone, Debug)]
pub enum IngestionStateUpdate {
    StartingBlock(u64),
    Finalized(u64),
    Segmented(u64),
    Grouped(u64),
    Ingested(String),
}

impl IngestionStateClient {
    pub fn new(client: &EtcdClient) -> Self {
        let kv_client = client.kv_client();
        let watch_client = client.watch_client();

        Self {
            kv_client,
            watch_client,
        }
    }

    pub async fn watch_changes(
        &mut self,
        ct: CancellationToken,
    ) -> Result<
        impl Stream<Item = Result<IngestionStateUpdate, IngestionStateClientError>>,
        IngestionStateClientError,
    > {
        let (_watcher, stream) = self
            .watch_client
            .watch_prefix(INGESTION_PREFIX_KEY, ct)
            .await
            .change_context(IngestionStateClientError)
            .attach_printable("failed to watch ingestion state")?;

        let changes = stream.flat_map(|response| {
            let response = match response {
                Err(err) => {
                    return futures::stream::iter(vec![
                        Err(err).change_context(IngestionStateClientError)
                    ]);
                }
                Ok(response) => response,
            };

            let changes = response
                .events()
                .iter()
                .filter_map(|event| {
                    let kv = event.kv()?;

                    match IngestionStateUpdate::from_kv(kv) {
                        Ok(Some(update)) => Some(Ok(update)),
                        Ok(None) => None,
                        Err(err) => Some(Err(err)),
                    }
                })
                .collect::<Vec<Result<IngestionStateUpdate, _>>>();
            futures::stream::iter(changes)
        });

        Ok(changes)
    }

    pub async fn get_starting_block(&mut self) -> Result<Option<u64>, IngestionStateClientError> {
        let response = self
            .kv_client
            .get(STARTING_BLOCK_KEY)
            .await
            .change_context(IngestionStateClientError)
            .attach_printable("failed to get starting block")?;

        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };

        let value = String::from_utf8(kv.value().to_vec())
            .change_context(IngestionStateClientError)
            .attach_printable("failed to decode starting block")?;

        let block = value
            .parse::<u64>()
            .change_context(IngestionStateClientError)
            .attach_printable("failed to parse starting block")?;

        Ok(Some(block))
    }

    pub async fn put_starting_block(
        &mut self,
        block: u64,
    ) -> Result<(), IngestionStateClientError> {
        let value = block.to_string();
        self.kv_client
            .put(STARTING_BLOCK_KEY, value.as_bytes())
            .await
            .change_context(IngestionStateClientError)
            .attach_printable("failed to put starting block")?;

        Ok(())
    }

    pub async fn get_finalized(&mut self) -> Result<Option<u64>, IngestionStateClientError> {
        let response = self
            .kv_client
            .get(FINALIZED_KEY)
            .await
            .change_context(IngestionStateClientError)
            .attach_printable("failed to get finalized block")?;

        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };

        let value = String::from_utf8(kv.value().to_vec())
            .change_context(IngestionStateClientError)
            .attach_printable("failed to decode finalized block")?;

        let block = value
            .parse::<u64>()
            .change_context(IngestionStateClientError)
            .attach_printable("failed to parse finalized block")?;

        Ok(Some(block))
    }

    pub async fn put_finalized(&mut self, block: u64) -> Result<(), IngestionStateClientError> {
        let value = block.to_string();
        self.kv_client
            .put(FINALIZED_KEY, value.as_bytes())
            .await
            .change_context(IngestionStateClientError)
            .attach_printable("failed to put finalized block")?;

        Ok(())
    }

    pub async fn get_ingested(&mut self) -> Result<Option<ObjectETag>, IngestionStateClientError> {
        let response = self
            .kv_client
            .get(INGESTED_KEY)
            .await
            .change_context(IngestionStateClientError)
            .attach_printable("failed to get latest ingested block")?;

        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };

        let etag = String::from_utf8(kv.value().to_vec())
            .change_context(IngestionStateClientError)
            .attach_printable("failed to decode etag")?;

        Ok(Some(ObjectETag(etag)))
    }

    pub async fn put_ingested(
        &mut self,
        etag: ObjectETag,
    ) -> Result<(), IngestionStateClientError> {
        let value = etag.0;
        self.kv_client
            .put_and_delete(INGESTED_KEY, value.as_bytes(), PENDING_KEY)
            .await
            .change_context(IngestionStateClientError)
            .attach_printable("failed to put latest ingested block")?;

        Ok(())
    }

    pub async fn put_pending(&mut self, generation: u64) -> Result<(), IngestionStateClientError> {
        let value = generation.to_string();
        self.kv_client
            .put(PENDING_KEY, value.as_bytes())
            .await
            .change_context(IngestionStateClientError)
            .attach_printable("failed to put pending block generation")?;

        Ok(())
    }

    pub async fn get_segmented(&mut self) -> Result<Option<u64>, IngestionStateClientError> {
        let response = self
            .kv_client
            .get(SEGMENTED_KEY)
            .await
            .change_context(IngestionStateClientError)
            .attach_printable("failed to get segmented block")?;

        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };

        let value = String::from_utf8(kv.value().to_vec())
            .change_context(IngestionStateClientError)
            .attach_printable("failed to decode segmented block")?;

        let block = value
            .parse::<u64>()
            .change_context(IngestionStateClientError)
            .attach_printable("failed to parse segmented block")?;

        Ok(Some(block))
    }

    pub async fn put_segmented(&mut self, block: u64) -> Result<(), IngestionStateClientError> {
        let value = block.to_string();
        self.kv_client
            .put(SEGMENTED_KEY, value.as_bytes())
            .await
            .change_context(IngestionStateClientError)
            .attach_printable("failed to put segmented block")?;

        Ok(())
    }

    pub async fn get_grouped(&mut self) -> Result<Option<u64>, IngestionStateClientError> {
        let response = self
            .kv_client
            .get(GROUPED_KEY)
            .await
            .change_context(IngestionStateClientError)
            .attach_printable("failed to get grouped block")?;

        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };

        let value = String::from_utf8(kv.value().to_vec())
            .change_context(IngestionStateClientError)
            .attach_printable("failed to decode grouped block")?;

        let block = value
            .parse::<u64>()
            .change_context(IngestionStateClientError)
            .attach_printable("failed to parse grouped block")?;

        Ok(Some(block))
    }

    pub async fn put_grouped(&mut self, block: u64) -> Result<(), IngestionStateClientError> {
        let value = block.to_string();
        self.kv_client
            .put(GROUPED_KEY, value.as_bytes())
            .await
            .change_context(IngestionStateClientError)
            .attach_printable("failed to put grouped block")?;

        Ok(())
    }
}

impl error_stack::Context for IngestionStateClientError {}

impl std::fmt::Display for IngestionStateClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ingestion state client error")
    }
}

impl IngestionStateUpdate {
    pub fn from_kv(kv: &etcd_client::KeyValue) -> Result<Option<Self>, IngestionStateClientError> {
        let key = String::from_utf8(kv.key().to_vec())
            .change_context(IngestionStateClientError)
            .attach_printable("failed to decode key")?;

        let value = String::from_utf8(kv.value().to_vec())
            .change_context(IngestionStateClientError)
            .attach_printable("failed to decode value")?;

        if key.ends_with(STARTING_BLOCK_KEY) {
            let block = value
                .parse::<u64>()
                .change_context(IngestionStateClientError)
                .attach_printable("failed to parse starting block")?;
            Ok(Some(IngestionStateUpdate::StartingBlock(block)))
        } else if key.ends_with(FINALIZED_KEY) {
            let block = value
                .parse::<u64>()
                .change_context(IngestionStateClientError)
                .attach_printable("failed to parse finalized block")?;
            Ok(Some(IngestionStateUpdate::Finalized(block)))
        } else if key.ends_with(INGESTED_KEY) {
            Ok(Some(IngestionStateUpdate::Ingested(value)))
        } else if key.ends_with(SEGMENTED_KEY) {
            let block = value
                .parse::<u64>()
                .change_context(IngestionStateClientError)
                .attach_printable("failed to parse segmented block")?;
            Ok(Some(IngestionStateUpdate::Segmented(block)))
        } else if key.ends_with(GROUPED_KEY) {
            let block = value
                .parse::<u64>()
                .change_context(IngestionStateClientError)
                .attach_printable("failed to parse grouped block")?;
            Ok(Some(IngestionStateUpdate::Grouped(block)))
        } else {
            Ok(None)
        }
    }
}

pub mod testing {
    use std::borrow::Cow;

    use apibara_etcd::EtcdClient;
    use futures::Future;
    use testcontainers::{
        core::{ContainerPort, WaitFor},
        ContainerAsync, Image,
    };

    pub struct EtcdServer;

    pub trait EtcdServerExt {
        fn etcd_client(&self) -> impl Future<Output = EtcdClient> + Send;
    }

    impl Image for EtcdServer {
        fn name(&self) -> &str {
            "bitnami/etcd"
        }

        fn tag(&self) -> &str {
            "latest"
        }

        fn ready_conditions(&self) -> Vec<WaitFor> {
            Vec::default()
        }

        fn env_vars(
            &self,
        ) -> impl IntoIterator<Item = (impl Into<Cow<'_, str>>, impl Into<Cow<'_, str>>)> {
            vec![("ALLOW_NONE_AUTHENTICATION".to_string(), "yes".to_string())]
        }

        fn expose_ports(&self) -> &[ContainerPort] {
            &[ContainerPort::Tcp(2379)]
        }
    }

    pub fn etcd_server_container() -> EtcdServer {
        EtcdServer
    }

    impl EtcdServerExt for ContainerAsync<EtcdServer> {
        async fn etcd_client(&self) -> EtcdClient {
            let port = self
                .get_host_port_ipv4(2379)
                .await
                .expect("Etcd port 2379 not found");

            let endpoint = format!("http://localhost:{port}");
            EtcdClient::connect(&[endpoint], Default::default())
                .await
                .expect("Etcd connection error")
        }
    }
}
