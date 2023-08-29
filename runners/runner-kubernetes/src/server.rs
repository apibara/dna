use apibara_operator::crd::Indexer as KubeIndexer;
use apibara_runner_common::runner::v1::{
    indexer_runner_server, CreateIndexerRequest, DeleteIndexerRequest, GetIndexerRequest, Indexer,
    ListIndexersRequest, ListIndexersResponse, UpdateIndexerRequest,
};
use k8s_openapi::api::core::v1::{ConfigMap, Secret};
use kube::{
    api::{Patch, PatchParams},
    Api,
};
use tonic::{Request, Response};
use tracing::warn;

use crate::kube_resources::KubeResources;

#[derive(Debug, Clone)]
pub struct RunnerServiceOptions {
    /// Namespace where to create resources.
    pub target_namespace: String,
}

pub struct RunnnerService {
    client: kube::Client,
    options: RunnerServiceOptions,
}

impl RunnnerService {
    pub fn new(client: kube::Client, options: RunnerServiceOptions) -> Self {
        Self { client, options }
    }

    pub fn into_service(self) -> indexer_runner_server::IndexerRunnerServer<Self> {
        indexer_runner_server::IndexerRunnerServer::new(self)
    }
}

#[tonic::async_trait]
impl indexer_runner_server::IndexerRunner for RunnnerService {
    async fn create_indexer(
        &self,
        request: Request<CreateIndexerRequest>,
    ) -> Result<Response<Indexer>, tonic::Status> {
        let request = request.into_inner();

        let resources = KubeResources::from_request(request)?;

        let indexer_api: Api<KubeIndexer> =
            Api::namespaced(self.client.clone(), &self.options.target_namespace);
        let secret_api: Api<Secret> =
            Api::namespaced(self.client.clone(), &self.options.target_namespace);
        let cm_api: Api<ConfigMap> =
            Api::namespaced(self.client.clone(), &self.options.target_namespace);

        if let Some(config_map) = &resources.script_config_map {
            cm_api
                .patch(
                    &resources.indexer_id,
                    &PatchParams::apply("runner"),
                    &Patch::Apply(config_map),
                )
                .await
                .map_err(|err| {
                    warn!(
                        "failed to create ConfigMap for indexer {}: {}",
                        resources.indexer_id, err
                    );
                    tonic::Status::internal("failed to create indexer.")
                })?;
        }

        secret_api
            .patch(
                &resources.indexer_id,
                &PatchParams::apply("runner"),
                &Patch::Apply(&resources.env_secret),
            )
            .await
            .map_err(|err| {
                warn!(
                    "failed to create env Secret for indexer {}: {}",
                    resources.indexer_id, err
                );
                tonic::Status::internal("failed to create indexer.")
            })?;

        indexer_api
            .patch(
                &resources.indexer_id,
                &PatchParams::apply("runner"),
                &Patch::Apply(&resources.indexer),
            )
            .await
            .map_err(|err| {
                warn!("failed to create indexer {}: {}", resources.indexer_id, err);
                tonic::Status::internal("failed to create indexer.")
            })?;

        let proto_indexer = resources.into_indexer()?;

        Ok(Response::new(proto_indexer))
    }

    async fn get_indexer(
        &self,
        _request: Request<GetIndexerRequest>,
    ) -> Result<Response<Indexer>, tonic::Status> {
        todo!()
    }

    async fn list_indexers(
        &self,
        _request: Request<ListIndexersRequest>,
    ) -> Result<Response<ListIndexersResponse>, tonic::Status> {
        todo!()
    }

    async fn update_indexer(
        &self,
        _request: Request<UpdateIndexerRequest>,
    ) -> Result<Response<Indexer>, tonic::Status> {
        todo!()
    }

    async fn delete_indexer(
        &self,
        _request: Request<DeleteIndexerRequest>,
    ) -> Result<Response<()>, tonic::Status> {
        todo!()
    }
}
