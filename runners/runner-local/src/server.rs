use tracing::warn;

use crate::error::LocalRunnerError;
use crate::manager::IndexerManager;

use apibara_runner_common::runner::v1::{
    indexer_runner_server, CreateIndexerRequest, DeleteIndexerRequest, GetIndexerRequest, Indexer,
    ListIndexersRequest, ListIndexersResponse, UpdateIndexerRequest,
};

use tokio::process::Child;
use tonic::{Request, Response};

// TODO: Ctrl-C doesn't work if the server has a lock on the indexers mutex

pub struct IndexerInfo {
    pub indexer_id: String,
    pub child: Child,
    pub indexer: Indexer,
    pub status_server_address: String,
}
pub struct RunnerService {
    pub indexer_manager: IndexerManager,
}

impl RunnerService {
    pub fn new(indexer_manager: IndexerManager) -> Self {
        Self { indexer_manager }
    }
    pub fn into_service(self) -> indexer_runner_server::IndexerRunnerServer<Self> {
        indexer_runner_server::IndexerRunnerServer::new(self)
    }
}

#[tonic::async_trait]
impl indexer_runner_server::IndexerRunner for RunnerService {
    async fn create_indexer(
        &self,
        request: Request<CreateIndexerRequest>,
    ) -> Result<Response<Indexer>, tonic::Status> {
        let request = request.into_inner();
        let indexer = request
            .indexer
            .ok_or(LocalRunnerError::missing_argument("indexer"))
            .map_err(|err| {
                warn!(err = ?err, "failed to create indexer");
                err.current_context().to_tonic_status()
            })?;

        if self.indexer_manager.has_indexer(&indexer.name).await {
            let err = LocalRunnerError::already_exists(&indexer.name);
            warn!(err = ?err, "failed to create indexer");
            return Err(err.current_context().to_tonic_status());
        }

        self.indexer_manager
            .create_indexer(request.indexer_id, indexer.clone())
            .await
            .map_err(|err| {
                warn!(err = ?err, "failed to create indexer");
                err.current_context().to_tonic_status()
            })?;

        // TODO: indexer could be created by the request handler fail because
        // get_indexer fail, example: failed to connect to status server
        // Although fixed for refresh_status, we have to make sure it's fixed everywhere
        let result = self.indexer_manager.refresh_status(&indexer.name).await;

        if let Err(err) = result {
            warn!(err = ?err, "failed to refresh status")
        }

        let indexer = self
            .indexer_manager
            .get_indexer(&indexer.name)
            .await
            .map_err(|err| {
                warn!(err = ?err, "failed to get indexer");
                err.current_context().to_tonic_status()
            })?;

        Ok(Response::new(indexer))
    }

    async fn get_indexer(
        &self,
        request: Request<GetIndexerRequest>,
    ) -> Result<Response<Indexer>, tonic::Status> {
        let request = request.into_inner();

        self.indexer_manager
            .refresh_status(&request.name)
            .await
            .map_err(|err| {
                warn!(err = ?err, "failed to refresh status");
                err.current_context().to_tonic_status()
            })?;

        let indexer = self
            .indexer_manager
            .get_indexer(&request.name)
            .await
            .map_err(|err| {
                warn!(err = ?err, "failed to get indexer");
                err.current_context().to_tonic_status()
            })?;

        Ok(Response::new(indexer))
    }

    async fn list_indexers(
        &self,
        _request: Request<ListIndexersRequest>,
    ) -> Result<Response<ListIndexersResponse>, tonic::Status> {
        let result = self.indexer_manager.refresh_status_all().await;

        if let Err(err) = result {
            warn!(err = ?err, "failed to refresh status")
        }

        let indexers = self.indexer_manager.list_indexers().await.map_err(|err| {
            warn!(err = ?err, "failed to list indexers");
            err.current_context().to_tonic_status()
        })?;

        Ok(Response::new(ListIndexersResponse {
            indexers,
            next_page_token: "".to_string(),
        }))
    }

    async fn delete_indexer(
        &self,
        request: Request<DeleteIndexerRequest>,
    ) -> Result<Response<()>, tonic::Status> {
        let request = request.into_inner();

        self.indexer_manager
            .delete_indexer(&request.name)
            .await
            .map_err(|err| {
                warn!(err = ?err, "failed to delete indexer");
                err.current_context().to_tonic_status()
            })?;

        Ok(Response::new(()))
    }

    async fn update_indexer(
        &self,
        _request: Request<UpdateIndexerRequest>,
    ) -> Result<Response<Indexer>, tonic::Status> {
        todo!()
    }
}
