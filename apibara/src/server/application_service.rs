mod pb {
    tonic::include_proto!("apibara.application.v1alpha1");
}

use std::{pin::Pin, sync::Arc};

use futures::Stream;
use tonic::{Code, Request, Response, Status, Streaming};
use tracing::debug;

use crate::application::{ApplicationId, ApplicationPersistence, State};

use self::pb::{
    application_manager_server::{ApplicationManager, ApplicationManagerServer},
    CreateApplicationRequest, CreateApplicationResponse, DeleteApplicationRequest,
    DeleteApplicationResponse, GetApplicationRequest, GetApplicationResponse, IndexerRequest,
    IndexerResponse, ListApplicationRequest, ListApplicationResponse,
};

type TonicResult<T> = Result<Response<T>, Status>;

pub struct ApplicationManagerService {
    application_persistence: Arc<dyn ApplicationPersistence>,
}

impl ApplicationManagerService {
    pub fn new(application_persistence: Arc<dyn ApplicationPersistence>) -> Self {
        ApplicationManagerService {
            application_persistence,
        }
    }

    pub fn into_service(self) -> ApplicationManagerServer<ApplicationManagerService> {
        ApplicationManagerServer::new(self)
    }
}

#[tonic::async_trait]
impl ApplicationManager for ApplicationManagerService {
    async fn create_application(
        &self,
        request: Request<CreateApplicationRequest>,
    ) -> TonicResult<CreateApplicationResponse> {
        let message: CreateApplicationRequest = request.into_inner();
        debug!("create application: {:?}", message);
        let id: ApplicationId = message
            .id
            .parse()
            .map_err(|_| Status::new(Code::InvalidArgument, "invalid application id"))?;

        let existing = self
            .application_persistence
            .get_state(&id)
            .await
            .map_err(|err| Status::new(Code::Internal, err.to_string()))?;

        if existing.is_some() {
            return Err(Status::new(
                Code::AlreadyExists,
                format!("application {} already exists", id),
            ));
        }

        let state = State {
            id,
            index_from_block: message.index_from_block,
            indexed_to_block: None,
        };

        self.application_persistence
            .write_state(&state)
            .await
            .map_err(|err| Status::new(Code::Internal, err.to_string()))?;

        let response = CreateApplicationResponse {
            application: Some(state.into()),
        };

        Ok(Response::new(response))
    }

    async fn get_application(
        &self,
        request: Request<GetApplicationRequest>,
    ) -> TonicResult<GetApplicationResponse> {
        let message: GetApplicationRequest = request.into_inner();
        debug!("get application: {:?}", message);
        let id: ApplicationId = message
            .id
            .parse()
            .map_err(|_| Status::new(Code::InvalidArgument, "invalid application id"))?;

        let application = self
            .application_persistence
            .get_state(&id)
            .await
            .map_err(|err| Status::new(Code::Internal, err.to_string()))?;

        match application {
            None => Err(Status::new(
                Code::NotFound,
                format!("application {} not found", id),
            )),
            Some(application) => {
                let response = GetApplicationResponse {
                    application: Some(application.into()),
                };
                Ok(Response::new(response))
            }
        }
    }

    async fn list_application(
        &self,
        _request: Request<ListApplicationRequest>,
    ) -> TonicResult<ListApplicationResponse> {
        let states = self
            .application_persistence
            .list_states()
            .await
            .map_err(|err| Status::new(Code::Internal, err.to_string()))?;
        let response = ListApplicationResponse {
            applications: states.into_iter().map(Into::into).collect(),
        };
        Ok(Response::new(response))
    }

    async fn delete_application(
        &self,
        request: Request<DeleteApplicationRequest>,
    ) -> TonicResult<DeleteApplicationResponse> {
        let message: DeleteApplicationRequest = request.into_inner();
        debug!("delete application: {:?}", message);
        let id: ApplicationId = message
            .id
            .parse()
            .map_err(|_| Status::new(Code::InvalidArgument, "invalid application id"))?;

        let application = self
            .application_persistence
            .get_state(&id)
            .await
            .map_err(|err| Status::new(Code::Internal, err.to_string()))?;

        match application {
            None => Err(Status::new(
                Code::NotFound,
                format!("application {} not found", id),
            )),
            Some(application) => {
                self.application_persistence
                    .delete_state(&id)
                    .await
                    .map_err(|err| Status::new(Code::Internal, err.to_string()))?;

                let response = DeleteApplicationResponse {
                    application: Some(application.into()),
                };
                Ok(Response::new(response))
            }
        }
    }

    type ConnectIndexerStream =
        Pin<Box<dyn Stream<Item = Result<IndexerResponse, Status>> + Send + 'static>>;

    async fn connect_indexer(
        &self,
        _request: Request<Streaming<IndexerRequest>>,
    ) -> TonicResult<Self::ConnectIndexerStream> {
        todo!()
    }
}

#[allow(clippy::from_over_into)] // id initialization can fail
impl Into<pb::Application> for State {
    fn into(self) -> pb::Application {
        pb::Application {
            id: self.id.into_string(),
            indexed_to_block: self.indexed_to_block,
            index_from_block: self.index_from_block,
        }
    }
}
