mod pb {
    tonic::include_proto!("apibara.application.v1alpha1");
}

use anyhow::Error;
use futures::Stream;
use std::{pin::Pin, str::FromStr, sync::Arc, time::Duration};
use tokio::{
    sync::mpsc::{self, error::TrySendError},
    task::JoinHandle,
};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{Code, Request, Response, Status, Streaming};
use tracing::debug;

use crate::{
    application::{Application, ApplicationId, ApplicationPersistence, State},
    chain::{BlockHash, BlockHeader, Event, EventFilter, TopicValue},
    indexer::IndexerConfig,
    indexer::Message as IndexerMessage,
};

use self::pb::{
    application_manager_server::{ApplicationManager, ApplicationManagerServer},
    connect_indexer_request::Message as ConnectIndexerRequestMessage,
    connect_indexer_response::Message as ConnectIndexerResponseMessage,
    AckBlock, ApplicationConnected, ConnectIndexerRequest, ConnectIndexerResponse,
    CreateApplicationRequest, CreateApplicationResponse, DeleteApplicationRequest,
    DeleteApplicationResponse, GetApplicationRequest, GetApplicationResponse,
    ListApplicationRequest, ListApplicationResponse, NewBlock, NewEvents, Reorg,
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
        Pin<Box<dyn Stream<Item = Result<ConnectIndexerResponse, Status>> + Send + 'static>>;

    async fn connect_indexer(
        &self,
        request: Request<Streaming<ConnectIndexerRequest>>,
    ) -> TonicResult<Self::ConnectIndexerStream> {
        let mut request_stream = request.into_inner();

        // check application exists
        let id = if let Some(message) = request_stream.next().await {
            match message {
                Ok(ConnectIndexerRequest {
                    message: Some(message),
                }) => match message {
                    ConnectIndexerRequestMessage::Connect(connect) => connect
                        .id
                        .parse()
                        .map_err(|_| Status::new(Code::Internal, "invalid application id"))?,
                    _ => {
                        return Err(Status::new(
                            Code::InvalidArgument,
                            "first message must be ConnectApplication",
                        ))
                    }
                },
                _ => {
                    return Err(Status::new(
                        Code::Internal,
                        "error retrieving first message",
                    ))
                }
            }
        } else {
            return Err(Status::new(Code::Internal, "stream closed"));
        };

        let application = self
            .application_persistence
            .get_state(&id)
            .await
            .map_err(|err| Status::new(Code::Internal, err.to_string()))?;

        let (response_tx, response_rx) = mpsc::channel(128);

        // inform client that connection went well
        match application {
            None => {
                return Err(Status::new(
                    Code::NotFound,
                    format!("application {} not found", id),
                ))
            }
            Some(application) => {
                let connected = ApplicationConnected {
                    application: Some(application.into()),
                };
                let message = ConnectIndexerResponseMessage::Connected(connected);
                let response = ConnectIndexerResponse {
                    message: Some(message),
                };
                response_tx
                    .try_send(Ok(response))
                    .map_err(|err| Status::new(Code::Internal, err.to_string()))?;
            }
        };

        // start streaming events to client
        // config should come from application state
        let transfer_topic = TopicValue::from_str(
            "0x028ECC732E12D910338C3C78B1960BB6E6A8F6746B914696D6EBE15ED46AA241",
        )
        .map_err(|err| Status::new(Code::Internal, err.to_string()))?;
        let filter = EventFilter::empty().add_topic(transfer_topic);
        let indexer_config = IndexerConfig::new(241_000).add_filter(filter);
        let application = Application::new(id.clone(), indexer_config);
        let (mut application_handle, application_client) = application
            .start()
            .await
            .map_err(|err| Status::new(Code::Internal, err.to_string()))?;

        let _: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            debug!(application_id=?id, "starting application stream");
            let rest_time = Duration::from_secs(1);

            loop {
                // TODO: check application_handle as well
                let state = application_client.application_state().await?;
                if state.is_started() {
                    break;
                }
                tokio::time::sleep(rest_time).await;
            }

            debug!(application_id=?id, "application started");
            let mut indexer_stream = application_client.start_indexing().await?;

            loop {
                tokio::select! {
                    indexer_msg = indexer_stream.next() => {
                        debug!("indexer message {:?}", indexer_msg);
                        match indexer_msg {
                            None => {},
                            Some(indexer_msg) => {
                                let message = indexer_msg.into();
                                let response = ConnectIndexerResponse{
                                    message: Some(message)
                                };
                                response_tx.try_send(Ok(response))?;
                            }
                        }
                    }
                    /*
                    client_msg = request_stream.next() => {
                        debug!("client message {:?}", client_msg);
                        break
                    }
                    */
                    _ = &mut application_handle => {
                        return Err(Error::msg("application service stopped"))
                    }
                }
            }
            // Ok(())
        });

        let response_stream = ReceiverStream::new(response_rx);

        let response = Response::new(Box::pin(response_stream) as Self::ConnectIndexerStream);

        Ok(response)
    }
}

#[allow(clippy::from_over_into)]
impl Into<pb::Application> for State {
    fn into(self) -> pb::Application {
        pb::Application {
            id: self.id.into_string(),
            indexed_to_block: self.indexed_to_block,
            index_from_block: self.index_from_block,
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<pb::BlockHeader> for BlockHeader {
    fn into(self) -> pb::BlockHeader {
        let ts_seconds = self.timestamp.timestamp();
        let timestamp = prost_types::Timestamp {
            seconds: ts_seconds,
            nanos: 0,
        };
        pb::BlockHeader {
            number: self.number,
            hash: self.hash.to_vec(),
            parent_hash: self.parent_hash.as_ref().map(BlockHash::to_vec),
            timestamp: Some(timestamp),
        }
    }
}

impl Into<ConnectIndexerResponseMessage> for IndexerMessage {
    fn into(self) -> ConnectIndexerResponseMessage {
        match self {
            IndexerMessage::NewBlock(new_head) => {
                let new_head = new_head.into();
                let new_block = NewBlock {
                    new_head: Some(new_head),
                };
                ConnectIndexerResponseMessage::NewBlock(new_block)
            }
            IndexerMessage::Reorg(new_head) => {
                let new_head = new_head.into();
                let reorg = Reorg {
                    new_head: Some(new_head),
                };
                ConnectIndexerResponseMessage::Reorg(reorg)
            }
            IndexerMessage::NewEvents(block_events) => {
                let events = block_events.events.into_iter().map(Into::into).collect();
                let new_events = NewEvents {
                    block_hash: block_events.hash.to_vec(),
                    block_number: block_events.number,
                    events,
                };
                ConnectIndexerResponseMessage::NewEvents(new_events)
            }
        }
    }
}

impl Into<pb::Event> for Event {
    fn into(self) -> pb::Event {
        pb::Event {}
    }
}
