mod pb {
    tonic::include_proto!("apibara.application.v1alpha1");
}

use std::{pin::Pin, sync::Arc};

use anyhow::Error;
use futures::Stream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info};

use crate::{
    chain::{BlockHash, BlockHeader, ChainProvider, Event, TopicValue},
    indexer::{
        ClientToIndexerMessage as IndexerClientMessage, IndexerManager, IndexerPersistence,
        Message as IndexerMessage, State as IndexerState,
    },
    persistence::Id,
};

use self::pb::{
    connect_indexer_request::Message as ConnectIndexerRequestMessage,
    connect_indexer_response::Message as ConnectIndexerResponseMessage,
    indexer_manager_server::{IndexerManager as IndexerManagerTrait, IndexerManagerServer},
    ConnectIndexerRequest, ConnectIndexerResponse, CreateIndexerRequest, CreateIndexerResponse,
    DeleteIndexerRequest, DeleteIndexerResponse, GetIndexerRequest, GetIndexerResponse,
    IndexerConnected, ListIndexerRequest, ListIndexerResponse, NewBlock, NewEvents, Reorg,
};

pub struct IndexerManagerService<P: ChainProvider, IP: IndexerPersistence> {
    provider: Arc<P>,
    indexer_manager: IndexerManager<IP>,
}

type TonicResult<T> = Result<Response<T>, Status>;

impl<P: ChainProvider, IP: IndexerPersistence> IndexerManagerService<P, IP> {
    pub fn new(provider: Arc<P>, indexer_persistence: Arc<IP>) -> Self {
        let indexer_manager = IndexerManager::new(indexer_persistence);

        IndexerManagerService {
            provider,
            indexer_manager,
        }
    }

    pub fn into_service(self) -> IndexerManagerServer<IndexerManagerService<P, IP>> {
        IndexerManagerServer::new(self)
    }
}

#[tonic::async_trait]
impl<P: ChainProvider, IP: IndexerPersistence> IndexerManagerTrait
    for IndexerManagerService<P, IP>
{
    async fn create_indexer(
        &self,
        request: Request<CreateIndexerRequest>,
    ) -> TonicResult<CreateIndexerResponse> {
        let message: CreateIndexerRequest = request.into_inner();
        debug!("create indexer: {:?}", message);
        let id: Id = message.id.parse().map_err(RequestError)?;

        let indexer = self
            .indexer_manager
            .create_indexer(&id, message.index_from_block)
            .await
            .map_err(RequestError)?;

        let response = CreateIndexerResponse {
            indexer: Some(indexer.into()),
        };

        Ok(Response::new(response))
    }

    async fn get_indexer(
        &self,
        request: Request<GetIndexerRequest>,
    ) -> TonicResult<GetIndexerResponse> {
        let message: GetIndexerRequest = request.into_inner();
        debug!("get indexer: {:?}", message);
        let id: Id = message.id.parse().map_err(RequestError)?;

        let indexer = self
            .indexer_manager
            .get_indexer(&id)
            .await
            .map_err(RequestError)?
            .map(Into::into);

        let response = GetIndexerResponse { indexer };

        Ok(Response::new(response))
    }

    async fn delete_indexer(
        &self,
        request: Request<DeleteIndexerRequest>,
    ) -> TonicResult<DeleteIndexerResponse> {
        let message: DeleteIndexerRequest = request.into_inner();
        debug!("delete indexer: {:?}", message);
        let id: Id = message.id.parse().map_err(RequestError)?;

        let deleted = self
            .indexer_manager
            .delete_indexer(&id)
            .await
            .map_err(RequestError)?
            .into();

        let response = DeleteIndexerResponse {
            indexer: Some(deleted),
        };

        Ok(Response::new(response))
    }

    async fn list_indexer(
        &self,
        _request: Request<ListIndexerRequest>,
    ) -> TonicResult<ListIndexerResponse> {
        todo!()
    }

    type ConnectIndexerStream =
        Pin<Box<dyn Stream<Item = Result<ConnectIndexerResponse, Status>> + Send + 'static>>;

    async fn connect_indexer(
        &self,
        request: Request<Streaming<ConnectIndexerRequest>>,
    ) -> TonicResult<Self::ConnectIndexerStream> {
        debug!("connect indexer");

        let request_stream = request.into_inner().map(|m| match m {
            Ok(m) => m.try_into(),
            Err(err) => Err(Error::msg(err.to_string())),
        });

        let indexer_stream = self
            .indexer_manager
            .connect_indexer(Box::pin(request_stream), self.provider.clone())
            .await
            .map_err(RequestError)?;

        let response_stream: Pin<
            Box<dyn Stream<Item = Result<ConnectIndexerResponse, Status>> + Send>,
        > = Box::pin(indexer_stream.map(|m| match m {
            Err(err) => Err(RequestError(err))?,
            Ok(message) => {
                let message = message.into();
                let response = ConnectIndexerResponse {
                    message: Some(message),
                };
                Ok(response)
            }
        }));

        let response = Response::new(response_stream);

        Ok(response)
    }
}

struct RequestError(anyhow::Error);

impl From<RequestError> for Status {
    fn from(err: RequestError) -> Self {
        Status::internal(err.0.to_string())
    }
}

#[allow(clippy::from_over_into)]
impl Into<pb::Indexer> for IndexerState {
    fn into(self) -> pb::Indexer {
        pb::Indexer {
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

impl Into<pb::Event> for Event {
    fn into(self) -> pb::Event {
        let data = self.data.into_iter().map(Into::into).collect();
        let topics = self.topics.into_iter().map(Into::into).collect();
        pb::Event {
            address: self.address.to_vec(),
            block_index: self.block_index as u64,
            data,
            topics,
        }
    }
}

impl Into<pb::TopicValue> for TopicValue {
    fn into(self) -> pb::TopicValue {
        pb::TopicValue {
            value: self.to_vec(),
        }
    }
}

impl Into<ConnectIndexerResponseMessage> for IndexerMessage {
    fn into(self) -> ConnectIndexerResponseMessage {
        match self {
            IndexerMessage::Connected(state) => {
                let connected = IndexerConnected {
                    indexer: Some(state.into()),
                };
                ConnectIndexerResponseMessage::Connected(connected)
            }
            IndexerMessage::NewBlock(new_head) => {
                let new_block = NewBlock {
                    new_head: Some(new_head.into()),
                };
                ConnectIndexerResponseMessage::NewBlock(new_block)
            }
            IndexerMessage::Reorg(new_head) => {
                let reorg = Reorg {
                    new_head: Some(new_head.into()),
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

impl TryFrom<ConnectIndexerRequest> for IndexerClientMessage {
    type Error = anyhow::Error;

    fn try_from(request: ConnectIndexerRequest) -> Result<Self, Self::Error> {
        match request.message {
            None => Err(Error::msg("missing required field message")),
            Some(ConnectIndexerRequestMessage::Connect(connect)) => {
                let id = connect.id.parse()?;
                Ok(IndexerClientMessage::Connect(id))
            }
            Some(ConnectIndexerRequestMessage::Ack(ack)) => {
                let hash = BlockHash::from_bytes(&ack.hash);
                Ok(IndexerClientMessage::AckBlock(hash))
            }
        }
    }
}
