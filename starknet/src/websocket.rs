use crate::db::StorageReader;
use crate::ingestion::IngestionStreamClient;
use crate::server::stream::IngestionStream;
use crate::stream::{DbBatchProducer, SequentialCursorProducer};
use apibara_core::starknet::v1alpha2::Block;
use apibara_core::starknet::v1alpha2::Filter;
use apibara_node::server::QuotaClient;
use apibara_node::stream::{new_data_stream, StreamConfigurationStream, StreamError};
use apibara_sdk::{Configuration, DataMessage};
use futures::future;
use futures::{SinkExt, StreamExt, TryStreamExt};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;
use warp::ws::{Message, WebSocket};
use warp::Filter as WarpFilter;

#[derive(Clone)]
pub struct WebsocketStreamServer<R: StorageReader + Send + Sync + 'static> {
    address: String,
    blocks_per_second_quota: u32,
    ingestion: Arc<IngestionStreamClient>,
    storage: Arc<R>,
}

impl<R: StorageReader + Send + Sync + 'static> WebsocketStreamServer<R> {
    pub fn new(
        address: String,
        db: Arc<R>,
        ingestion: IngestionStreamClient,
        blocks_per_second_quota: u32,
    ) -> WebsocketStreamServer<R> {
        let ingestion = Arc::new(ingestion);
        WebsocketStreamServer {
            address,
            ingestion,
            storage: db,
            blocks_per_second_quota,
        }
    }

    pub async fn start(self: Arc<Self>) {
        let socket_address: SocketAddr = self.address.parse().expect("valid socket Address");

        let ws = warp::path("ws")
            .and(warp::ws())
            .map(move |ws: warp::ws::Ws| {
                let self_ = self.clone();
                ws.on_upgrade(move |websocket| self_.connect(websocket))
            });

        let server = warp::serve(ws).try_bind(socket_address);

        info!("Running websocket server at {}!", socket_address);

        server.await
    }

    async fn connect(self: Arc<Self>, ws: WebSocket) {
        // Establishing a connection
        let (user_tx, user_rx) = ws.split();

        let configuration_stream = Box::pin(
            user_rx
                .map_err(Into::into)
                .map_err(StreamError::Internal)
                .and_then(|message| async move {
                    serde_json::from_slice::<Configuration<Filter>>(message.as_bytes())
                        .map_err(Into::into)
                        .map_err(StreamError::Internal)
                        .and_then(|message| {
                            message
                                .to_stream_data_request()
                                .map_err(Into::into)
                                .map_err(StreamError::Internal)
                        })
                }),
        );

        let configuration_stream = StreamConfigurationStream::new(configuration_stream);

        let meter = apibara_node::server::SimpleMeter::default();
        let quota_client = QuotaClient::no_quota();
        // let stream_span = self.request_observer.stream_data_span(&metadata);
        // let stream_meter = self.request_observer.stream_data_meter(&metadata);

        let ingestion_stream = self.ingestion.subscribe().await;
        let ingestion_stream = IngestionStream::new(ingestion_stream);
        let batch_producer = DbBatchProducer::new(self.storage.clone());
        let cursor_producer = SequentialCursorProducer::new(self.storage.clone());

        let data_stream = new_data_stream(
            configuration_stream,
            ingestion_stream,
            cursor_producer,
            batch_producer,
            self.blocks_per_second_quota,
            meter,
            quota_client,
        );

        // TODO: send the first decoding error downstream
        data_stream
            .and_then(|message| async {
                let message = DataMessage::<Block>::from_stream_data_response(message).ok_or(
                    StreamError::internal("Cannot convert StreamDataResponse to DataMessage"),
                )?;
                serde_json::to_string(&message)
                    .map(Message::text)
                    .map_err(Into::into)
                    .map_err(StreamError::Internal)
            })
            .take_while(|result| future::ready(result.is_ok()))
            .forward(user_tx.sink_map_err(StreamError::internal))
            .await
            .unwrap(); // we have to unwrap here since ws.on_upgrade expects ()
    }
}
