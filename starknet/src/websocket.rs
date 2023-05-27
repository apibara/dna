use crate::db::StorageReader;
use crate::ingestion::IngestionStreamClient;
use crate::server::stream::IngestionStream;
use crate::stream::{DbBatchProducer, SequentialCursorProducer};
use apibara_core::starknet::v1alpha2::Block;
use apibara_core::{
    node::v1alpha2::{StreamDataRequest},
    starknet::v1alpha2::Filter,
};
use apibara_node::stream::{
    new_data_stream, StreamConfigurationStream, StreamError,
};
use apibara_sdk::{Configuration, DataMessage};
use futures::{SinkExt, StreamExt, TryStreamExt};
use prost::Message as ProstMessage;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::info;
use warp::ws::{Message, WebSocket};
use warp::Filter as WarpFilter;

static NEXT_USERID: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(1);

#[derive(Clone)]
pub struct WebsocketStreamServer<R: StorageReader + Send + Sync + 'static> {
    ingestion: Arc<IngestionStreamClient>,
    storage: Arc<R>,
}

impl<R: StorageReader + Send + Sync + 'static> WebsocketStreamServer<R> {
    pub fn new(db: Arc<R>, ingestion: IngestionStreamClient) -> WebsocketStreamServer<R> {
        let ingestion = Arc::new(ingestion);
        WebsocketStreamServer {
            ingestion,
            storage: db,
        }
    }

    pub async fn start(self: Arc<Self>) {
        // TODO: use --websocket-address param here
        let addr = "127.0.0.1:8080";
        let socket_address: SocketAddr = addr.parse().expect("valid socket Address");

        let ws = warp::path("ws")
            .and(warp::ws())
            .map(move |ws: warp::ws::Ws| {
                let self_ = self.clone();
                ws.on_upgrade(move |websocket| self_.connect(websocket))
            });

        let server = warp::serve(ws).try_bind(socket_address);

        info!("Running websocket server at {}!", addr);

        server.await
    }

    async fn connect(self: Arc<Self>, ws: WebSocket) {
        let id = NEXT_USERID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        info!("Welcome User {}", id);

        // Establishing a connection
        let (user_tx, mut user_rx) = ws.split();

        // TODO: use channel instead of unbounded_channel
        let (configuration_tx, configuration_rx) = mpsc::unbounded_channel();
        let configuration_rx =
            StreamConfigurationStream::new(UnboundedReceiverStream::new(configuration_rx));

        let meter = apibara_node::server::SimpleMeter::default();
        // let stream_span = self.request_observer.stream_data_span(&metadata);
        // let stream_meter = self.request_observer.stream_data_meter(&metadata);

        let ingestion_stream = self.ingestion.subscribe().await;
        let ingestion_stream = IngestionStream::new(ingestion_stream);
        let batch_producer = DbBatchProducer::new(self.storage.clone());
        let cursor_producer = SequentialCursorProducer::new(self.storage.clone());

        let data_stream = new_data_stream(
            configuration_rx,
            ingestion_stream,
            cursor_producer,
            batch_producer,
            meter,
        );

        // TODO: don't use unwrap
        tokio::spawn(
            data_stream
                .map_ok(|m| {
                    let message = DataMessage::<Block>::from_stream_data_response(m).unwrap();
                    serde_json::to_string(&message).map(Message::text).unwrap()
                })
                .forward(user_tx.sink_map_err(StreamError::internal)),
        );

        while let Some(result) = user_rx.next().await {
            match result {
                Ok(message) => {
                    // TODO: handle result=Ok(Close(None,),)
                    let message =
                        serde_json::from_slice::<Configuration<Filter>>(message.as_bytes());

                    let message = message.map(Configuration::<Filter>::to_stream_data_request);

                    match configuration_tx.send(message) {
                        Ok(()) => {}
                        Err(err) => {
                            eprintln!("{}", err)
                        }
                    }
                }
                Err(err) => {
                    eprintln!("{}", err);
                }
            }
        }

        // Disconnect
        disconnect(id).await;
    }
}

async fn disconnect(id: usize) {
    info!("Good bye user {}", id);
}
