use crate::db::StorageReader;
use crate::ingestion::IngestionStreamClient;
use crate::server::stream::IngestionStream;
use crate::stream::{DbBatchProducer, SequentialCursorProducer};
use apibara_core::{
    node::v1alpha2::{Cursor, DataFinality, StreamDataRequest},
    starknet::v1alpha2::Filter,
};
use apibara_node::stream::{new_data_stream, StreamConfigurationStream, StreamError};
use futures::{SinkExt, StreamExt, TryStreamExt};
use tracing::info;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use warp::ws::{Message, WebSocket};
use warp::Filter as WarpFilter;

static NEXT_USERID: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(1);

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ConfigurationRequest {
    pub batch_size: Option<u64>,
    pub stream_id: u64,
    pub finality: Option<DataFinality>,
    pub starting_cursor: Option<Cursor>,
    pub filter: Filter,
}

impl ConfigurationRequest {
    pub fn to_stream_data_request(self) -> StreamDataRequest {
        StreamDataRequest {
            stream_id: Some(self.stream_id),
            batch_size: self.batch_size,
            starting_cursor: self.starting_cursor,
            finality: self.finality.map(Into::into),
            filter: serde_json::to_vec(&self.filter).unwrap(),
        }
    }
}

#[derive(Clone)]
pub struct WebsocketStreamServer<R: StorageReader + Send + Sync + 'static> {
    ingestion: Arc<IngestionStreamClient>,
    storage: Arc<R>,
}

impl<R: StorageReader + Send + Sync + 'static> WebsocketStreamServer<R> {
    pub fn new(db: Arc<R>, ingestion: IngestionStreamClient) -> WebsocketStreamServer<R> {
        let ingestion = Arc::new(ingestion);
        // let storage = DatabaseStorage::new(db);

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

        println!("Running websocket server at {}!", addr);

        server.await
    }

    async fn connect(self: Arc<Self>, ws: WebSocket) {
        let id = NEXT_USERID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        println!("Welcome User {}", id);

        // Establishing a connection
        let (user_tx, mut user_rx) = ws.split();

        // TODO: use channel instead of unbounded_channel
        let (configuration_tx, configuration_rx) = mpsc::unbounded_channel();
        let configuration_rx =
            StreamConfigurationStream::new(UnboundedReceiverStream::new(configuration_rx)).map(|m| {
                info!("ws: received in configuration stream: {:#?}", m);
                m
            });

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
                    let m = serde_json::to_string(&m.stream_id)
                        .map(Message::text)
                        .unwrap();
                    info!("ws: sending m={:#?}", m);
                    m
                })
                .forward(user_tx.sink_map_err(StreamError::internal)),
        );

        while let Some(result) = user_rx.next().await {
            info!("ws: received result={:#?}", result);
            match result {
                Ok(message) => {
                    let message =
                        serde_json::from_slice::<ConfigurationRequest>(message.as_bytes());
                    let message = message.map(ConfigurationRequest::to_stream_data_request);

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
    println!("Good bye user {}", id);
}
