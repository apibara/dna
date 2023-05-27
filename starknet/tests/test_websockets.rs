mod common;

use std::time::Duration;

use apibara_core::{
    node::v1alpha2::DataFinality,
    starknet::v1alpha2::{Block, Filter, HeaderFilter},
};
use apibara_node::o11y::init_opentelemetry;
use apibara_sdk::{ClientBuilder, Configuration, DataMessage};
use apibara_starknet::{start_node, StartArgs};
use futures::FutureExt;
use futures_util::{TryStreamExt, SinkExt};
use testcontainers::clients;
// use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::info;

use common::{Devnet, DevnetClient};

use std::env;

use futures_util::{future, pin_mut, StreamExt as FutureUtilStreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};


#[tokio::test]
// #[ignore]
async fn test_reorg_from_client_pov_websockets() {
    init_opentelemetry().unwrap();

    let docker = clients::Cli::default();
    let devnet = docker.run(Devnet::default());

    let rpc_port = devnet.get_host_port_ipv4(5050);
    let cts = CancellationToken::new();

    let node_handle = tokio::spawn({
        let cts = cts.clone();
        async move {
            let args = StartArgs {
                rpc: format!("http://localhost:{}/rpc", rpc_port),
                data: None,
                name: None,
                wait_for_rpc: true,
                devnet: true,
                use_metadata: Vec::default(),
                websocket_address: None,
            };
            start_node(args, cts).await.unwrap();
        }
    });

    // give time for node to start
    tokio::time::sleep(Duration::from_secs(5)).await;

    let new_starting_cursor = {
        let configuration = Configuration::<Filter>::default()
            .with_finality(DataFinality::DataStatusAccepted)
            .with_batch_size(10)
            .with_filter(|mut filter| {
                filter.with_header(HeaderFilter::new());
                filter
            });

        let uri = "http://localhost:7171".parse().unwrap();
        let (mut data_stream, data_client) = ClientBuilder::<Filter, Block>::default()
            .connect(uri)
            .await
            .unwrap();

        let connect_addr = "ws://localhost:8080/ws";
        let url = url::Url::parse(&connect_addr).unwrap();
        let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
        println!("WebSocket handshake has been successfully completed");
        let (write, read) = ws_stream.split();
        let (mut tx, rx) = futures_channel::mpsc::unbounded();
        let rx_to_ws = tokio::spawn(rx.map(Ok).forward(write));

        let configuration_json = r#"{"stream_id": 2, "batch_size": 10,"data_finality": "finalized","filter": {"header":{}}}"#;

        tx.send(Message::text(configuration_json)).await.unwrap();

        data_client.send(configuration).await.unwrap();
        info!("connected. tests starting");
        let devnet_client = DevnetClient::new(format!("http://localhost:{}", rpc_port));

        // generate 10 new blocks
        for _ in 0..10 {
            devnet_client.mint().await.unwrap();
        }

        info!("read data messages");
        // stream data. should receive 11 blocks.
        let mut block_hash = None;
        for i in 0..11 {
            let message = data_stream.try_next().await.unwrap().unwrap();
            match message {
                DataMessage::Data {
                    cursor,
                    end_cursor,
                    finality: _finality,
                    mut batch,
                } => {
                    if let Some(cursor) = cursor {
                        assert_eq!(cursor.order_key, i - 1);
                        assert!(!cursor.unique_key.is_empty());
                    } else {
                        assert_eq!(i, 0);
                    }
                    assert_eq!(end_cursor.order_key, i);
                    assert!(!end_cursor.unique_key.is_empty());
                    assert_eq!(batch.len(), 1);
                    let block = batch.remove(0);
                    if i == 5 {
                        block_hash =
                            Some(block.header.clone().unwrap().block_hash.unwrap().to_hex());
                    }
                    assert_eq!(block.header.unwrap().block_number, i);
                }
                _ => unreachable!(),
            }
        }

        info!("check stream finished");
        // reached the top of the stream. no next block.
        let next_message = data_stream.try_next().now_or_never();
        assert!(next_message.is_none());

        // generate new block, expect new message
        info!("check new message. save cursor");
        devnet_client.mint().await.unwrap();
        let starting_cursor = match data_stream.try_next().await.unwrap().unwrap() {
            DataMessage::Data {
                cursor: _cursor,
                end_cursor,
                finality: _finality,
                batch: _batch,
            } => {
                assert_eq!(end_cursor.order_key, 11);
                end_cursor
            }
            _ => unreachable!(),
        };

        info!("check small reorg");
        devnet_client
            .abort_blocks(&block_hash.unwrap())
            .await
            .unwrap();
        devnet_client.mint().await.unwrap();
        match data_stream.try_next().await.unwrap().unwrap() {
            DataMessage::Invalidate { cursor } => {
                // block 5 is reorged too
                assert_eq!(cursor.unwrap().order_key, 4);
            }
            _ => unreachable!(),
        }

        match data_stream.try_next().await.unwrap().unwrap() {
            DataMessage::Data {
                cursor: _cursor,
                end_cursor,
                finality: _finality,
                batch: _batch,
            } => {
                assert_eq!(end_cursor.order_key, 5);
            }
            _ => unreachable!(),
        }

        starting_cursor
    };

    {
        let configuration = Configuration::<Filter>::default()
            .with_finality(DataFinality::DataStatusAccepted)
            .with_batch_size(10)
            .with_starting_cursor(new_starting_cursor)
            .with_filter(|mut filter| {
                filter.with_header(HeaderFilter::new());
                filter
            });

        let uri = "http://localhost:7171".parse().unwrap();
        let (mut data_stream, data_client) = ClientBuilder::<Filter, Block>::default()
            .connect(uri)
            .await
            .unwrap();
        data_client.send(configuration).await.unwrap();
        info!("re-connected. tests starting");
        // first message should be warning of reorg
        match data_stream.try_next().await.unwrap().unwrap() {
            DataMessage::Invalidate { cursor } => {
                // block 5 is reorged too
                assert_eq!(cursor.unwrap().order_key, 4);
            }
            _ => unreachable!(),
        }
    }

    info!("all done");
    cts.cancel();
    let _ = tokio::join!(node_handle);
}

async fn main() {
    let connect_addr = "http://localhost:8080";
    let url = url::Url::parse(&connect_addr).unwrap();
    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
    println!("WebSocket handshake has been successfully completed");


    let (stdin_tx, stdin_rx) = futures_channel::mpsc::unbounded();
    tokio::spawn(read_stdin(stdin_tx));

    let (write, read) = ws_stream.split();

    let stdin_to_ws = stdin_rx.map(Ok).forward(write);
    let ws_to_stdout = {
        read.for_each(|message| async {
            let data = message.unwrap().into_data();
            tokio::io::stdout().write_all(&data).await.unwrap();
        })
    };

    pin_mut!(stdin_to_ws, ws_to_stdout);
    future::select(stdin_to_ws, ws_to_stdout).await;
}

// Our helper method which will read data from stdin and send it along the
// sender provided.
async fn read_stdin(tx: futures_channel::mpsc::UnboundedSender<Message>) {
    let mut stdin = tokio::io::stdin();
    loop {
        let mut buf = vec![0; 1024];
        let n = match stdin.read(&mut buf).await {
            Err(_) | Ok(0) => break,
            Ok(n) => n,
        };
        buf.truncate(n);
        tx.unbounded_send(Message::binary(buf)).unwrap();
    }
}
