mod common;

use std::time::Duration;

use apibara_core::{
    node::v1alpha2::DataFinality,
    starknet::v1alpha2::{Block, Filter, HeaderFilter},
};
use apibara_node::o11y::init_opentelemetry;
use apibara_sdk::{configuration, ClientBuilder, Configuration, DataMessage};
use apibara_starknet::{start_node, StartArgs};
use futures::FutureExt;
use testcontainers::clients;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::info;

use common::{Devnet, DevnetClient};

#[tokio::test]
#[ignore]
async fn test_reorg_from_client_pov() {
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
                head_refresh_interval_ms: None,
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

        let (config_client, config_stream) = configuration::channel(128);
        config_client.send(configuration).await.unwrap();
        let uri = "http://localhost:7171".parse().unwrap();
        let mut data_stream = ClientBuilder::<Filter, Block>::default()
            .connect(uri, config_stream)
            .await
            .unwrap();
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

        let (config_client, config_stream) = configuration::channel(128);
        config_client.send(configuration).await.unwrap();
        let uri = "http://localhost:7171".parse().unwrap();
        let mut data_stream = ClientBuilder::<Filter, Block>::default()
            .connect(uri, config_stream)
            .await
            .unwrap();
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
