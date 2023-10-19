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
use tempdir::TempDir;
use testcontainers::clients;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::info;

use common::{Devnet, DevnetClient};

// Starknet-devnet doesn't support RCP 0.4 yet
// #[tokio::test]
// #[ignore]
#[allow(dead_code)]
async fn test_starknet_reorgs() {
    init_opentelemetry().unwrap();

    let docker = clients::Cli::default();
    let devnet = docker.run(Devnet::default());

    let rpc_port = devnet.get_host_port_ipv4(5050);
    let devnet_client = DevnetClient::new(format!("http://localhost:{}", rpc_port));
    // share data between runs to test restarts.
    let tempdir = TempDir::new("test-starknet-reorgs").unwrap();

    let node_args = StartArgs {
        rpc: format!("http://localhost:{}/rpc", rpc_port),
        data: None,
        name: Some(
            tempdir
                .path()
                .to_path_buf()
                .into_os_string()
                .into_string()
                .unwrap(),
        ),
        wait_for_rpc: true,
        devnet: false,
        head_refresh_interval_ms: None,
        use_metadata: Vec::default(),
        blocks_per_second_limit: None,
        address: None,
        websocket_address: None,
        quota_server: None,
    };

    let configuration = Configuration::<Filter>::default()
        .with_finality(DataFinality::DataStatusAccepted)
        .with_batch_size(10)
        .with_filter(|mut filter| {
            filter.with_header(HeaderFilter::new());
            filter
        });

    {
        let cts = CancellationToken::new();
        let node_handle = tokio::spawn({
            let cts = cts.clone();
            let node_args = node_args.clone();
            async move {
                start_node(node_args, cts).await.unwrap();
            }
        });

        // give time for node to start
        tokio::time::sleep(Duration::from_secs(5)).await;

        // generate 10 new blocks
        for _ in 0..10 {
            devnet_client.mint().await.unwrap();
        }

        // check chain is 11 blocks long
        let uri = "http://localhost:7171".parse().unwrap();
        let (config_client, config_stream) = configuration::channel(128);
        config_client.send(configuration.clone()).await.unwrap();
        let mut data_stream = ClientBuilder::default()
            .connect(uri)
            .await
            .unwrap()
            .start_stream::<Filter, Block, _>(config_stream)
            .await
            .unwrap();

        info!("connected. tests starting");
        let mut block_hash = None;
        for i in 0..11 {
            let message = data_stream.try_next().await.unwrap().unwrap();
            match message {
                DataMessage::Data {
                    cursor: _cursor,
                    end_cursor: _end_cursor,
                    finality: _finality,
                    mut batch,
                } => {
                    let block = batch.remove(0);
                    if i == 5 {
                        block_hash =
                            Some(block.header.clone().unwrap().block_hash.unwrap().to_hex());
                    }
                }
                _ => unreachable!(),
            }
        }
        let next_message = data_stream.try_next().now_or_never();
        assert!(next_message.is_none());

        info!("all done");
        cts.cancel();
        let _ = tokio::join!(node_handle);

        // abort blocks after 5.
        devnet_client
            .abort_blocks(&block_hash.unwrap())
            .await
            .unwrap();
    };

    info!("restarting node");
    // allow db file to be closed.
    tokio::time::sleep(Duration::from_secs(5)).await;

    {
        let cts = CancellationToken::new();
        info!(args = ?node_args, "starting node");
        let node_handle = tokio::spawn({
            let cts = cts.clone();
            async move {
                start_node(node_args, cts).await.unwrap();
            }
        });

        info!("restarted");
        // give time for node to start
        tokio::time::sleep(Duration::from_secs(5)).await;
        info!("reconnecting...");

        // now stream is shorter
        let (config_client, config_stream) = configuration::channel(128);
        config_client.send(configuration).await.unwrap();
        let uri = "http://localhost:7171".parse().unwrap();
        let mut data_stream = ClientBuilder::default()
            .connect(uri)
            .await
            .unwrap()
            .start_stream::<Filter, Block, _>(config_stream)
            .await
            .unwrap();

        info!("reconnected. tests starting");
        for _ in 0..5 {
            data_stream.try_next().await.unwrap().unwrap();
        }
        let next_message = data_stream.try_next().now_or_never();
        assert!(next_message.is_none());

        cts.cancel();
        let _ = tokio::join!(node_handle);
    };
}
