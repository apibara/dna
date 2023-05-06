use std::time::Duration;

use apibara_core::{
    node::v1alpha2::DataFinality,
    starknet::v1alpha2::{Block, Filter, HeaderFilter},
};
use apibara_sdk::{ClientBuilder, Configuration, DataMessage};
use apibara_starknet::{start_node, StartArgs};
use futures::FutureExt;
use serde_json::json;
use testcontainers::{clients, core::WaitFor, Image, ImageArgs};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[derive(Default, Clone, Debug)]
pub struct Devnet;

#[derive(Default, Clone, Debug)]
pub struct DevnetArgs;

impl Image for Devnet {
    type Args = DevnetArgs;

    fn name(&self) -> String {
        "shardlabs/starknet-devnet".to_string()
    }

    fn tag(&self) -> String {
        "e0c5aa285b3f90f7f452008f41e0e780e4a5958f-seed0".to_string()
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::message_on_stdout(
            "Listening on http://0.0.0.0:5050/",
        )]
    }

    fn expose_ports(&self) -> Vec<u16> {
        vec![5050]
    }
}

impl ImageArgs for DevnetArgs {
    fn into_iterator(self) -> Box<dyn Iterator<Item = String>> {
        Box::new(vec!["--disable-rpc-request-validation".to_string()].into_iter())
    }
}

pub struct DevnetClient {
    client: reqwest::Client,
    base_url: String,
}

impl DevnetClient {
    pub fn new(base_url: String) -> Self {
        DevnetClient {
            client: reqwest::Client::new(),
            base_url,
        }
    }

    pub async fn mint(&self) -> Result<(), reqwest::Error> {
        let body = json!({
            "address": "0x2f11193236547f88303219f3952f7da05c62b01d6656467321ca7e186a39288",
            "amount": 1000,
        });
        let response = self
            .client
            .post(format!("{}/mint", self.base_url))
            .json(&body)
            .send()
            .await?;
        let _text = response.text().await?;
        Ok(())
    }

    pub async fn abort_blocks(&self, block_hash: &str) -> Result<(), reqwest::Error> {
        let body = json!({ "startingBlockHash": block_hash });
        let response = self
            .client
            .post(format!("{}/abort_blocks", self.base_url))
            .json(&body)
            .send()
            .await?;
        let text = response.text().await?;
        info!(response = text, "aborted blocks");
        Ok(())
    }
}

#[tokio::test]
#[ignore]
async fn test_reorg_from_client_pov() {
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
                        assert!(cursor.unique_key.len() > 0);
                    } else {
                        assert_eq!(i, 0);
                    }
                    assert_eq!(end_cursor.order_key, i);
                    assert!(end_cursor.unique_key.len() > 0);
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
