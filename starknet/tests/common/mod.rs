#![allow(dead_code)]
use serde_json::json;
use testcontainers::{core::WaitFor, Image, ImageArgs};
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
        "90e05410dfa18452199c8ee2ffba465e9d2642d6-seed0".to_string()
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
