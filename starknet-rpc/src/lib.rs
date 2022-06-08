//! Crate to request data from the StarkNet RPC server.
use anyhow::{Context, Result};
use chrono::{NaiveDateTime, naive::serde::ts_seconds};
use jsonrpsee_core::{client::ClientT, rpc_params};
use jsonrpsee_http_client::{HttpClient, HttpClientBuilder};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use starknet::core::{serde::unsigned_field_element::UfeHex, types::FieldElement};

#[derive(Debug, Clone)]
pub enum BlockNumber {
    Number(u64),
    Latest,
    Pending,
}

#[derive(Debug, Clone)]
pub enum BlockHash {
    Hash(FieldElement),
    Latest,
    Pending,
}

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct Block {
    #[serde_as(as = "UfeHex")]
    pub block_hash: FieldElement,
    #[serde_as(as = "UfeHex")]
    pub parent_hash: FieldElement,
    pub block_number: u64,
    #[serde(with = "ts_seconds")]
    pub accepted_time: NaiveDateTime,
}

#[derive(Debug, Clone)]
pub struct RpcProvider {
    client: HttpClient,
}

impl RpcProvider {
    pub fn new(url: impl AsRef<str>) -> Result<RpcProvider> {
        let client = HttpClientBuilder::default().build(url)?;
        Ok(RpcProvider { client })
    }

    pub async fn get_block_by_number(&self, number: &BlockNumber) -> Result<Block> {
        let block = self
            .client
            .request(
                "starknet_getBlockByNumber",
                rpc_params![&number, "TXN_HASH"],
            )
            .await
            .context("failed to fetch block by number")?;
        Ok(block)
    }

    pub async fn get_block_by_hash(&self, hash: &BlockHash) -> Result<Block> {
        let block = self
            .client
            .request("starknet_getBlockByHash", rpc_params![&hash, "TXN_HASH"])
            .await
            .context(format!("failed to fetch block by hash: {:?}", hash))?;
        Ok(block)
    }
}

impl Serialize for BlockNumber {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            BlockNumber::Number(num) => serializer.serialize_u64(*num),
            BlockNumber::Latest => serializer.serialize_str("latest"),
            BlockNumber::Pending => serializer.serialize_str("pending"),
        }
    }
}

impl Serialize for BlockHash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            BlockHash::Hash(hash) => serializer.serialize_str(&format!("{:#064x}", hash)),
            BlockHash::Latest => serializer.serialize_str("latest"),
            BlockHash::Pending => serializer.serialize_str("pending"),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{BlockHash, BlockNumber, RpcProvider};
    use starknet::core::types::FieldElement;

    #[test]
    fn serialize_block_number() {
        let number = BlockNumber::Number(123);
        let number = serde_json::to_string(&number).expect("failed to ser number");
        assert!(number == "123");
        let latest = BlockNumber::Latest;
        let latest = serde_json::to_string(&latest).expect("failed to ser latest");
        assert!(latest == r#""latest""#);
        let pending = BlockNumber::Pending;
        let pending = serde_json::to_string(&pending).expect("failed to ser pending");
        assert!(pending == r#""pending""#);
    }

    #[test]
    fn serialize_block_hash() {
        let hash = FieldElement::from_hex_be(
            "0x49e194898e0c1d0ce01de33b68b81152438b4899f36a44c9fa6a4f7dd74376e",
        )
        .expect("failed to parse block hash");
        let hash = BlockHash::Hash(hash);
        let hash = serde_json::to_string(&hash).expect("failed to ser hash");
        assert!(hash == r#""0x049e194898e0c1d0ce01de33b68b81152438b4899f36a44c9fa6a4f7dd74376e""#);
        let latest = BlockHash::Latest;
        let latest = serde_json::to_string(&latest).expect("failed to ser latest");
        assert!(latest == r#""latest""#);
        let pending = BlockHash::Pending;
        let pending = serde_json::to_string(&pending).expect("failed to ser pending");
        assert!(pending == r#""pending""#);
    }

    #[tokio::test]
    async fn fetch_block_by_number() {
        let client =
            RpcProvider::new("http://localhost:9545").expect("failed to create rpc provider");

        let number = BlockNumber::Number(0);
        let block = client.get_block_by_number(&number).await.unwrap();
        assert!(block.block_number == 0);
        assert!(block.parent_hash == FieldElement::ZERO);

        let latest = BlockNumber::Latest;
        let block = client.get_block_by_number(&latest).await.unwrap();
        assert!(block.parent_hash != FieldElement::ZERO);
    }

    #[tokio::test]
    async fn fetch_block_by_hash() {
        let client =
            RpcProvider::new("http://localhost:9545").expect("failed to create rpc provider");

        let hash = FieldElement::from_hex_be(
            "0x7767f4eabf7e5e68fbf6bfd13cf9400a4ec97c52dec806f7d2380b70271d266",
        )
        .expect("failed to parse block hash");
        let hash = BlockHash::Hash(hash);
        let block = client.get_block_by_hash(&hash).await.unwrap();
        assert!(block.block_number == 231287);
    }
}
