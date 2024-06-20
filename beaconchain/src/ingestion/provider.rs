use error_stack::{Result, ResultExt};
use reqwest::Client;

pub mod models {
    use alloy_primitives::{Address, Bytes};
    use serde::{Deserialize, Serialize};
    use serde_with::{serde_as, DisplayFromStr};

    pub use alloy_primitives::B256;

    pub use alloy_rpc_types_beacon::header::HeaderResponse;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct BeaconBlockResponse {
        pub finalized: bool,
        pub data: BeaconBlockData,
    }

    #[serde_as]
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct BeaconBlockData {
        pub message: BeaconBlock,
        pub signature: Bytes,
    }

    #[serde_as]
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct BeaconBlock {
        #[serde_as(as = "DisplayFromStr")]
        pub slot: u64,
        #[serde_as(as = "DisplayFromStr")]
        pub proposer_index: u64,
        pub parent_root: B256,
        pub state_root: B256,
        pub body: BeaconBlockBody,
    }

    #[serde_as]
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct BeaconBlockBody {
        pub randao_reveal: Bytes,
        pub eth1_data: Eth1Data,
        pub graffiti: B256,
        pub execution_payload: ExecutionPayload,
    }

    #[serde_as]
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct Eth1Data {
        #[serde_as(as = "DisplayFromStr")]
        pub deposit_count: u64,
        pub deposit_root: B256,
        pub block_hash: B256,
    }

    #[serde_as]
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ExecutionPayload {
        pub parent_hash: B256,
        pub fee_recipient: Address,
        pub state_root: B256,
        pub receipts_root: B256,
        pub logs_bloom: Bytes,
        pub prev_randao: B256,
        #[serde_as(as = "DisplayFromStr")]
        pub block_number: u64,
        #[serde_as(as = "DisplayFromStr")]
        pub timestamp: u64,
        pub transactions: Vec<Bytes>,
        pub withdrawals: Vec<Withdrawal>,
    }

    #[serde_as]
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct Withdrawal {
        #[serde_as(as = "DisplayFromStr")]
        pub index: u64,
        #[serde_as(as = "DisplayFromStr")]
        pub validator_index: u64,
        pub address: Address,
        #[serde_as(as = "DisplayFromStr")]
        pub amount: u64,
    }
}

#[derive(Debug)]
pub enum BeaconApiError {
    Request,
    NotFound,
    DeserializeResponse,
}

/// Block identifier.
#[derive(Debug, Clone)]
pub enum BlockId {
    /// Current head block.
    Head,
    /// Most recent finalized block.
    Finalized,
    /// Block by slot.
    Slot(u64),
    /// Block by root.
    BlockRoot(models::B256),
}

#[derive(Clone)]
pub struct BeaconApiProvider {
    client: Client,
    url: String,
}

impl BeaconApiProvider {
    pub fn new(url: impl Into<String>) -> Self {
        let url = url.into().trim_end_matches('/').to_string();
        Self {
            client: Client::new(),
            url,
        }
    }

    pub async fn get_header(
        &self,
        block_id: BlockId,
    ) -> Result<models::HeaderResponse, BeaconApiError> {
        let request = HeaderRequest::new(block_id);
        self.send_request(request).await
    }

    pub async fn get_block(
        &self,
        block_id: BlockId,
    ) -> Result<models::BeaconBlockResponse, BeaconApiError> {
        let request = BlockRequest::new(block_id);
        self.send_request(request).await
    }

    /// Send a request to the beacon node.
    ///
    /// TODO: this function can be turned into a `Transport` trait if we ever need it.
    async fn send_request<Req>(&self, request: Req) -> Result<Req::Response, BeaconApiError>
    where
        Req: BeaconApiRequest,
    {
        let url = format!("{}{}", self.url, request.path());
        let response = self
            .client
            .get(&url)
            .header("Content-Type", "application/json")
            .send()
            .await
            .change_context(BeaconApiError::Request)?;

        if response.status().as_u16() == 404 {
            return Err(BeaconApiError::NotFound.into());
        }

        let text_response = response
            .text()
            .await
            .change_context(BeaconApiError::Request)?;
        let response = serde_json::from_str(&text_response)
            .change_context(BeaconApiError::DeserializeResponse)?;

        Ok(response)
    }
}

pub trait BeaconApiRequest {
    type Response: serde::de::DeserializeOwned;

    fn path(&self) -> String;
}

pub struct HeaderRequest {
    block_id: BlockId,
}

pub struct BlockRequest {
    block_id: BlockId,
}

impl std::fmt::Display for BlockId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlockId::Head => write!(f, "head"),
            BlockId::Finalized => write!(f, "finalized"),
            BlockId::Slot(slot) => write!(f, "{}", slot),
            BlockId::BlockRoot(root) => write!(f, "{}", root),
        }
    }
}

impl std::fmt::Display for BeaconApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BeaconApiError::Request => write!(f, "failed to send request"),
            BeaconApiError::DeserializeResponse => write!(f, "failed to deserialize response"),
            BeaconApiError::NotFound => write!(f, "not found"),
        }
    }
}

impl error_stack::Context for BeaconApiError {}

impl HeaderRequest {
    pub fn new(block_id: BlockId) -> Self {
        Self { block_id }
    }
}

impl BeaconApiRequest for HeaderRequest {
    type Response = models::HeaderResponse;

    fn path(&self) -> String {
        format!("/eth/v1/beacon/headers/{}", self.block_id)
    }
}

impl BlockRequest {
    pub fn new(block_id: BlockId) -> Self {
        Self { block_id }
    }
}

impl BeaconApiRequest for BlockRequest {
    type Response = models::BeaconBlockResponse;

    fn path(&self) -> String {
        format!("/eth/v2/beacon/blocks/{}", self.block_id)
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::hex::FromHex;

    use super::{models::B256, BlockId};

    #[test]
    pub fn test_block_id_display() {
        assert_eq!(BlockId::Head.to_string(), "head");
        assert_eq!(BlockId::Finalized.to_string(), "finalized");
        assert_eq!(BlockId::Slot(1234).to_string(), "1234");
        let root =
            B256::from_hex("0xd2f1aae62645bc68f920b42e82edb47f56212fa45c27b24dc398e27d6fe844c2")
                .unwrap();
        assert_eq!(
            BlockId::BlockRoot(root).to_string(),
            "0xd2f1aae62645bc68f920b42e82edb47f56212fa45c27b24dc398e27d6fe844c2"
        );
    }
}
