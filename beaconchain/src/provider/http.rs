use std::{fmt::Debug, time::Duration};

use error_stack::{Report, Result, ResultExt};
use reqwest::{
    header::{HeaderMap, HeaderValue},
    Client,
};

use crate::provider::models;

#[derive(Debug)]
pub enum BeaconApiError {
    Request,
    NotFound,
    DeserializeResponse,
    Timeout,
    Unauthorized,
    ServerError,
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
    options: BeaconApiProviderOptions,
}

pub trait BeaconApiProviderErrorExt {
    fn is_not_found(&self) -> bool;
}

#[derive(Debug, Clone)]
pub struct BeaconApiProviderOptions {
    /// Timeout for normal requests.
    pub timeout: Duration,
    /// Timeout for validators requests.
    pub validators_timeout: Duration,
    /// Headers to send with the requests.
    pub headers: HeaderMap<HeaderValue>,
}

impl BeaconApiProvider {
    pub fn new(url: impl Into<String>, options: BeaconApiProviderOptions) -> Self {
        let url = url.into().trim_end_matches('/').to_string();
        Self {
            client: Client::new(),
            url,
            options,
        }
    }

    pub async fn get_header(
        &self,
        block_id: BlockId,
    ) -> Result<models::HeaderResponse, BeaconApiError> {
        let request = HeaderRequest::new(block_id);
        self.send_request(request, self.options.timeout).await
    }

    pub async fn get_block(
        &self,
        block_id: BlockId,
    ) -> Result<models::BeaconBlockResponse, BeaconApiError> {
        let request = BlockRequest::new(block_id);
        self.send_request(request, self.options.timeout).await
    }

    pub async fn get_blob_sidecar(
        &self,
        block_id: BlockId,
    ) -> Result<models::BlobSidecarResponse, BeaconApiError> {
        let request = BlobSidecarRequest::new(block_id);
        self.send_request(request, self.options.timeout).await
    }

    pub async fn get_validators(
        &self,
        block_id: BlockId,
    ) -> Result<models::ValidatorsResponse, BeaconApiError> {
        let request = ValidatorsRequest::new(block_id);
        self.send_request(request, self.options.validators_timeout)
            .await
    }

    pub async fn get_block_root(
        &self,
        block_id: BlockId,
    ) -> Result<models::BlockRootResponse, BeaconApiError> {
        let request = BlockRootRequest::new(block_id);
        self.send_request(request, self.options.timeout).await
    }

    /// Send a request to the beacon node.
    ///
    /// TODO: this function can be turned into a `Transport` trait if we ever need it.
    #[tracing::instrument(level = "debug", skip(self))]
    async fn send_request<Req>(
        &self,
        request: Req,
        timeout: Duration,
    ) -> Result<Req::Response, BeaconApiError>
    where
        Req: BeaconApiRequest + Debug,
    {
        let url = format!("{}{}", self.url, request.path());
        let response = match self
            .client
            .get(&url)
            .header("Content-Type", "application/json")
            .headers(self.options.headers.clone())
            .timeout(timeout)
            .send()
            .await
        {
            Ok(response) => response,
            Err(err) if err.is_timeout() => {
                return Err(err).change_context(BeaconApiError::Timeout);
            }
            Err(err) => {
                return Err(err).change_context(BeaconApiError::Timeout);
            }
        };

        if response.status().as_u16() == 404 {
            return Err(BeaconApiError::NotFound.into());
        }

        if response.status().as_u16() == 401 {
            return Err(BeaconApiError::Unauthorized.into());
        }

        if response.status().as_u16() != 200 {
            return Err(BeaconApiError::ServerError.into());
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

#[derive(Debug)]
pub struct HeaderRequest {
    block_id: BlockId,
}

#[derive(Debug)]
pub struct BlockRequest {
    block_id: BlockId,
}

#[derive(Debug)]
pub struct BlobSidecarRequest {
    block_id: BlockId,
}

#[derive(Debug)]
pub struct ValidatorsRequest {
    block_id: BlockId,
}

#[derive(Debug)]
pub struct BlockRootRequest {
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

pub trait BeaconApiErrorExt {
    fn is_not_found(&self) -> bool;
}

impl BeaconApiError {
    pub fn is_not_found(&self) -> bool {
        matches!(self, BeaconApiError::NotFound)
    }
}

impl BeaconApiErrorExt for Report<BeaconApiError> {
    fn is_not_found(&self) -> bool {
        self.current_context().is_not_found()
    }
}

impl std::fmt::Display for BeaconApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BeaconApiError::Request => write!(f, "failed to send request"),
            BeaconApiError::DeserializeResponse => write!(f, "failed to deserialize response"),
            BeaconApiError::NotFound => write!(f, "not found"),
            BeaconApiError::Timeout => write!(f, "the request timed out"),
            BeaconApiError::Unauthorized => write!(f, "unauthorized"),
            BeaconApiError::ServerError => write!(f, "server error"),
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

impl BlobSidecarRequest {
    pub fn new(block_id: BlockId) -> Self {
        Self { block_id }
    }
}

impl BeaconApiRequest for BlobSidecarRequest {
    type Response = models::BlobSidecarResponse;

    fn path(&self) -> String {
        format!("/eth/v1/beacon/blob_sidecars/{}", self.block_id)
    }
}

impl ValidatorsRequest {
    pub fn new(block_id: BlockId) -> Self {
        Self { block_id }
    }
}

impl BeaconApiRequest for ValidatorsRequest {
    type Response = models::ValidatorsResponse;

    fn path(&self) -> String {
        format!("/eth/v1/beacon/states/{}/validators", self.block_id)
    }
}

impl BlockRootRequest {
    pub fn new(block_id: BlockId) -> Self {
        Self { block_id }
    }
}

impl BeaconApiRequest for BlockRootRequest {
    type Response = models::BlockRootResponse;

    fn path(&self) -> String {
        format!("/eth/v1/beacon/blocks/{}/root", self.block_id)
    }
}

impl Default for BeaconApiProviderOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(5),
            validators_timeout: Duration::from_secs(60),
            headers: HeaderMap::default(),
        }
    }
}

impl BeaconApiProviderErrorExt for Report<BeaconApiError> {
    fn is_not_found(&self) -> bool {
        matches!(self.current_context(), BeaconApiError::NotFound)
    }
}
