use apibara_core::quota::v1::{
    quota_client::QuotaClient as GrpcQuotaClient, CheckRequest, QuotaStatus as GrpcQuotaStatus,
    UpdateAndCheckRequest,
};
use hyper::Uri;
use tonic::{metadata::MetadataMap, transport::Channel, Request};
use tracing::debug;

#[derive(Debug, thiserror::Error)]
pub enum QuotaError {
    #[error("missing team metadata key")]
    MissingTeamMetadataKey,
    #[error("invalid team metadata value")]
    InvalidTeamMetadataKey,
    #[error("missing client metadata key")]
    MissingClientMetadataKey,
    #[error("invalid client metadata value")]
    InvalidClientMetadataKey,
    #[error("grpc error: {0}")]
    Grpc(#[from] tonic::transport::Error),
    #[error("grpc request error: {0}")]
    Request(#[from] tonic::Status),
}

#[derive(Debug, Clone)]
pub enum QuotaStatus {
    /// Quota left.
    Ok,
    /// Quota exceeded.
    Exceeded,
}

impl QuotaStatus {
    pub fn is_exceeded(&self) -> bool {
        match self {
            QuotaStatus::Ok => false,
            QuotaStatus::Exceeded => true,
        }
    }
}

#[derive(Debug, Clone)]
pub enum QuotaConfiguration {
    NoQuota,
    RemoteQuota {
        /// Network name, used for reporting.
        network_name: String,
        /// Metadata key used to identify the team.
        team_metadata_key: String,
        /// Metadata key used to identify the client.
        client_metadata_key: Option<String>,
        /// Quota server address.
        server_address: Uri,
    },
}

#[derive(Debug, Clone)]
pub struct QuotaClientFactory {
    configuration: QuotaConfiguration,
}

#[derive(Debug, Default, Clone)]
pub struct NoQuotaClient;

#[derive(Debug, Clone)]
pub struct RemoteQuotaClient {
    client: GrpcQuotaClient<Channel>,
    network_name: String,
    team_name: String,
    client_name: Option<String>,
}

pub enum QuotaClient {
    NoQuotaClient(NoQuotaClient),
    RemoteQuotaClient(RemoteQuotaClient),
}

impl QuotaClientFactory {
    pub fn new(configuration: QuotaConfiguration) -> Self {
        QuotaClientFactory { configuration }
    }

    pub async fn client_with_metadata(
        &self,
        metadata: &MetadataMap,
    ) -> Result<QuotaClient, QuotaError> {
        match &self.configuration {
            QuotaConfiguration::NoQuota => Ok(QuotaClient::no_quota()),
            QuotaConfiguration::RemoteQuota {
                network_name,
                team_metadata_key,
                client_metadata_key,
                server_address,
            } => {
                let team_name = metadata
                    .get(team_metadata_key)
                    .ok_or(QuotaError::MissingTeamMetadataKey)?
                    .to_str()
                    .map_err(|_| QuotaError::InvalidTeamMetadataKey)?
                    .to_string();

                let client_name = if let Some(client_metadata_key) = client_metadata_key {
                    let value = metadata
                        .get(client_metadata_key)
                        .ok_or(QuotaError::MissingClientMetadataKey)?
                        .to_str()
                        .map_err(|_| QuotaError::InvalidClientMetadataKey)?
                        .to_string();
                    Some(value)
                } else {
                    None
                };

                let endpoint = Channel::builder(server_address.clone());

                debug!(
                    server_address = %server_address,
                    team_name = %team_name,
                    client_name = ?client_name,
                    "using remote quota server"
                );

                let client = GrpcQuotaClient::connect(endpoint).await?;

                Ok(QuotaClient::remote_quota(
                    client,
                    team_name,
                    client_name,
                    network_name.clone(),
                ))
            }
        }
    }
}

impl QuotaClient {
    pub fn no_quota() -> Self {
        QuotaClient::NoQuotaClient(NoQuotaClient::new())
    }

    pub fn remote_quota(
        client: GrpcQuotaClient<Channel>,
        team_name: String,
        client_name: Option<String>,
        network_name: String,
    ) -> Self {
        let inner = RemoteQuotaClient::new(client, team_name, client_name, network_name);
        QuotaClient::RemoteQuotaClient(inner)
    }

    pub async fn check(&self) -> Result<QuotaStatus, QuotaError> {
        match self {
            QuotaClient::NoQuotaClient(client) => Ok(client.check()),
            QuotaClient::RemoteQuotaClient(client) => Ok(client.check().await?),
        }
    }

    pub async fn update_and_check(&self, du: u64) -> Result<QuotaStatus, QuotaError> {
        match self {
            QuotaClient::NoQuotaClient(client) => Ok(client.update_and_check(du)),
            QuotaClient::RemoteQuotaClient(client) => Ok(client.update_and_check(du).await?),
        }
    }
}

impl NoQuotaClient {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn check(&self) -> QuotaStatus {
        QuotaStatus::Ok
    }

    pub fn update_and_check(&self, _data_units: u64) -> QuotaStatus {
        QuotaStatus::Ok
    }
}

impl RemoteQuotaClient {
    pub fn new(
        client: GrpcQuotaClient<Channel>,
        team_name: String,
        client_name: Option<String>,
        network_name: String,
    ) -> Self {
        RemoteQuotaClient {
            client,
            network_name,
            team_name,
            client_name,
        }
    }

    pub async fn check(&self) -> Result<QuotaStatus, QuotaError> {
        let request = CheckRequest {
            network: self.network_name.clone(),
            team_name: self.team_name.clone(),
            client_name: self.client_name.clone(),
        };
        let request = Request::new(request);
        let response = self.client.clone().check(request).await?;
        let response = response.into_inner();
        if response.status == GrpcQuotaStatus::Ok as i32 {
            Ok(QuotaStatus::Ok)
        } else {
            Ok(QuotaStatus::Exceeded)
        }
    }

    pub async fn update_and_check(&self, du: u64) -> Result<QuotaStatus, QuotaError> {
        let request = UpdateAndCheckRequest {
            network: self.network_name.clone(),
            team_name: self.team_name.clone(),
            client_name: self.client_name.clone(),
            data_units: du,
        };
        let request = Request::new(request);
        let response = self.client.clone().update_and_check(request).await?;
        let response = response.into_inner();
        if response.status == GrpcQuotaStatus::Ok as i32 {
            Ok(QuotaStatus::Ok)
        } else {
            Ok(QuotaStatus::Exceeded)
        }
    }
}

impl QuotaError {
    pub fn human_readable(&self) -> &'static str {
        match &self {
            Self::InvalidTeamMetadataKey => "invalid team metadata value",
            Self::MissingTeamMetadataKey => "team metadata is required",
            Self::InvalidClientMetadataKey => "invalid client metadata value",
            Self::MissingClientMetadataKey => "client metadata is required",
            Self::Grpc(_) => "quota server error",
            _ => "internal",
        }
    }
}
