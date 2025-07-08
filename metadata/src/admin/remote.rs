use tonic::transport::Channel;

use crate::admin::pb::admin_service_client::AdminServiceClient;
use crate::admin::Admin;

/// The remote admin service.
pub struct RemoteAdminService {
    /// The gRPC client.
    client: AdminServiceClient<Channel>,
}

impl RemoteAdminService {
    /// Create a new remote admin service.
    pub fn new(channel: Channel) -> Self {
        Self {
            client: AdminServiceClient::new(channel),
        }
    }
}

#[async_trait::async_trait]
impl Admin for RemoteAdminService {
    /// Create a new tenant.
    async fn create_tenant(&self, name: super::TenantName) -> super::AdminResult<super::Tenant> {
        let request = super::pb::CreateTenantRequest {
            tenant_id: name.id(),
            tenant: Some(super::pb::Tenant {}),
        };

        let response = self.client.clone().create_tenant(request).await?;

        Ok(response.into_inner().into())
    }
}
