//! Remote admin service implementation that communicates with a remote admin service via gRPC.

use std::marker::Send;

use async_trait::async_trait;
use bytes::Bytes;
use http_body::Body;

use crate::admin::{
    Admin, AdminResult, ListNamespacesRequest, ListNamespacesResponse, ListTenantsRequest,
    ListTenantsResponse, ListTopicsRequest, ListTopicsResponse, Namespace, NamespaceName,
    NamespaceOptions, Tenant, TenantName, Topic, TopicName, TopicOptions,
};
use crate::protocol::wings::v1::CreateTenantRequest;
use crate::protocol::wings::v1::admin_service_client::AdminServiceClient;

use super::server::tenant_to_proto;

pub type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Remote admin service that communicates with a remote admin service via gRPC.
pub struct RemoteAdminService<T> {
    client: AdminServiceClient<T>,
}

impl<T> RemoteAdminService<T>
where
    T: tonic::client::GrpcService<tonic::body::Body> + Clone,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    /// Create a new remote admin service with the given transport.
    pub fn new(inner: T) -> Self {
        Self::new_with_client(AdminServiceClient::new(inner))
    }

    /// Create a new remote admin service with the given client.
    pub fn new_with_client(client: AdminServiceClient<T>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl<T> Admin for RemoteAdminService<T>
where
    T: tonic::client::GrpcService<tonic::body::Body> + Send + Sync + Clone,
    <T as tonic::client::GrpcService<tonic::body::Body>>::Future: Send,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    async fn create_tenant(&self, name: TenantName) -> AdminResult<Tenant> {
        let request = CreateTenantRequest {
            tenant_id: name.id().to_string(),
            tenant: None,
        };

        let response = self
            .client
            .clone()
            .create_tenant(request)
            .await
            .unwrap()
            .into_inner();

        let tenant_name = TenantName::parse(&response.name).unwrap();
        Ok(Tenant::new(tenant_name))
    }

    async fn get_tenant(&self, name: TenantName) -> AdminResult<Tenant> {
        todo!("implement get_tenant")
    }

    async fn list_tenants(&self, request: ListTenantsRequest) -> AdminResult<ListTenantsResponse> {
        todo!("implement list_tenants")
    }

    async fn delete_tenant(&self, name: TenantName) -> AdminResult<()> {
        todo!("implement delete_tenant")
    }

    async fn create_namespace(
        &self,
        name: NamespaceName,
        options: NamespaceOptions,
    ) -> AdminResult<Namespace> {
        todo!("implement create_namespace")
    }

    async fn get_namespace(&self, name: NamespaceName) -> AdminResult<Namespace> {
        todo!("implement get_namespace")
    }

    async fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> AdminResult<ListNamespacesResponse> {
        todo!("implement list_namespaces")
    }

    async fn delete_namespace(&self, name: NamespaceName) -> AdminResult<()> {
        todo!("implement delete_namespace")
    }

    async fn create_topic(&self, name: TopicName, options: TopicOptions) -> AdminResult<Topic> {
        todo!("implement create_topic")
    }

    async fn get_topic(&self, name: TopicName) -> AdminResult<Topic> {
        todo!("implement get_topic")
    }

    async fn list_topics(&self, request: ListTopicsRequest) -> AdminResult<ListTopicsResponse> {
        todo!("implement list_topics")
    }

    async fn delete_topic(&self, name: TopicName, force: bool) -> AdminResult<()> {
        todo!("implement delete_topic")
    }
}
