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
use crate::protocol::wings::v1 as pb;
use crate::protocol::wings::v1::admin_service_client::AdminServiceClient;

use super::AdminError;

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
        let request = pb::CreateTenantRequest {
            tenant_id: name.id().to_string(),
            tenant: None,
        };

        self.client
            .clone()
            .create_tenant(request)
            .await
            .map_err(|status| status_to_admin_error("tenant", status))?
            .into_inner()
            .try_into()
    }

    async fn get_tenant(&self, name: TenantName) -> AdminResult<Tenant> {
        let request = pb::GetTenantRequest {
            name: name.to_string(),
        };

        self.client
            .clone()
            .get_tenant(request)
            .await
            .map_err(|status| status_to_admin_error("tenant", status))?
            .into_inner()
            .try_into()
    }

    async fn list_tenants(&self, request: ListTenantsRequest) -> AdminResult<ListTenantsResponse> {
        let request = pb::ListTenantsRequest::from(request);

        self.client
            .clone()
            .list_tenants(request)
            .await
            .map_err(|status| status_to_admin_error("tenant", status))?
            .into_inner()
            .try_into()
    }

    async fn delete_tenant(&self, name: TenantName) -> AdminResult<()> {
        let request = pb::DeleteTenantRequest {
            name: name.to_string(),
        };

        self.client
            .clone()
            .delete_tenant(request)
            .await
            .map_err(|status| status_to_admin_error("tenant", status))?;

        Ok(())
    }

    async fn create_namespace(
        &self,
        name: NamespaceName,
        options: NamespaceOptions,
    ) -> AdminResult<Namespace> {
        let request = pb::CreateNamespaceRequest {
            parent: name.parent().to_string(),
            namespace_id: name.id().to_string(),
            namespace: pb::Namespace::from(options).into(),
        };

        self.client
            .clone()
            .create_namespace(request)
            .await
            .map_err(|status| status_to_admin_error("namespace", status))?
            .into_inner()
            .try_into()
    }

    async fn get_namespace(&self, name: NamespaceName) -> AdminResult<Namespace> {
        let request = pb::GetNamespaceRequest {
            name: name.to_string(),
        };

        self.client
            .clone()
            .get_namespace(request)
            .await
            .map_err(|status| status_to_admin_error("namespace", status))?
            .into_inner()
            .try_into()
    }

    async fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> AdminResult<ListNamespacesResponse> {
        let request = pb::ListNamespacesRequest::from(request);

        self.client
            .clone()
            .list_namespaces(request)
            .await
            .map_err(|status| status_to_admin_error("namespace", status))?
            .into_inner()
            .try_into()
    }

    async fn delete_namespace(&self, name: NamespaceName) -> AdminResult<()> {
        let request = pb::DeleteNamespaceRequest {
            name: name.to_string(),
        };

        self.client
            .clone()
            .delete_namespace(request)
            .await
            .map_err(|status| status_to_admin_error("tenant", status))?;

        Ok(())
    }

    async fn create_topic(&self, name: TopicName, options: TopicOptions) -> AdminResult<Topic> {
        let request = pb::CreateTopicRequest {
            parent: name.parent().to_string(),
            topic_id: name.id().to_string(),
            topic: pb::Topic::from(options).into(),
        };

        self.client
            .clone()
            .create_topic(request)
            .await
            .map_err(|status| status_to_admin_error("topic", status))?
            .into_inner()
            .try_into()
    }

    async fn get_topic(&self, name: TopicName) -> AdminResult<Topic> {
        let request = pb::GetTopicRequest {
            name: name.to_string(),
        };

        self.client
            .clone()
            .get_topic(request)
            .await
            .map_err(|status| status_to_admin_error("topic", status))?
            .into_inner()
            .try_into()
    }

    async fn list_topics(&self, request: ListTopicsRequest) -> AdminResult<ListTopicsResponse> {
        let request = pb::ListTopicsRequest::from(request);

        self.client
            .clone()
            .list_topics(request)
            .await
            .map_err(|status| status_to_admin_error("topic", status))?
            .into_inner()
            .try_into()
    }

    async fn delete_topic(&self, name: TopicName, force: bool) -> AdminResult<()> {
        let request = pb::DeleteTopicRequest {
            name: name.to_string(),
            force,
        };

        self.client
            .clone()
            .delete_topic(request)
            .await
            .map_err(|status| status_to_admin_error("topic", status))?;

        Ok(())
    }
}

fn status_to_admin_error(resource: &'static str, status: tonic::Status) -> AdminError {
    use tonic::Code;

    match status.code() {
        Code::NotFound => AdminError::NotFound {
            resource,
            message: status.message().to_string(),
        },
        Code::AlreadyExists => AdminError::AlreadyExists {
            resource,
            message: status.message().to_string(),
        },
        Code::InvalidArgument => AdminError::InvalidArgument {
            resource,
            message: status.message().to_string(),
        },
        _ => AdminError::Internal {
            message: format!("unknown error from remote service: {}", status.message()),
        },
    }
}
