//! gRPC server implementation for the AdminService.

use std::sync::Arc;

use async_trait::async_trait;
use tonic::{Request, Response, Status};

use crate::admin::{
    Admin, AdminError, ListNamespacesRequest, ListTenantsRequest, ListTopicsRequest, NamespaceName,
    NamespaceOptions, TenantName, TopicName, TopicOptions,
};
use crate::protocol::wings::v1::{
    self as pb,
    admin_service_server::{AdminService as AdminServiceTrait, AdminServiceServer},
};
use crate::resource::ResourceError;

/// gRPC server implementation for the AdminService.
pub struct AdminService {
    admin: Arc<dyn Admin>,
}

impl AdminService {
    /// Create a new AdminService server with the given admin implementation.
    pub fn new(admin: Arc<dyn Admin>) -> Self {
        Self { admin }
    }

    pub fn into_service(self) -> AdminServiceServer<Self> {
        AdminServiceServer::new(self)
    }
}

#[async_trait]
impl AdminServiceTrait for AdminService {
    async fn create_tenant(
        &self,
        request: Request<pb::CreateTenantRequest>,
    ) -> Result<Response<pb::Tenant>, Status> {
        let request = request.into_inner();

        let tenant_name = TenantName::new(request.tenant_id)
            .map_err(AdminError::InvalidResourceName)
            .map_err(Into::into)
            .map_err(admin_error_to_status)?;

        let tenant = self
            .admin
            .create_tenant(tenant_name)
            .await
            .map_err(admin_error_to_status)?;

        Ok(Response::new(tenant.into()))
    }

    async fn get_tenant(
        &self,
        request: Request<pb::GetTenantRequest>,
    ) -> Result<Response<pb::Tenant>, Status> {
        let request = request.into_inner();

        let tenant_name = TenantName::parse(&request.name)
            .map_err(AdminError::InvalidResourceName)
            .map_err(Into::into)
            .map_err(admin_error_to_status)?;

        let tenant = self
            .admin
            .get_tenant(tenant_name)
            .await
            .map_err(admin_error_to_status)?;

        Ok(Response::new(tenant.into()))
    }

    async fn list_tenants(
        &self,
        request: Request<pb::ListTenantsRequest>,
    ) -> Result<Response<pb::ListTenantsResponse>, Status> {
        let request = ListTenantsRequest::from(request.into_inner());

        let response = self
            .admin
            .list_tenants(request)
            .await
            .map_err(admin_error_to_status)?;

        Ok(Response::new(response.into()))
    }

    async fn delete_tenant(
        &self,
        request: Request<pb::DeleteTenantRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();

        let tenant_name = TenantName::parse(&request.name)
            .map_err(AdminError::InvalidResourceName)
            .map_err(Into::into)
            .map_err(admin_error_to_status)?;

        self.admin
            .delete_tenant(tenant_name)
            .await
            .map_err(admin_error_to_status)?;

        Ok(Response::new(()))
    }

    async fn create_namespace(
        &self,
        request: Request<pb::CreateNamespaceRequest>,
    ) -> Result<Response<pb::Namespace>, Status> {
        let request = request.into_inner();

        let tenant_name = TenantName::parse(&request.parent)
            .map_err(AdminError::InvalidResourceName)
            .map_err(Into::into)
            .map_err(admin_error_to_status)?;

        let namespace_name = NamespaceName::new(request.namespace_id, tenant_name)
            .map_err(AdminError::InvalidResourceName)
            .map_err(Into::into)
            .map_err(admin_error_to_status)?;

        let options = NamespaceOptions::try_from(request.namespace.unwrap_or_default())
            .map_err(admin_error_to_status)?;

        let namespace = self
            .admin
            .create_namespace(namespace_name, options)
            .await
            .map_err(admin_error_to_status)?;

        Ok(Response::new(namespace.into()))
    }

    async fn get_namespace(
        &self,
        request: Request<pb::GetNamespaceRequest>,
    ) -> Result<Response<pb::Namespace>, Status> {
        let request = request.into_inner();

        let namespace_name = NamespaceName::parse(&request.name)
            .map_err(AdminError::InvalidResourceName)
            .map_err(Into::into)
            .map_err(admin_error_to_status)?;

        let namespace = self
            .admin
            .get_namespace(namespace_name)
            .await
            .map_err(admin_error_to_status)?;

        Ok(Response::new(namespace.into()))
    }

    async fn list_namespaces(
        &self,
        request: Request<pb::ListNamespacesRequest>,
    ) -> Result<Response<pb::ListNamespacesResponse>, Status> {
        let request = request.into_inner();
        let request = ListNamespacesRequest::try_from(request).map_err(admin_error_to_status)?;

        let response = self
            .admin
            .list_namespaces(request)
            .await
            .map_err(admin_error_to_status)?;

        Ok(Response::new(response.into()))
    }

    async fn delete_namespace(
        &self,
        request: Request<pb::DeleteNamespaceRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();

        let namespace_name = NamespaceName::parse(&request.name)
            .map_err(AdminError::InvalidResourceName)
            .map_err(Into::into)
            .map_err(admin_error_to_status)?;

        self.admin
            .delete_namespace(namespace_name)
            .await
            .map_err(admin_error_to_status)?;

        Ok(Response::new(()))
    }

    async fn create_topic(
        &self,
        request: Request<pb::CreateTopicRequest>,
    ) -> Result<Response<pb::Topic>, Status> {
        let request = request.into_inner();

        let namespace_name = NamespaceName::parse(&request.parent)
            .map_err(AdminError::InvalidResourceName)
            .map_err(Into::into)
            .map_err(admin_error_to_status)?;

        let topic_name = TopicName::new(request.topic_id, namespace_name)
            .map_err(AdminError::InvalidResourceName)
            .map_err(Into::into)
            .map_err(admin_error_to_status)?;

        let options = TopicOptions::try_from(request.topic.unwrap_or_default())
            .map_err(admin_error_to_status)?;

        let topic = self
            .admin
            .create_topic(topic_name, options)
            .await
            .map_err(admin_error_to_status)?;

        Ok(Response::new(topic.into()))
    }

    async fn get_topic(
        &self,
        request: Request<pb::GetTopicRequest>,
    ) -> Result<Response<pb::Topic>, Status> {
        let request = request.into_inner();

        let topic_name = TopicName::parse(&request.name)
            .map_err(AdminError::InvalidResourceName)
            .map_err(Into::into)
            .map_err(admin_error_to_status)?;

        let topic = self
            .admin
            .get_topic(topic_name)
            .await
            .map_err(admin_error_to_status)?;

        Ok(Response::new(topic.into()))
    }

    async fn list_topics(
        &self,
        request: Request<pb::ListTopicsRequest>,
    ) -> Result<Response<pb::ListTopicsResponse>, Status> {
        let request = request.into_inner();

        let request = ListTopicsRequest::try_from(request).map_err(admin_error_to_status)?;

        let response = self
            .admin
            .list_topics(request)
            .await
            .map_err(admin_error_to_status)?;

        Ok(Response::new(response.into()))
    }

    async fn delete_topic(
        &self,
        request: Request<pb::DeleteTopicRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();

        let topic_name = TopicName::parse(&request.name)
            .map_err(AdminError::InvalidResourceName)
            .map_err(Into::into)
            .map_err(admin_error_to_status)?;

        self.admin
            .delete_topic(topic_name, request.force)
            .await
            .map_err(admin_error_to_status)?;

        Ok(Response::new(()))
    }
}

fn admin_error_to_status(error: error_stack::Report<AdminError>) -> Status {
    match error.current_context() {
        AdminError::NotFound { resource, message } => {
            Status::not_found(format!("{} not found: {}", resource, message))
        }
        AdminError::AlreadyExists { resource, message } => {
            Status::already_exists(format!("{} already exists: {}", resource, message))
        }
        AdminError::InvalidArgument { resource, message } => {
            Status::invalid_argument(format!("invalid {}: {}", resource, message))
        }
        AdminError::InvalidResourceName(resource_error) => match resource_error {
            ResourceError::InvalidFormat { expected, actual } => Status::invalid_argument(format!(
                "invalid resource name format: expected '{}' but got '{}'",
                expected, actual
            )),
            ResourceError::InvalidName { name } => {
                Status::invalid_argument(format!("invalid resource name: {}", name))
            }
            ResourceError::MissingParent { name } => {
                Status::invalid_argument(format!("missing parent resource in name: {}", name))
            }
            ResourceError::InvalidResourceId { id } => {
                Status::invalid_argument(format!("invalid resource id: {}", id))
            }
        },
        AdminError::Internal { message } => {
            Status::internal(format!("internal error: {}", message))
        }
    }
}
