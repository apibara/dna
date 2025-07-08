//! gRPC server implementation for the AdminService.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tonic::{Request, Response, Status};

use crate::admin::{
    Admin, AdminError, ListNamespacesRequest, ListTenantsRequest, ListTopicsRequest, Namespace,
    NamespaceName, NamespaceOptions, SecretName, Tenant, TenantName, Topic, TopicName,
    TopicOptions,
};
use crate::protocol::wings::v1::admin_service_server::AdminServiceServer;
use crate::protocol::wings::v1::{
    CreateNamespaceRequest, CreateTenantRequest, CreateTopicRequest, DeleteNamespaceRequest,
    DeleteTenantRequest, DeleteTopicRequest, GetNamespaceRequest, GetTenantRequest,
    GetTopicRequest, ListNamespacesRequest as ProtoListNamespacesRequest,
    ListNamespacesResponse as ProtoListNamespacesResponse,
    ListTenantsRequest as ProtoListTenantsRequest, ListTenantsResponse as ProtoListTenantsResponse,
    ListTopicsRequest as ProtoListTopicsRequest, ListTopicsResponse as ProtoListTopicsResponse,
    Namespace as ProtoNamespace, Tenant as ProtoTenant, Topic as ProtoTopic,
    admin_service_server::AdminService as AdminServiceTrait,
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
        request: Request<CreateTenantRequest>,
    ) -> Result<Response<ProtoTenant>, Status> {
        let req = request.into_inner();

        let tenant_name = TenantName::new(req.tenant_id)
            .map_err(|e| Status::invalid_argument(format!("invalid tenant id: {}", e)))?;

        let tenant = self
            .admin
            .create_tenant(tenant_name)
            .await
            .map_err(admin_error_to_status)?;

        Ok(Response::new(tenant_to_proto(&tenant)))
    }

    async fn get_tenant(
        &self,
        request: Request<GetTenantRequest>,
    ) -> Result<Response<ProtoTenant>, Status> {
        let req = request.into_inner();

        let tenant_name = TenantName::parse(&req.name)
            .map_err(|e| Status::invalid_argument(format!("invalid tenant name: {}", e)))?;

        let tenant = self
            .admin
            .get_tenant(tenant_name)
            .await
            .map_err(admin_error_to_status)?;

        Ok(Response::new(tenant_to_proto(&tenant)))
    }

    async fn list_tenants(
        &self,
        request: Request<ProtoListTenantsRequest>,
    ) -> Result<Response<ProtoListTenantsResponse>, Status> {
        let req = request.into_inner();

        let page_size = if req.page_size <= 0 {
            None
        } else {
            Some(req.page_size)
        };

        let page_token = if req.page_token.is_empty() {
            None
        } else {
            Some(req.page_token)
        };

        let list_request = ListTenantsRequest {
            page_size,
            page_token,
        };

        let response = self
            .admin
            .list_tenants(list_request)
            .await
            .map_err(admin_error_to_status)?;

        let proto_response = ProtoListTenantsResponse {
            tenants: response
                .tenants
                .into_iter()
                .map(|t| tenant_to_proto(&t))
                .collect(),
            next_page_token: response.next_page_token.unwrap_or_default(),
        };

        Ok(Response::new(proto_response))
    }

    async fn delete_tenant(
        &self,
        request: Request<DeleteTenantRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();

        let tenant_name = TenantName::parse(&req.name)
            .map_err(|e| Status::invalid_argument(format!("invalid tenant name: {}", e)))?;

        self.admin
            .delete_tenant(tenant_name)
            .await
            .map_err(admin_error_to_status)?;

        Ok(Response::new(()))
    }

    async fn create_namespace(
        &self,
        request: Request<CreateNamespaceRequest>,
    ) -> Result<Response<ProtoNamespace>, Status> {
        let req = request.into_inner();

        let namespace_name = parse_namespace_name(&req.namespace_id, &req.parent)?;

        let namespace_proto = req
            .namespace
            .ok_or_else(|| Status::invalid_argument("namespace field is required"))?;

        let options = proto_to_namespace_options(&namespace_proto)?;

        let namespace = self
            .admin
            .create_namespace(namespace_name, options)
            .await
            .map_err(admin_error_to_status)?;

        Ok(Response::new(namespace_to_proto(&namespace)))
    }

    async fn get_namespace(
        &self,
        request: Request<GetNamespaceRequest>,
    ) -> Result<Response<ProtoNamespace>, Status> {
        let req = request.into_inner();

        let namespace_name = parse_namespace_name_from_full(&req.name)?;

        let namespace = self
            .admin
            .get_namespace(namespace_name)
            .await
            .map_err(admin_error_to_status)?;

        Ok(Response::new(namespace_to_proto(&namespace)))
    }

    async fn list_namespaces(
        &self,
        request: Request<ProtoListNamespacesRequest>,
    ) -> Result<Response<ProtoListNamespacesResponse>, Status> {
        let req = request.into_inner();

        let parent = TenantName::parse(&req.parent)
            .map_err(|e| Status::invalid_argument(format!("invalid parent tenant name: {}", e)))?;

        let page_size = if req.page_size <= 0 {
            None
        } else {
            Some(req.page_size)
        };

        let page_token = if req.page_token.is_empty() {
            None
        } else {
            Some(req.page_token)
        };

        let list_request = ListNamespacesRequest {
            parent,
            page_size,
            page_token,
        };

        let response = self
            .admin
            .list_namespaces(list_request)
            .await
            .map_err(admin_error_to_status)?;

        let proto_response = ProtoListNamespacesResponse {
            namespaces: response
                .namespaces
                .into_iter()
                .map(|n| namespace_to_proto(&n))
                .collect(),
            next_page_token: response.next_page_token.unwrap_or_default(),
        };

        Ok(Response::new(proto_response))
    }

    async fn delete_namespace(
        &self,
        request: Request<DeleteNamespaceRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();

        let namespace_name = parse_namespace_name_from_full(&req.name)?;

        self.admin
            .delete_namespace(namespace_name)
            .await
            .map_err(admin_error_to_status)?;

        Ok(Response::new(()))
    }

    async fn create_topic(
        &self,
        request: Request<CreateTopicRequest>,
    ) -> Result<Response<ProtoTopic>, Status> {
        let req = request.into_inner();

        let topic_name = parse_topic_name(&req.topic_id, &req.parent)?;

        let topic_proto = req
            .topic
            .ok_or_else(|| Status::invalid_argument("topic field is required"))?;

        let options = proto_to_topic_options(&topic_proto)?;

        let topic = self
            .admin
            .create_topic(topic_name, options)
            .await
            .map_err(admin_error_to_status)?;

        Ok(Response::new(topic_to_proto(&topic)))
    }

    async fn get_topic(
        &self,
        request: Request<GetTopicRequest>,
    ) -> Result<Response<ProtoTopic>, Status> {
        let req = request.into_inner();

        let topic_name = parse_topic_name_from_full(&req.name)?;

        let topic = self
            .admin
            .get_topic(topic_name)
            .await
            .map_err(admin_error_to_status)?;

        Ok(Response::new(topic_to_proto(&topic)))
    }

    async fn list_topics(
        &self,
        request: Request<ProtoListTopicsRequest>,
    ) -> Result<Response<ProtoListTopicsResponse>, Status> {
        let req = request.into_inner();

        let parent = parse_namespace_name_from_full(&req.parent)?;

        let page_size = if req.page_size <= 0 {
            None
        } else {
            Some(req.page_size)
        };

        let page_token = if req.page_token.is_empty() {
            None
        } else {
            Some(req.page_token)
        };

        let list_request = ListTopicsRequest {
            parent,
            page_size,
            page_token,
        };

        let response = self
            .admin
            .list_topics(list_request)
            .await
            .map_err(admin_error_to_status)?;

        let proto_response = ProtoListTopicsResponse {
            topics: response
                .topics
                .into_iter()
                .map(|t| topic_to_proto(&t))
                .collect(),
            next_page_token: response.next_page_token.unwrap_or_default(),
        };

        Ok(Response::new(proto_response))
    }

    async fn delete_topic(
        &self,
        _request: Request<DeleteTopicRequest>,
    ) -> Result<Response<crate::protocol::google::longrunning::Operation>, Status> {
        todo!();
    }
}

// Helper functions for converting between protobuf and admin types

pub fn tenant_to_proto(tenant: &Tenant) -> ProtoTenant {
    ProtoTenant {
        name: tenant.name.to_string(),
    }
}

fn namespace_to_proto(namespace: &Namespace) -> ProtoNamespace {
    ProtoNamespace {
        name: namespace.name.to_string(),
        flush_size_bytes: namespace.flush_size.as_u64(),
        flush_interval_millis: namespace.flush_interval.as_millis() as u64,
        default_object_store_config: namespace.default_object_store_config.to_string(),
        frozen_object_store_config: namespace
            .frozen_object_store_config
            .as_ref()
            .map(|s| s.to_string()),
    }
}

fn proto_to_namespace_options(proto: &ProtoNamespace) -> Result<NamespaceOptions, Status> {
    let flush_size = bytesize::ByteSize::b(proto.flush_size_bytes);
    let flush_interval = Duration::from_millis(proto.flush_interval_millis);

    let default_object_store_config = SecretName::parse(&proto.default_object_store_config)
        .map_err(|e| {
            Status::invalid_argument(format!("invalid default object store config: {}", e))
        })?;

    let frozen_object_store_config = proto
        .frozen_object_store_config
        .as_ref()
        .map(|s| SecretName::parse(s))
        .transpose()
        .map_err(|e| {
            Status::invalid_argument(format!("invalid frozen object store config: {}", e))
        })?;

    Ok(NamespaceOptions {
        flush_size,
        flush_interval,
        default_object_store_config,
        frozen_object_store_config,
    })
}

fn topic_to_proto(_topic: &Topic) -> ProtoTopic {
    todo!();
}

fn proto_to_topic_options(_proto: &ProtoTopic) -> Result<TopicOptions, Status> {
    todo!();
}

fn parse_namespace_name(namespace_id: &str, parent: &str) -> Result<NamespaceName, Status> {
    let tenant_name = TenantName::parse(parent)
        .map_err(|e| Status::invalid_argument(format!("invalid parent tenant name: {}", e)))?;

    NamespaceName::new(namespace_id, tenant_name)
        .map_err(|e| Status::invalid_argument(format!("invalid namespace id: {}", e)))
}

fn parse_namespace_name_from_full(name: &str) -> Result<NamespaceName, Status> {
    NamespaceName::parse(name)
        .map_err(|e| Status::invalid_argument(format!("invalid namespace name: {}", e)))
}

fn parse_topic_name(topic_id: &str, parent: &str) -> Result<TopicName, Status> {
    let namespace_name = parse_namespace_name_from_full(parent)?;
    TopicName::new(topic_id, namespace_name)
        .map_err(|e| Status::invalid_argument(format!("invalid topic id: {}", e)))
}

fn parse_topic_name_from_full(name: &str) -> Result<TopicName, Status> {
    TopicName::parse(name)
        .map_err(|e| Status::invalid_argument(format!("invalid topic name: {}", e)))
}

fn admin_error_to_status(error: error_stack::Report<AdminError>) -> Status {
    match error.current_context() {
        AdminError::TenantNotFound { tenant_id } => {
            Status::not_found(format!("tenant not found: {}", tenant_id))
        }
        AdminError::TenantAlreadyExists { tenant_id } => {
            Status::already_exists(format!("tenant already exists: {}", tenant_id))
        }
        AdminError::TenantNotEmpty { tenant_id } => Status::failed_precondition(format!(
            "tenant has namespaces and cannot be deleted: {}",
            tenant_id
        )),
        AdminError::NamespaceNotFound { namespace_id } => {
            Status::not_found(format!("namespace not found: {}", namespace_id))
        }
        AdminError::NamespaceAlreadyExists { namespace_id } => {
            Status::already_exists(format!("namespace already exists: {}", namespace_id))
        }
        AdminError::NamespaceNotEmpty { namespace_id } => Status::failed_precondition(format!(
            "namespace has topics and cannot be deleted: {}",
            namespace_id
        )),
        AdminError::TopicNotFound { topic_id } => {
            Status::not_found(format!("topic not found: {}", topic_id))
        }
        AdminError::TopicAlreadyExists { topic_id } => {
            Status::already_exists(format!("topic already exists: {}", topic_id))
        }
        AdminError::InvalidTopicOptions { message } => {
            Status::invalid_argument(format!("invalid topic options: {}", message))
        }
        AdminError::InvalidTopicSchema { inner } => {
            Status::invalid_argument(format!("invalid topic schema: {}", inner))
        }
        AdminError::InvalidResourceName { name } => {
            Status::invalid_argument(format!("invalid resource name: {}", name))
        }
        AdminError::InvalidPageSize { size } => {
            Status::invalid_argument(format!("invalid page size: {}", size))
        }
        AdminError::InvalidPageToken { token } => {
            Status::invalid_argument(format!("invalid page token: {}", token))
        }
        AdminError::InvalidResource(resource_error) => match resource_error {
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
        AdminError::InvalidArgument { message } => {
            Status::invalid_argument(format!("invalid argument: {}", message))
        }
        AdminError::NotSupported => Status::unimplemented("operation not supported"),
        AdminError::Internal { message } => {
            Status::internal(format!("internal error: {}", message))
        }
    }
}
